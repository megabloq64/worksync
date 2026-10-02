using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using WorkSync.ML;
using WorkSync.Storage;

namespace WorkSync.Grains;

[GenerateSerializer, Alias("worksync.CoordinatorState")]
public sealed class CoordinatorState
{
    [Id(0)] public int? ChampionVersion { get; set; }
    [Id(1)] public double? BaselineMaeLog { get; set; }
    [Id(2)] public double? BaselineMeanLogBytes { get; set; }
    [Id(3)] public DateTimeOffset? LastTrainedAt { get; set; }
    [Id(4)] public long RecordsSince { get; set; }
    [Id(5)] public long ScoredSince { get; set; }
    [Id(6)] public double SumAbsResidual { get; set; }
    [Id(7)] public double SumLogBytes { get; set; }
    [Id(8)] public TrainingRunInfo? ActiveRun { get; set; }
    [Id(9)] public DateTimeOffset? LeaseExpiresAt { get; set; }
    [Id(10)] public List<TrainingRunInfo> History { get; set; } = [];
}

/// <summary>
/// Single writer for "which model is champion". Accumulates statistics from ingestors, decides when to retrain
/// (count / interval / accuracy drift / input drift), holds the training lease, applies promotions and rollbacks,
/// and pushes new versions to every silo.
/// </summary>
public sealed partial class TrainingCoordinatorGrain(
    [PersistentState("coordinator", "worksync")] IPersistentState<CoordinatorState> state,
    IModelRegistry registry,
    IEventStore store,
    IOptions<WorkSyncGrainOptions> options,
    TimeProvider clock,
    ILogger<TrainingCoordinatorGrain> logger) : Grain, ITrainingCoordinatorGrain, IRemindable
{
    private const int HistoryLimit = 25;
    private const string ReminderName = "retrain-check";
    private readonly WorkSyncGrainOptions _options = options.Value;
    private DateTimeOffset? _nextCheck;
    private bool _dirty;

    private CoordinatorState S => state.State;

    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        await this.RegisterOrUpdateReminder(ReminderName, _options.ReminderPeriod, _options.ReminderPeriod);
        this.RegisterGrainTimer(CheckAsync, new GrainTimerCreationOptions(_options.TriggerCheckInterval, _options.TriggerCheckInterval) { KeepAlive = true });
        _nextCheck = clock.GetUtcNow() + _options.TriggerCheckInterval;

        // Adopt a model that was published out-of-band (CLI, seeding) if we don't know a champion yet.
        if (S.ChampionVersion is null && await registry.GetCurrentVersionAsync(cancellationToken) is { } current)
        {
            await AdoptChampionAsync(current, cancellationToken);
            await state.WriteStateAsync(cancellationToken);
        }
    }

    public Task ReceiveReminder(string reminderName, TickStatus status) => CheckAsync(CancellationToken.None);

    public async Task ReportAsync(TrainingStatsDelta delta)
    {
        S.RecordsSince += delta.NewRecords;
        S.ScoredSince += delta.ScoredRecords;
        S.SumAbsResidual += delta.SumAbsLogResidual;
        S.SumLogBytes += delta.SumLogBytes;
        _dirty = true;
        if (S.RecordsSince >= _options.RetrainAfterNewRecords)
        {
            await TryStartAsync($"{S.RecordsSince} new transfers", force: false);
        }
    }

    public async Task<TrainingStatus> GetStatusAsync()
    {
        var (mae, shift) = Drift();
        var records = S.RecordsSince;
        // Before the first model exists, report how much data is actually stored.
        if (S.ChampionVersion is null)
        {
            var now = clock.GetUtcNow();
            records = await store.CountRecordsAsync(now - _options.TrainingWindow, now + FutureSlack);
        }
        return new TrainingStatus(S.ChampionVersion, IsTraining, S.ActiveRun, records, mae, S.BaselineMaeLog, shift,
            S.LastTrainedAt, _nextCheck, S.History.AsReadOnly());
    }

    public Task<TrainingRunInfo?> RequestTrainingAsync(string reason, bool force = false) => TryStartAsync(reason, force);

    public async Task CompleteTrainingAsync(TrainingRunInfo result)
    {
        if (S.ActiveRun?.RunId != result.RunId)
        {
            LogStaleResult(result.RunId);
            return;
        }

        var now = clock.GetUtcNow();
        var requestedAt = S.ActiveRun.RequestedAt;
        S.ActiveRun = null;
        S.LeaseExpiresAt = null;
        S.History.Insert(0, result with { RequestedAt = requestedAt, CompletedAt = result.CompletedAt ?? now });
        if (S.History.Count > HistoryLimit) S.History.RemoveRange(HistoryLimit, S.History.Count - HistoryLimit);

        if (result.Version is not null)
        {
            // A run that produced a model (promoted or not) resets the triggers; a skipped/failed run does not,
            // so it will be retried on the next check.
            S.LastTrainedAt = now;
            ResetAccumulators();
        }

        if (result is { Promoted: true, Version: { } version })
        {
            await registry.SetCurrentVersionAsync(version);
            await AdoptChampionAsync(version, CancellationToken.None);
            LogPromoted(version, result.Outcome);
        }
        else
        {
            LogNotPromoted(result.Version, result.Outcome);
        }

        await state.WriteStateAsync();
        if (result is { Promoted: true, Version: { } v }) await BroadcastAsync(v);
    }

    public Task<int?> GetChampionVersionAsync() => Task.FromResult(S.ChampionVersion);

    public async Task RollbackAsync(int version)
    {
        await registry.SetCurrentVersionAsync(version);
        await AdoptChampionAsync(version, CancellationToken.None);
        ResetAccumulators();
        S.History.Insert(0, new TrainingRunInfo(Guid.NewGuid(), $"manual rollback to v{version}", clock.GetUtcNow(), clock.GetUtcNow(),
            version, true, "rolled back", null, null, 0));
        await state.WriteStateAsync();
        await BroadcastAsync(version);
    }

    public Task<IReadOnlyList<ModelVersionInfo>> ListModelsAsync() => registry.ListAsync();

    private bool IsTraining => S.ActiveRun is not null && S.LeaseExpiresAt > clock.GetUtcNow();

    /// <summary>Simulated transfers report simulated end times that can run ahead of the wall clock.</summary>
    private static TimeSpan FutureSlack => TimeSpan.FromDays(7);

    private async Task CheckAsync(CancellationToken ct)
    {
        _nextCheck = clock.GetUtcNow() + _options.TriggerCheckInterval;
        if (Trigger() is { } reason)
        {
            await TryStartAsync(reason, force: false);
        }
        else if (_dirty)
        {
            await state.WriteStateAsync(ct);
            _dirty = false;
        }
    }

    private string? Trigger()
    {
        var now = clock.GetUtcNow();
        if (S.ChampionVersion is null)
        {
            return S.RecordsSince >= _options.Training.MinimumRecords ? "initial model" : null;
        }
        if (S.RecordsSince >= _options.RetrainAfterNewRecords) return $"{S.RecordsSince} new transfers";
        if (S.RecordsSince > 0 && S.LastTrainedAt is { } last && now - last >= _options.RetrainInterval) return "scheduled interval";

        var (mae, shift) = Drift();
        if (S.ScoredSince >= _options.DriftMinimumSamples)
        {
            if (mae > S.BaselineMaeLog * _options.DriftMaeRatio) return $"accuracy drift (MAE {mae:F3} vs baseline {S.BaselineMaeLog:F3})";
            if (Math.Abs(shift ?? 0) > _options.DriftMeanLogBytesShift) return $"input drift (mean log-bytes shift {shift:F2})";
        }
        return null;
    }

    private (double? Mae, double? Shift) Drift() =>
        (S.ScoredSince > 0 ? S.SumAbsResidual / S.ScoredSince : null,
         S.RecordsSince > 0 && S.BaselineMeanLogBytes is { } b ? S.SumLogBytes / S.RecordsSince - b : null);

    private async Task<TrainingRunInfo?> TryStartAsync(string reason, bool force)
    {
        var now = clock.GetUtcNow();
        if (IsTraining)
        {
            return force ? S.ActiveRun : null;
        }
        if (S.ActiveRun is { } lost)
        {
            S.History.Insert(0, lost with { CompletedAt = now, Outcome = "lease expired" });
        }

        var run = new TrainingRunInfo(Guid.NewGuid(), reason, now, null, null, false, "running", null, null, 0);
        var job = new TrainingJob(run.RunId, reason, now - _options.TrainingWindow, now + FutureSlack, S.ChampionVersion);
        var accepted = await GrainFactory.GetGrain<ITrainerGrain>(WellKnownKeys.Trainer).StartAsync(job);
        if (!accepted) return null;

        S.ActiveRun = run;
        S.LeaseExpiresAt = now + _options.TrainingLease;
        await state.WriteStateAsync();
        _dirty = false;
        LogStarted(run.RunId, reason);
        return run;
    }

    private async Task AdoptChampionAsync(int version, CancellationToken ct)
    {
        var manifest = await registry.GetManifestAsync(version, ct) ?? throw new ModelVersionNotFoundException(version);
        S.ChampionVersion = version;
        S.BaselineMaeLog = manifest.Metrics.Duration.MaeLog;
        S.BaselineMeanLogBytes = manifest.TrainingMeanLogBytes;
    }

    private void ResetAccumulators()
    {
        S.RecordsSince = 0;
        S.ScoredSince = 0;
        S.SumAbsResidual = 0;
        S.SumLogBytes = 0;
    }

    private async Task BroadcastAsync(int version)
    {
        var hosts = await GrainFactory.GetGrain<IManagementGrain>(0).GetHosts(onlyActive: true);
        var pushes = hosts.Keys.Select(async silo =>
        {
            try
            {
                await GrainFactory.GetGrain<IModelSyncGrain>(silo.ToParsableString()).ActivateVersionAsync(version)
                    .WaitAsync(TimeSpan.FromSeconds(60));
            }
            catch (Exception ex)
            {
                // The silo's poller will pick the version up.
                LogPushFailed(ex, silo.ToString(), version);
            }
        });
        await Task.WhenAll(pushes);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Training run {RunId} started: {Reason}")]
    private partial void LogStarted(Guid runId, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "Promoted model v{Version}: {Outcome}")]
    private partial void LogPromoted(int version, string outcome);

    [LoggerMessage(Level = LogLevel.Information, Message = "Kept champion (challenger v{Version}): {Outcome}")]
    private partial void LogNotPromoted(int? version, string outcome);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Ignoring result of stale training run {RunId}")]
    private partial void LogStaleResult(Guid runId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to push model v{Version} to silo {Silo}")]
    private partial void LogPushFailed(Exception ex, string silo, int version);
}
