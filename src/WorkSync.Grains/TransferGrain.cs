using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using WorkSync.Domain;

namespace WorkSync.Grains;

[GenerateSerializer, Alias("worksync.TransferGrainState")]
public sealed class TransferGrainState
{
    [Id(0)] public TransferState State { get; set; } = TransferState.Pending;
    [Id(1)] public TransferRequest? Request { get; set; }
    [Id(2)] public DateTimeOffset StartedAt { get; set; }
    [Id(3)] public TimeSpan PlannedDuration { get; set; }
    [Id(4)] public bool PlannedFailure { get; set; }
    [Id(5)] public FailureReason PlannedReason { get; set; }
    [Id(6)] public long PlannedBytes { get; set; }
    [Id(7)] public int PlannedRetries { get; set; }
    [Id(8)] public long NextSequence { get; set; }
    [Id(9)] public TransferForecast? Forecast { get; set; }
    [Id(10)] public AnomalyResult? Anomaly { get; set; }
    [Id(11)] public int ProgressEvents { get; set; }
    [Id(12)] public double TimeCompression { get; set; } = 1;
}

/// <summary>
/// Runs one transfer: forecasts it, executes it (simulated, time-compressed), emits its event stream and exposes live
/// status. Event timestamps are in simulated time (start + fraction × planned duration); wall-clock waits are divided by
/// the compression factor. Each due event is emitted at most once per sequence number (the store de-duplicates
/// redeliveries), and an interrupted transfer resumes on reactivation.
/// </summary>
public sealed partial class TransferGrain(
    [PersistentState("transfer", "worksync")] IPersistentState<TransferGrainState> state,
    ITransferExecutor executor,
    ModelHost models,
    IOptions<WorkSyncGrainOptions> options,
    TimeProvider clock,
    ILogger<TransferGrain> logger) : Grain, ITransferGrain
{
    private static readonly TimeSpan MinimumTick = TimeSpan.FromMilliseconds(100);
    private IGrainTimer? _timer;

    private TransferGrainState S => state.State;

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        if (S.State == TransferState.Running) StartTimer();
        return Task.CompletedTask;
    }

    public async Task<TransferStatus> StartAsync(TransferRequest request)
    {
        request.Validate();
        if (S.State != TransferState.Pending) return Status();

        var now = clock.GetUtcNow();
        var o = options.Value;
        var plan = executor.Plan(request, now);
        S.Request = request;
        S.StartedAt = now;
        S.PlannedDuration = plan.Duration;
        S.PlannedFailure = plan.Failed;
        S.PlannedReason = plan.Reason;
        S.PlannedBytes = plan.BytesTransferred;
        S.PlannedRetries = plan.Retries;
        S.ProgressEvents = Math.Max(0, o.SimulationProgressEvents);
        S.TimeCompression = Math.Max(1, o.SimulationTimeCompression);
        S.Forecast = models.Current?.Forecast(request, now);
        S.State = TransferState.Running;
        S.NextSequence = 0;

        await EmitDueEventsAsync();
        if (S.State == TransferState.Running) StartTimer();
        return Status();
    }

    public Task<TransferStatus> GetStatusAsync() => Task.FromResult(Status());

    private void StartTimer()
    {
        var period = TimeSpan.FromTicks(Math.Max(MinimumTick.Ticks, RealDuration.Ticks / (S.ProgressEvents + 1)));
        _timer?.Dispose();
        _timer = this.RegisterGrainTimer(_ => EmitDueEventsAsync(), new GrainTimerCreationOptions(period, period) { KeepAlive = true });
    }

    private TimeSpan RealDuration => S.PlannedDuration / S.TimeCompression;

    private long TerminalSequence => S.ProgressEvents + 1;

    /// <summary>Sequence n is due once n/(P+1) of the real (compressed) duration has elapsed.</summary>
    private DateTimeOffset RealDueAt(long sequence) => S.StartedAt + RealDuration * ((double)sequence / TerminalSequence);

    private DateTimeOffset SimulatedAt(long sequence) => S.StartedAt + S.PlannedDuration * ((double)sequence / TerminalSequence);

    private async Task EmitDueEventsAsync()
    {
        if (S.State != TransferState.Running || S.Request is not { } request) return;
        var now = clock.GetUtcNow();
        var batch = new List<TransferEvent>();
        for (var seq = S.NextSequence; seq <= TerminalSequence && RealDueAt(seq) <= now; seq++)
        {
            batch.Add(CreateEvent(request, seq));
        }
        if (batch.Count == 0) return;

        await TransferEventStreams.For(this.GetStreamProvider(StreamNames.Provider), this.GetPrimaryKey()).OnNextBatchAsync(batch);
        S.NextSequence += batch.Count;

        if (batch[^1].IsTerminal)
        {
            S.State = S.PlannedFailure ? TransferState.Failed : TransferState.Completed;
            _timer?.Dispose();
            _timer = null;
            if (!S.PlannedFailure) S.Anomaly = await TryScoreAsync(request);
        }
        await state.WriteStateAsync();
    }

    private TransferEvent CreateEvent(TransferRequest request, long seq)
    {
        var id = this.GetPrimaryKey();
        var at = SimulatedAt(seq);
        if (seq == 0)
        {
            return new TransferStarted(EventId(seq), id, 0, at, request, S.Forecast?.ExpectedDuration.TotalSeconds, S.Forecast?.ModelVersion);
        }
        if (seq < TerminalSequence)
        {
            var fraction = (double)seq / TerminalSequence;
            return new TransferProgress(EventId(seq), id, seq, at, (long)(S.PlannedBytes * fraction), (int)(request.FileCount * fraction));
        }
        return S.PlannedFailure
            ? new TransferFailed(EventId(seq), id, seq, at, S.PlannedReason, S.PlannedBytes, S.PlannedRetries)
            : new TransferCompleted(EventId(seq), id, seq, at, S.PlannedBytes, S.PlannedRetries);
    }

    /// <summary>Deterministic per (transfer, sequence) so re-emission after a crash produces identical events.</summary>
    private Guid EventId(long seq)
    {
        Span<byte> bytes = stackalloc byte[16];
        this.GetPrimaryKey().TryWriteBytes(bytes);
        BitConverter.TryWriteBytes(bytes[8..], BitConverter.ToInt64(bytes[8..]) ^ (seq + 1));
        return new Guid(bytes);
    }

    private async Task<AnomalyResult?> TryScoreAsync(TransferRequest request)
    {
        var record = new TransferRecord(this.GetPrimaryKey(), request, S.StartedAt, S.StartedAt + S.PlannedDuration, false,
            FailureReason.None, S.PlannedBytes, S.PlannedRetries, S.Forecast?.ExpectedDuration.TotalSeconds, S.Forecast?.ModelVersion);
        try
        {
            return await GrainFactory.GetGrain<IPredictionGrain>(0).ScoreAnomalyAsync(record);
        }
        catch (ModelNotReadyException)
        {
            return null;
        }
        catch (Exception ex)
        {
            LogScoreFailed(ex, record.TransferId);
            return null;
        }
    }

    private TransferStatus Status()
    {
        var id = this.GetPrimaryKey();
        if (S.Request is null)
        {
            return new TransferStatus(id, TransferState.Pending, null, null, null, 0, null, null, FailureReason.None, null);
        }

        var done = S.State is TransferState.Completed or TransferState.Failed;
        var sent = Math.Max(0, S.NextSequence - 1);
        var progress = done ? 1.0 : (double)sent / TerminalSequence;
        DateTimeOffset? endedAt = done ? S.StartedAt + S.PlannedDuration : null;

        // Live ETA in simulated time: extrapolate observed progress; fall back to the forecast before any progress.
        DateTimeOffset? eta = endedAt;
        if (!done)
        {
            eta = progress > 0
                ? S.StartedAt + (SimulatedAt(sent) - S.StartedAt) / progress
                : S.Forecast?.EstimatedCompletion;
        }

        return new TransferStatus(id, S.State, S.Request, S.StartedAt, endedAt, progress, S.Forecast, eta,
            done && S.PlannedFailure ? S.PlannedReason : FailureReason.None, S.Anomaly);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Anomaly scoring failed for transfer {TransferId}")]
    private partial void LogScoreFailed(Exception ex, Guid transferId);
}
