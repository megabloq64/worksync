using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WorkSync.ML;
using WorkSync.Storage;

namespace WorkSync.Grains;

/// <summary>
/// Trains a challenger on the sliding window, evaluates the champion on the same holdout, publishes the challenger to
/// the registry and reports to the coordinator, which owns promotion. The CPU-heavy work runs on the thread pool so
/// it never blocks the Orleans scheduler. Only trainer-role silos host this grain type.
/// </summary>
public sealed partial class TrainerGrain(
    IEventStore store,
    IModelRegistry registry,
    IOptions<WorkSyncGrainOptions> options,
    TimeProvider clock,
    ILogger<TrainerGrain> logger) : Grain, ITrainerGrain
{
    private Task? _running;

    public Task<bool> IsBusyAsync() => Task.FromResult(_running is { IsCompleted: false });

    public Task<bool> StartAsync(TrainingJob job)
    {
        if (_running is { IsCompleted: false }) return Task.FromResult(false);
        DelayDeactivation(options.Value.TrainingLease);
        _running = RunAndReportAsync(job);
        return Task.FromResult(true);
    }

    private async Task RunAndReportAsync(TrainingJob job)
    {
        TrainingRunInfo result;
        try
        {
            result = await Task.Run(() => TrainAsync(job));
        }
        catch (Exception ex)
        {
            LogFailed(ex, job.RunId);
            result = Info(job, null, false, $"failed: {ex.GetBaseException().Message}", null, null, 0);
        }

        // Back on the grain scheduler here (await captured it).
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await GrainFactory.GetGrain<ITrainingCoordinatorGrain>(WellKnownKeys.Coordinator).CompleteTrainingAsync(result);
                return;
            }
            catch (Exception ex) when (attempt < 5)
            {
                LogReportRetry(ex, job.RunId, attempt);
                await Task.Delay(TimeSpan.FromSeconds(attempt * 2));
            }
        }
    }

    private async Task<TrainingRunInfo> TrainAsync(TrainingJob job)
    {
        var o = options.Value;
        var records = await store.ReadRecordsToListAsync(job.WindowStart, job.WindowEnd);
        TrainingResult trained;
        try
        {
            trained = new ModelTrainer(o.Training).Train(records, clock.GetUtcNow(), job.Reason);
        }
        catch (InsufficientTrainingDataException ex)
        {
            return Info(job, null, false, $"skipped: {ex.Message}", null, null, records.Count);
        }

        var challenger = trained.Bundle.Manifest.Metrics;
        ModelMetrics? champion = null;
        if (job.ChampionVersion is { } cv)
        {
            try
            {
                champion = ModelEvaluator.Evaluate(await registry.LoadAsync(cv), trained.Holdout);
            }
            catch (Exception ex) when (ex is ModelVersionNotFoundException or IncompatibleModelException)
            {
                // Champion vanished from the registry or predates the current feature schema; treat as no champion.
            }
        }

        var decision = ChampionChallenger.Decide(champion, challenger, o.Promotion);
        var version = await registry.PublishAsync(trained.Bundle);
        LogTrained(version, records.Count, challenger.Duration.RSquared, decision.Promote, decision.Reason);
        return Info(job, version, decision.Promote, decision.Reason, challenger, champion, trained.Bundle.Manifest.TrainingRows);
    }

    private TrainingRunInfo Info(TrainingJob job, int? version, bool promoted, string outcome, ModelMetrics? challenger, ModelMetrics? champion, int rows) =>
        new(job.RunId, job.Reason, clock.GetUtcNow(), clock.GetUtcNow(), version, promoted, outcome, challenger, champion, rows);

    [LoggerMessage(Level = LogLevel.Information, Message = "Trained v{Version} on {Rows} records (R² {RSquared:F3}); promote={Promote}: {Reason}")]
    private partial void LogTrained(int version, int rows, double rSquared, bool promote, string reason);

    [LoggerMessage(Level = LogLevel.Error, Message = "Training run {RunId} failed")]
    private partial void LogFailed(Exception ex, Guid runId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Reporting run {RunId} to coordinator failed (attempt {Attempt})")]
    private partial void LogReportRetry(Exception ex, Guid runId, int attempt);
}
