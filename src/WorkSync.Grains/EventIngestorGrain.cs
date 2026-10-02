using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.Streams;
using WorkSync.Domain;
using WorkSync.Storage;

namespace WorkSync.Grains;

/// <summary>
/// Consumes one event partition (implicit subscription keyed by the partition string). Each event is written to the
/// event store before the stream delivery completes, so a crash leads to redelivery rather than loss. Terminal events
/// are folded into a <see cref="TransferRecord"/>, scored against the live model, and summarised for the coordinator.
/// </summary>
[ImplicitStreamSubscription(StreamNames.TransferEvents)]
public sealed partial class EventIngestorGrain(
    IEventStore store,
    ModelHost models,
    IOptions<WorkSyncGrainOptions> options,
    ILogger<EventIngestorGrain> logger) : Grain, IEventIngestorGrain
{
    private long _processed;
    private int _newRecords;
    private int _scored;
    private double _sumAbsResidual;
    private double _sumLogBytes;
    private int _anomalies;

    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var stream = this.GetStreamProvider(StreamNames.Provider)
            .GetStream<TransferEvent>(StreamId.Create(StreamNames.TransferEvents, this.GetPrimaryKeyString()));
        var handles = await stream.GetAllSubscriptionHandles();
        if (handles.Count > 0)
        {
            foreach (var h in handles) await h.ResumeAsync(OnNextAsync);
        }
        else
        {
            await stream.SubscribeAsync(OnNextAsync);
        }

        var period = options.Value.StatsFlushInterval;
        this.RegisterGrainTimer(FlushAsync, new GrainTimerCreationOptions(period, period) { KeepAlive = false });
    }

    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        await FlushAsync(cancellationToken);
    }

    public Task<long> GetProcessedCountAsync() => Task.FromResult(_processed);

    private async Task OnNextAsync(TransferEvent e, StreamSequenceToken? token)
    {
        await store.AppendAsync(e);
        _processed++;
        if (!e.IsTerminal) return;

        // Even if this terminal event is a redelivery, the record may not have been saved last time: always try.
        var record = TransferAggregator.TryAggregate(await store.GetEventsAsync(e.TransferId));
        if (record is null || !await store.SaveRecordAsync(record)) return;

        _newRecords++;
        _sumLogBytes += Math.Log(Math.Max(1, record.Request.TotalBytes));
        if (!record.Failed && models.Current is { } predictor)
        {
            var anomaly = predictor.ScoreAnomaly(record);
            _scored++;
            _sumAbsResidual += Math.Abs(Math.Log(anomaly.ActualDuration.TotalSeconds / anomaly.ExpectedDuration.TotalSeconds));
            if (anomaly.IsSlowAnomaly)
            {
                _anomalies++;
                LogSlow(record.TransferId, record.Request.SourceProvider, record.Request.DestinationProvider, anomaly.SlowdownFactor);
            }
        }
    }

    private async Task FlushAsync(CancellationToken ct)
    {
        if (_newRecords == 0) return;
        var delta = new TrainingStatsDelta(_newRecords, _scored, _sumAbsResidual, _sumLogBytes, _anomalies);
        try
        {
            await GrainFactory.GetGrain<ITrainingCoordinatorGrain>(WellKnownKeys.Coordinator).ReportAsync(delta);
            (_newRecords, _scored, _sumAbsResidual, _sumLogBytes, _anomalies) = (0, 0, 0, 0, 0);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Keep the counters; next tick retries.
            LogFlushFailed(ex);
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Transfer {TransferId} ({Source}→{Destination}) was {Factor:F1}x slower than expected")]
    private partial void LogSlow(Guid transferId, CloudProvider source, CloudProvider destination, double factor);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to report training statistics")]
    private partial void LogFlushFailed(Exception ex);
}
