namespace WorkSync.Domain;

/// <summary>One finished transfer flattened into a single row. This is the unit of training data.</summary>
[GenerateSerializer, Immutable, Alias("worksync.TransferRecord")]
public sealed record TransferRecord(
    Guid TransferId,
    TransferRequest Request,
    DateTimeOffset StartedAt,
    DateTimeOffset EndedAt,
    bool Failed,
    FailureReason FailureReason,
    long BytesTransferred,
    int Retries,
    int GrantedDtus,
    double PoolUtilizationAtStart,
    double? PredictedDurationSeconds = null,
    int? ModelVersion = null)
{
    public double DurationSeconds => Math.Max(0.001, (EndedAt - StartedAt).TotalSeconds);

    /// <summary>Observed throughput. Only meaningful for completed transfers.</summary>
    public double ThroughputBytesPerSecond => BytesTransferred / DurationSeconds;

    /// <summary>Metered usage: the granted DTUs are reserved for the whole wall-clock duration.</summary>
    public double DtuHours => GrantedDtus * DurationSeconds / 3600.0;
}

public static class TransferAggregator
{
    /// <summary>
    /// Folds a transfer's events into a <see cref="TransferRecord"/>. Returns null until both the start event
    /// and a terminal event have been seen. Duplicate events (same sequence) are tolerated.
    /// </summary>
    public static TransferRecord? TryAggregate(IEnumerable<TransferEvent> events)
    {
        TransferStarted? started = null;
        TransferEvent? terminal = null;

        foreach (var e in events.DistinctBy(e => e.Sequence).OrderBy(e => e.Sequence))
        {
            switch (e)
            {
                case TransferStarted s:
                    started = s;
                    break;
                case TransferCompleted or TransferFailed when terminal is null:
                    terminal = e;
                    break;
            }
        }

        if (started is null || terminal is null)
        {
            return null;
        }

        // Producers that don't report grants (older or external engines) are assumed to have received what they asked for.
        var granted = started.GrantedDtus ?? started.Request.RequestedDtus;
        var utilization = started.PoolUtilizationAtStart ?? 0;
        return terminal switch
        {
            TransferCompleted c => new TransferRecord(
                started.TransferId, started.Request, started.Timestamp, c.Timestamp,
                Failed: false, FailureReason.None, c.BytesTransferred, c.Retries, granted, utilization,
                started.PredictedDurationSeconds, started.ModelVersion),
            TransferFailed f => new TransferRecord(
                started.TransferId, started.Request, started.Timestamp, f.Timestamp,
                Failed: true, f.Reason, f.BytesTransferred, f.Retries, granted, utilization,
                started.PredictedDurationSeconds, started.ModelVersion),
            _ => null,
        };
    }
}
