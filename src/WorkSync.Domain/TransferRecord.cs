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
    double? PredictedDurationSeconds = null,
    int? ModelVersion = null)
{
    public double DurationSeconds => Math.Max(0.001, (EndedAt - StartedAt).TotalSeconds);

    /// <summary>Observed throughput. Only meaningful for completed transfers.</summary>
    public double ThroughputBytesPerSecond => BytesTransferred / DurationSeconds;
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

        return terminal switch
        {
            TransferCompleted c => new TransferRecord(
                started.TransferId, started.Request, started.Timestamp, c.Timestamp,
                Failed: false, FailureReason.None, c.BytesTransferred, c.Retries,
                started.PredictedDurationSeconds, started.ModelVersion),
            TransferFailed f => new TransferRecord(
                started.TransferId, started.Request, started.Timestamp, f.Timestamp,
                Failed: true, f.Reason, f.BytesTransferred, f.Retries,
                started.PredictedDurationSeconds, started.ModelVersion),
            _ => null,
        };
    }
}
