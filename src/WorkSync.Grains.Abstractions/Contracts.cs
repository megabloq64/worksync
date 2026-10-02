using WorkSync.Domain;
using WorkSync.ML;

namespace WorkSync.Grains;

public static class StreamNames
{
    /// <summary>Name of the Orleans stream provider carrying transfer events (memory in Dev, Redis Streams in Prod).</summary>
    public const string Provider = "transfers";

    /// <summary>Stream namespace; ingestor grains subscribe implicitly to it.</summary>
    public const string TransferEvents = "transfer-events";

    /// <summary>
    /// Events are spread over a fixed number of logical partitions (stream keys). Each partition is consumed by one
    /// ingestor grain activation, so ordering per transfer is preserved while load spreads over the cluster.
    /// </summary>
    public const int PartitionCount = 16;

    public static string PartitionFor(Guid transferId)
    {
        // FNV-1a: stable across processes and machines (string.GetHashCode is randomized per process).
        Span<byte> bytes = stackalloc byte[16];
        transferId.TryWriteBytes(bytes);
        var hash = 2166136261u;
        foreach (var b in bytes)
        {
            hash = (hash ^ b) * 16777619u;
        }
        return $"p{hash % PartitionCount:D2}";
    }
}

public static class WellKnownKeys
{
    public const string Coordinator = "global";
    public const string Trainer = "trainer";
}

public enum TransferState
{
    Pending,
    Running,
    Completed,
    Failed,
}

[GenerateSerializer, Immutable, Alias("worksync.TransferStatus")]
public sealed record TransferStatus(
    Guid TransferId,
    TransferState State,
    TransferRequest? Request,
    DateTimeOffset? StartedAt,
    DateTimeOffset? EndedAt,
    double ProgressFraction,
    TransferForecast? Forecast,
    DateTimeOffset? LiveEstimatedCompletion,
    FailureReason FailureReason,
    AnomalyResult? Anomaly);

[GenerateSerializer, Immutable, Alias("worksync.TrainingStatsDelta")]
public sealed record TrainingStatsDelta(
    int NewRecords,
    int ScoredRecords,
    double SumAbsLogResidual,
    double SumLogBytes,
    int SlowAnomalies);

[GenerateSerializer, Immutable, Alias("worksync.TrainingJob")]
public sealed record TrainingJob(
    Guid RunId,
    string Reason,
    DateTimeOffset WindowStart,
    DateTimeOffset WindowEnd,
    int? ChampionVersion);

[GenerateSerializer, Immutable, Alias("worksync.TrainingRunInfo")]
public sealed record TrainingRunInfo(
    Guid RunId,
    string Reason,
    DateTimeOffset RequestedAt,
    DateTimeOffset? CompletedAt,
    int? Version,
    bool Promoted,
    string Outcome,
    ModelMetrics? ChallengerMetrics,
    ModelMetrics? ChampionMetrics,
    int TrainingRows);

[GenerateSerializer, Immutable, Alias("worksync.TrainingStatus")]
public sealed record TrainingStatus(
    int? ChampionVersion,
    bool IsTraining,
    TrainingRunInfo? ActiveRun,
    long RecordsSinceLastTraining,
    double? RollingMaeLog,
    double? BaselineMaeLog,
    double? MeanLogBytesShift,
    DateTimeOffset? LastTrainedAt,
    DateTimeOffset? NextScheduledCheck,
    IReadOnlyList<TrainingRunInfo> History);

/// <summary>Stateless, horizontally scaled scoring endpoint. Uses whatever model the local silo has loaded.</summary>
public interface IPredictionGrain : IGrainWithIntegerKey
{
    Task<TransferForecast> ForecastAsync(TransferRequest request, DateTimeOffset? startUtc = null);
    Task<BestTimeAdvice> BestTimeAsync(BestTimeQuery query);
    Task<AnomalyResult> ScoreAnomalyAsync(TransferRecord record);
    Task<int?> GetLoadedVersionAsync();
}

/// <summary>One transfer. Runs it (simulated executor), emits its events, and exposes live status + ETA.</summary>
public interface ITransferGrain : IGrainWithGuidKey
{
    Task<TransferStatus> StartAsync(TransferRequest request);
    Task<TransferStatus> GetStatusAsync();
}

/// <summary>Entry point for externally produced events (e.g. a real transfer engine posting batches to the API).</summary>
public interface IEventIngressGrain : IGrainWithIntegerKey
{
    Task<int> PublishAsync(IReadOnlyList<TransferEvent> events);
}

/// <summary>Consumes one event partition: persists events, aggregates finished transfers and feeds training statistics.</summary>
public interface IEventIngestorGrain : IGrainWithStringKey
{
    Task<long> GetProcessedCountAsync();
}

/// <summary>Cluster singleton (key "global") deciding when to retrain and which model version is champion.</summary>
public interface ITrainingCoordinatorGrain : IGrainWithStringKey
{
    Task ReportAsync(TrainingStatsDelta delta);
    Task<TrainingStatus> GetStatusAsync();
    Task<TrainingRunInfo?> RequestTrainingAsync(string reason, bool force = false);
    Task CompleteTrainingAsync(TrainingRunInfo result);
    Task<int?> GetChampionVersionAsync();
    Task RollbackAsync(int version);
    Task<IReadOnlyList<ModelVersionInfo>> ListModelsAsync();
}

/// <summary>Runs training jobs. Only activated on silos with the trainer role.</summary>
public interface ITrainerGrain : IGrainWithStringKey
{
    /// <summary>Starts the job in the background and returns immediately; completion is reported to the coordinator.</summary>
    Task<bool> StartAsync(TrainingJob job);
    Task<bool> IsBusyAsync();
}

/// <summary>One activation per silo (key = silo address) used to push a newly promoted model to that silo.</summary>
public interface IModelSyncGrain : IGrainWithStringKey
{
    Task<int?> ActivateVersionAsync(int version);
}
