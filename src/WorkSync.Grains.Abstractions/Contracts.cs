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
    AnomalyResult? Anomaly,
    DtuGrant? Dtus);

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

/// <summary>Result of reserving DTUs for a transfer. <see cref="Granted"/> may be less than requested (or 0) when the pool is busy.</summary>
[GenerateSerializer, Immutable, Alias("worksync.DtuGrant")]
public sealed record DtuGrant(Guid TransferId, int Requested, int Granted, double UtilizationBefore, int PoolSize)
{
    public bool IsPartial => Granted < Requested;
}

[GenerateSerializer, Immutable, Alias("worksync.DtuReservation")]
public sealed record DtuReservation(Guid TransferId, int Requested, int Granted, double UtilizationBefore, DateTimeOffset ReservedAt, DateTimeOffset LeaseExpiresAt);

[GenerateSerializer, Immutable, Alias("worksync.DtuPoolStatus")]
public sealed record DtuPoolStatus(
    string AccountId,
    AccountTier Tier,
    int PoolSize,
    int Reserved,
    int Available,
    double Utilization,
    IReadOnlyList<DtuReservation> Reservations);

/// <summary>
/// An account's DTU pool (key = account id). Transfers reserve DTUs for their lifetime and release them when they
/// finish; reservations carry a lease so DTUs held by a crashed transfer are reclaimed automatically.
/// </summary>
public interface IDtuPoolGrain : IGrainWithStringKey
{
    /// <summary>Grants min(requested, available) DTUs. Idempotent per transfer: repeating the call returns the original grant.</summary>
    Task<DtuGrant> ReserveAsync(Guid transferId, int requestedDtus, AccountTier tier, TimeSpan lease);

    /// <summary>Extends a reservation's lease (e.g. once the transfer knows how long it will run).</summary>
    Task<bool> RenewAsync(Guid transferId, TimeSpan lease);

    Task<bool> ReleaseAsync(Guid transferId);

    /// <param name="tier">Size the pool for this tier (the account's tier is otherwise remembered from its last reservation).</param>
    Task<DtuPoolStatus> GetStatusAsync(AccountTier? tier = null);
}

/// <summary>Stateless, horizontally scaled scoring endpoint. Uses whatever model the local silo has loaded.</summary>
public interface IPredictionGrain : IGrainWithIntegerKey
{
    /// <summary>
    /// Forecasts a transfer. Unless <paramref name="assumeFullGrant"/> is set, a transfer starting now is forecast
    /// with the DTUs its account pool could actually grant right now.
    /// </summary>
    Task<TransferForecast> ForecastAsync(TransferRequest request, DateTimeOffset? startUtc = null, bool assumeFullGrant = false);

    /// <summary>Compares DTU allocations for a transfer; <paramref name="assumePoolAvailable"/> ignores current reservations.</summary>
    Task<DtuAdvice> AdviseDtusAsync(TransferRequest request, DateTimeOffset? startUtc = null, bool assumePoolAvailable = false);

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
