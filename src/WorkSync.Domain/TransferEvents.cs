using System.Text.Json.Serialization;

namespace WorkSync.Domain;

/// <summary>What the user wants to move. This is everything known before the transfer starts.</summary>
[GenerateSerializer, Immutable, Alias("worksync.TransferRequest")]
public sealed record TransferRequest(
    CloudProvider SourceProvider,
    CloudRegion SourceRegion,
    CloudProvider DestinationProvider,
    CloudRegion DestinationRegion,
    long TotalBytes,
    int FileCount,
    int Concurrency = 4,
    AccountTier Tier = AccountTier.Standard)
{
    [JsonIgnore]
    public double AverageFileSizeBytes => FileCount <= 0 ? TotalBytes : (double)TotalBytes / FileCount;

    [JsonIgnore]
    public RegionDistance Distance => Regions.DistanceBetween(SourceRegion, DestinationRegion);

    public void Validate()
    {
        if (TotalBytes <= 0) throw new ArgumentOutOfRangeException(nameof(TotalBytes), "TotalBytes must be positive.");
        if (FileCount <= 0) throw new ArgumentOutOfRangeException(nameof(FileCount), "FileCount must be positive.");
        if (Concurrency is < 1 or > 256) throw new ArgumentOutOfRangeException(nameof(Concurrency), "Concurrency must be between 1 and 256.");
        if (!Enum.IsDefined(SourceProvider) || !Enum.IsDefined(DestinationProvider)) throw new ArgumentException("Unknown provider.");
        if (!Enum.IsDefined(SourceRegion) || !Enum.IsDefined(DestinationRegion)) throw new ArgumentException("Unknown region.");
        if (!Enum.IsDefined(Tier)) throw new ArgumentException("Unknown tier.");
    }
}

/// <summary>Base type of every event emitted while a transfer runs. Events are append-only and idempotent by <see cref="EventId"/>.</summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(TransferStarted), "started")]
[JsonDerivedType(typeof(TransferProgress), "progress")]
[JsonDerivedType(typeof(TransferCompleted), "completed")]
[JsonDerivedType(typeof(TransferFailed), "failed")]
[GenerateSerializer, Immutable, Alias("worksync.TransferEvent")]
public abstract record TransferEvent(Guid EventId, Guid TransferId, long Sequence, DateTimeOffset Timestamp)
{
    [JsonIgnore]
    public virtual bool IsTerminal => false;
}

[GenerateSerializer, Immutable, Alias("worksync.TransferStarted")]
public sealed record TransferStarted(
    Guid EventId,
    Guid TransferId,
    long Sequence,
    DateTimeOffset Timestamp,
    TransferRequest Request,
    double? PredictedDurationSeconds = null,
    int? ModelVersion = null) : TransferEvent(EventId, TransferId, Sequence, Timestamp);

[GenerateSerializer, Immutable, Alias("worksync.TransferProgress")]
public sealed record TransferProgress(
    Guid EventId,
    Guid TransferId,
    long Sequence,
    DateTimeOffset Timestamp,
    long BytesTransferred,
    int FilesTransferred) : TransferEvent(EventId, TransferId, Sequence, Timestamp);

[GenerateSerializer, Immutable, Alias("worksync.TransferCompleted")]
public sealed record TransferCompleted(
    Guid EventId,
    Guid TransferId,
    long Sequence,
    DateTimeOffset Timestamp,
    long BytesTransferred,
    int Retries) : TransferEvent(EventId, TransferId, Sequence, Timestamp)
{
    public override bool IsTerminal => true;
}

[GenerateSerializer, Immutable, Alias("worksync.TransferFailed")]
public sealed record TransferFailed(
    Guid EventId,
    Guid TransferId,
    long Sequence,
    DateTimeOffset Timestamp,
    FailureReason Reason,
    long BytesTransferred,
    int Retries) : TransferEvent(EventId, TransferId, Sequence, Timestamp)
{
    public override bool IsTerminal => true;
}
