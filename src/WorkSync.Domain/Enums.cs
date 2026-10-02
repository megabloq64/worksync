namespace WorkSync.Domain;

public enum CloudProvider
{
    AzureBlob,
    S3,
    GCS,
    OneDrive,
    GoogleDrive,
    Dropbox,
    Box,
}

public enum CloudRegion
{
    UsEast,
    UsWest,
    CanadaCentral,
    BrazilSouth,
    EuWest,
    EuNorth,
    UkSouth,
    AsiaEast,
    AsiaSouth,
    JapanEast,
    AustraliaEast,
}

public enum Continent
{
    NorthAmerica,
    SouthAmerica,
    Europe,
    Asia,
    Oceania,
}

public enum AccountTier
{
    Free,
    Standard,
    Premium,
    Enterprise,
}

public enum FailureReason
{
    None,
    Throttled,
    AuthExpired,
    NetworkError,
    QuotaExceeded,
    FileConflict,
    ProviderOutage,
}

/// <summary>How far apart the source and destination regions are.</summary>
public enum RegionDistance
{
    SameRegion,
    SameContinent,
    CrossContinent,
}

public static class CloudProviderExtensions
{
    /// <summary>Object stores are built for bulk throughput; consumer drives have per-file API overhead and stricter throttling.</summary>
    public static bool IsObjectStore(this CloudProvider provider) =>
        provider is CloudProvider.AzureBlob or CloudProvider.S3 or CloudProvider.GCS;
}
