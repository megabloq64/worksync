namespace WorkSync.Domain;

/// <summary>
/// Data Transfer Units: the metering unit for transfer capacity. One DTU is a fixed bundle of CPU, memory and
/// bandwidth that runs one transfer worker. Each account has a pool of DTUs (sized by its tier); every transfer
/// reserves part of that pool for its whole lifetime, and its parallel workers draw on that reservation.
/// </summary>
public static class Dtu
{
    public const int MaxPerTransfer = 256;

    /// <summary>Nominal resources of one DTU (documentation and simulation; real values come from the platform).</summary>
    public const double VCpu = 0.5;
    public const int MemoryMiB = 1024;
    public const double BandwidthMBps = 12;

    /// <summary>Candidate allocations the advisor evaluates.</summary>
    public static readonly int[] StandardSizes = [1, 2, 4, 8, 16, 32, 64, 128, 256];

    public static int DefaultPoolSize(AccountTier tier) => tier switch
    {
        AccountTier.Free => 4,
        AccountTier.Standard => 16,
        AccountTier.Premium => 64,
        AccountTier.Enterprise => 256,
        _ => 16,
    };

    /// <summary>DTU-hours metered for holding <paramref name="dtus"/> for <paramref name="duration"/>.</summary>
    public static double Hours(int dtus, TimeSpan duration) => dtus * duration.TotalHours;
}
