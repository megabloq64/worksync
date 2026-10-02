using WorkSync.Domain;

namespace WorkSync.DataGen;

/// <summary>Multiplies effective bandwidth for transfers touching a provider, letting simulations inject drift (e.g. "Dropbox got 2x slower").</summary>
public sealed record DriftProfile(IReadOnlyDictionary<CloudProvider, double> BandwidthMultipliers)
{
    public static DriftProfile None { get; } = new(new Dictionary<CloudProvider, double>());

    public double For(TransferRequest r) =>
        BandwidthMultipliers.GetValueOrDefault(r.SourceProvider, 1.0) *
        (r.SourceProvider == r.DestinationProvider ? 1.0 : BandwidthMultipliers.GetValueOrDefault(r.DestinationProvider, 1.0));

    /// <summary>Parses "Dropbox=0.5,S3=0.8".</summary>
    public static DriftProfile Parse(string? spec)
    {
        if (string.IsNullOrWhiteSpace(spec)) return None;
        var map = new Dictionary<CloudProvider, double>();
        foreach (var part in spec.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var kv = part.Split('=', 2, StringSplitOptions.TrimEntries);
            if (kv.Length != 2 || !Enum.TryParse<CloudProvider>(kv[0], ignoreCase: true, out var p) ||
                !double.TryParse(kv[1], System.Globalization.CultureInfo.InvariantCulture, out var f) || f <= 0)
            {
                throw new FormatException($"Invalid drift entry '{part}'. Expected Provider=Factor.");
            }
            map[p] = f;
        }
        return new DriftProfile(map);
    }
}

public sealed record SimulatedOutcome(TimeSpan Duration, bool Failed, FailureReason Reason, long BytesTransferred, int Retries);

/// <summary>
/// The "physics" used to make synthetic transfers look real. Effective bandwidth depends on the slower provider,
/// distance, provider pairing, concurrency, tier, local-time congestion at both ends, and lognormal noise. Failure
/// odds rise with size, small-file counts, consumer providers, the free tier, and peak hours.
/// </summary>
public static class TransferSimulator
{
    private const double MB = 1024 * 1024;

    public static double BaseBandwidthMBps(CloudProvider p) => p switch
    {
        CloudProvider.AzureBlob => 140,
        CloudProvider.S3 => 150,
        CloudProvider.GCS => 130,
        CloudProvider.OneDrive => 28,
        CloudProvider.GoogleDrive => 34,
        CloudProvider.Dropbox => 40,
        CloudProvider.Box => 22,
        _ => 30,
    };

    /// <summary>Per-file API overhead in seconds (metadata calls, session creation).</summary>
    public static double PerFileOverheadSeconds(CloudProvider p) => p.IsObjectStore() ? 0.015 : 0.12;

    /// <summary>Multiplier (≤ 1) for network/provider congestion at a region's local time.</summary>
    public static double Congestion(CloudRegion region, DateTimeOffset utc)
    {
        var local = Regions.ToLocalTime(region, utc);
        var weekend = local.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday;
        var h = local.Hour;
        var factor = h switch
        {
            >= 9 and < 12 => 0.55,
            >= 12 and < 14 => 0.62,
            >= 14 and < 18 => 0.52,
            >= 18 and < 23 => 0.78,
            >= 7 and < 9 => 0.75,
            _ => 1.0,
        };
        return weekend ? Math.Min(1.0, factor + 0.3) : factor;
    }

    public static double TierFactor(AccountTier t) => t switch
    {
        AccountTier.Free => 0.45,
        AccountTier.Standard => 1.0,
        AccountTier.Premium => 1.35,
        AccountTier.Enterprise => 1.7,
        _ => 1.0,
    };

    public static double DistanceFactor(RegionDistance d) => d switch
    {
        RegionDistance.SameRegion => 1.0,
        RegionDistance.SameContinent => 0.72,
        _ => 0.42,
    };

    /// <summary>Deterministic expected bandwidth in bytes/sec before noise.</summary>
    public static double ExpectedBandwidth(TransferRequest r, DateTimeOffset startUtc, DriftProfile? drift = null)
    {
        var mbps = Math.Min(BaseBandwidthMBps(r.SourceProvider), BaseBandwidthMBps(r.DestinationProvider));
        mbps *= DistanceFactor(r.Distance);
        mbps *= r.SourceProvider == r.DestinationProvider ? 1.25 : 1.0;
        mbps *= Math.Min(4.0, 1 + 0.55 * Math.Log(r.Concurrency));
        mbps *= TierFactor(r.Tier);
        // Congestion is felt at both ends; the source end dominates because reads are the bottleneck.
        mbps *= Math.Pow(Congestion(r.SourceRegion, startUtc), 0.7) * Math.Pow(Congestion(r.DestinationRegion, startUtc), 0.3);
        mbps *= (drift ?? DriftProfile.None).For(r);
        return mbps * MB;
    }

    public static double FailureLogit(TransferRequest r, DateTimeOffset startUtc)
    {
        var gb = r.TotalBytes / (MB * 1024);
        var logit = -4.2;
        logit += 0.35 * Math.Log10(1 + gb);
        logit += 0.25 * Math.Log10(1 + r.FileCount);
        logit += r.SourceProvider.IsObjectStore() ? 0 : 0.55;
        logit += r.DestinationProvider.IsObjectStore() ? 0 : 0.45;
        logit += r.Tier == AccountTier.Free ? 0.7 : r.Tier == AccountTier.Enterprise ? -0.4 : 0;
        logit += r.Distance == RegionDistance.CrossContinent ? 0.3 : 0;
        logit += r.Concurrency > 32 ? 0.4 : 0;
        logit += (1 - Congestion(r.SourceRegion, startUtc)) * 1.6;
        return logit;
    }

    /// <summary>Deterministic expected duration (no noise, no retries).</summary>
    public static double ExpectedDurationSeconds(TransferRequest r, DateTimeOffset startUtc, DriftProfile? drift = null) =>
        2.0 + r.TotalBytes / ExpectedBandwidth(r, startUtc, drift) + PerFileOverhead(r, startUtc);

    private static double PerFileOverhead(TransferRequest r, DateTimeOffset startUtc)
    {
        var perFile = Math.Max(PerFileOverheadSeconds(r.SourceProvider), PerFileOverheadSeconds(r.DestinationProvider));
        // Per-file API calls are rate-limited harder when the source provider is busy.
        return r.FileCount * perFile / Math.Min(r.Concurrency, Math.Max(1, r.FileCount)) / Math.Sqrt(Congestion(r.SourceRegion, startUtc));
    }

    public static SimulatedOutcome Simulate(TransferRequest r, DateTimeOffset startUtc, Random rng, DriftProfile? drift = null)
    {
        var bandwidth = ExpectedBandwidth(r, startUtc, drift) * LogNormal(rng, 0, 0.18);
        var seconds = 2.0 + r.TotalBytes / bandwidth + PerFileOverhead(r, startUtc);

        var throttleOdds = 1 - Congestion(r.SourceRegion, startUtc) + (r.SourceProvider.IsObjectStore() ? 0 : 0.25);
        var retries = Poisson(rng, Math.Clamp(throttleOdds * Math.Log10(2 + r.FileCount) * 0.8, 0, 6));
        seconds += retries * (5 + rng.NextDouble() * 25);

        var pFail = 1 / (1 + Math.Exp(-FailureLogit(r, startUtc)));
        if (rng.NextDouble() < pFail)
        {
            var progress = rng.NextDouble();
            return new SimulatedOutcome(
                TimeSpan.FromSeconds(Math.Max(1, seconds * progress)),
                Failed: true,
                PickReason(rng, r),
                (long)(r.TotalBytes * progress),
                retries + 1 + rng.Next(3));
        }

        return new SimulatedOutcome(TimeSpan.FromSeconds(seconds), false, FailureReason.None, r.TotalBytes, retries);
    }

    private static FailureReason PickReason(Random rng, TransferRequest r)
    {
        var roll = rng.NextDouble();
        return roll switch
        {
            < 0.35 => FailureReason.Throttled,
            < 0.55 => FailureReason.NetworkError,
            < 0.68 => r.Tier == AccountTier.Free ? FailureReason.QuotaExceeded : FailureReason.FileConflict,
            < 0.80 => FailureReason.AuthExpired,
            < 0.92 => FailureReason.FileConflict,
            _ => FailureReason.ProviderOutage,
        };
    }

    private static double LogNormal(Random rng, double mu, double sigma)
    {
        // Box-Muller
        var u1 = 1.0 - rng.NextDouble();
        var u2 = rng.NextDouble();
        var z = Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Cos(2 * Math.PI * u2);
        return Math.Exp(mu + sigma * z);
    }

    private static int Poisson(Random rng, double lambda)
    {
        if (lambda <= 0) return 0;
        var l = Math.Exp(-lambda);
        var k = 0;
        var p = 1.0;
        do
        {
            k++;
            p *= rng.NextDouble();
        } while (p > l);
        return k - 1;
    }
}
