using WorkSync.Domain;

namespace WorkSync.ML;

/// <summary>Row schema fed to ML.NET. Categorical columns are strings, numeric ones are floats. Labels are filled only for training.</summary>
public sealed class TransferModelInput
{
    public string SourceProvider { get; set; } = "";
    public string DestinationProvider { get; set; } = "";
    public string ProviderPair { get; set; } = "";
    public string SourceRegion { get; set; } = "";
    public string DestinationRegion { get; set; } = "";
    public string Distance { get; set; } = "";
    public string Tier { get; set; } = "";

    public float SameProvider { get; set; }
    public float SourceIsObjectStore { get; set; }
    public float DestinationIsObjectStore { get; set; }
    public float LogBytes { get; set; }
    public float LogFileCount { get; set; }
    public float LogAvgFileSize { get; set; }
    // DTU allocation: the workers actually granted (not merely requested) drive throughput.
    public float LogGrantedDtus { get; set; }
    public float GrantRatio { get; set; }
    public float PoolUtilizationAtStart { get; set; }
    public float LogBytesPerDtu { get; set; }

    public float SourceLocalHour { get; set; }
    public float SourceHourSin { get; set; }
    public float SourceHourCos { get; set; }
    public float SourceDayOfWeek { get; set; }
    public float SourceIsWeekend { get; set; }
    public float SourceIsBusinessHours { get; set; }
    public float DestinationLocalHour { get; set; }
    public float DestinationIsWeekend { get; set; }
    public float DestinationIsBusinessHours { get; set; }

    // Labels / weights (training only)
    public float LogDuration { get; set; }
    public bool Failed { get; set; }
    public float LogThroughput { get; set; }
    public float Weight { get; set; } = 1f;
}

internal sealed class RegressionOutput
{
    public float Score { get; set; }
}

internal sealed class BinaryOutput
{
    public float Probability { get; set; }
}

public static class FeatureBuilder
{
    public static readonly string[] CategoricalColumns =
        [nameof(TransferModelInput.SourceProvider), nameof(TransferModelInput.DestinationProvider), nameof(TransferModelInput.ProviderPair),
         nameof(TransferModelInput.SourceRegion), nameof(TransferModelInput.DestinationRegion), nameof(TransferModelInput.Distance),
         nameof(TransferModelInput.Tier)];

    public static readonly string[] NumericColumns =
        [nameof(TransferModelInput.SameProvider), nameof(TransferModelInput.SourceIsObjectStore), nameof(TransferModelInput.DestinationIsObjectStore),
         nameof(TransferModelInput.LogBytes), nameof(TransferModelInput.LogFileCount), nameof(TransferModelInput.LogAvgFileSize),
         nameof(TransferModelInput.LogGrantedDtus), nameof(TransferModelInput.GrantRatio), nameof(TransferModelInput.PoolUtilizationAtStart),
         nameof(TransferModelInput.LogBytesPerDtu), nameof(TransferModelInput.SourceLocalHour), nameof(TransferModelInput.SourceHourSin),
         nameof(TransferModelInput.SourceHourCos), nameof(TransferModelInput.SourceDayOfWeek), nameof(TransferModelInput.SourceIsWeekend),
         nameof(TransferModelInput.SourceIsBusinessHours), nameof(TransferModelInput.DestinationLocalHour),
         nameof(TransferModelInput.DestinationIsWeekend), nameof(TransferModelInput.DestinationIsBusinessHours)];

    /// <summary>Bump whenever the feature columns change; bundles trained on another schema cannot score these rows.</summary>
    public const int SchemaVersion = 2;

    /// <param name="grantedDtus">DTUs reserved for the transfer; defaults to a full grant of the requested DTUs.</param>
    /// <param name="poolUtilization">Fraction of the account pool already reserved when the transfer starts.</param>
    public static TransferModelInput FromRequest(TransferRequest r, DateTimeOffset startUtc, int? grantedDtus = null, double poolUtilization = 0)
    {
        var granted = Math.Max(0, grantedDtus ?? r.RequestedDtus);
        var srcLocal = Regions.ToLocalTime(r.SourceRegion, startUtc);
        var dstLocal = Regions.ToLocalTime(r.DestinationRegion, startUtc);
        var srcHour = srcLocal.Hour + srcLocal.Minute / 60.0;
        var angle = 2 * Math.PI * srcHour / 24.0;

        return new TransferModelInput
        {
            SourceProvider = r.SourceProvider.ToString(),
            DestinationProvider = r.DestinationProvider.ToString(),
            ProviderPair = $"{r.SourceProvider}->{r.DestinationProvider}",
            SourceRegion = r.SourceRegion.ToString(),
            DestinationRegion = r.DestinationRegion.ToString(),
            Distance = r.Distance.ToString(),
            Tier = r.Tier.ToString(),
            SameProvider = r.SourceProvider == r.DestinationProvider ? 1 : 0,
            SourceIsObjectStore = r.SourceProvider.IsObjectStore() ? 1 : 0,
            DestinationIsObjectStore = r.DestinationProvider.IsObjectStore() ? 1 : 0,
            LogBytes = (float)Math.Log(Math.Max(1, r.TotalBytes)),
            LogFileCount = (float)Math.Log(Math.Max(1, r.FileCount)),
            LogAvgFileSize = (float)Math.Log(Math.Max(1, r.AverageFileSizeBytes)),
            LogGrantedDtus = (float)Math.Log(Math.Max(1, granted)),
            GrantRatio = (float)Math.Clamp((double)granted / Math.Max(1, r.RequestedDtus), 0, 1),
            PoolUtilizationAtStart = (float)Math.Clamp(poolUtilization, 0, 1),
            LogBytesPerDtu = (float)Math.Log(Math.Max(1.0, (double)r.TotalBytes / Math.Max(1, granted))),
            SourceLocalHour = (float)srcHour,
            SourceHourSin = (float)Math.Sin(angle),
            SourceHourCos = (float)Math.Cos(angle),
            SourceDayOfWeek = (float)srcLocal.DayOfWeek,
            SourceIsWeekend = srcLocal.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday ? 1 : 0,
            SourceIsBusinessHours = IsBusinessHours(srcLocal) ? 1 : 0,
            DestinationLocalHour = dstLocal.Hour + dstLocal.Minute / 60f,
            DestinationIsWeekend = dstLocal.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday ? 1 : 0,
            DestinationIsBusinessHours = IsBusinessHours(dstLocal) ? 1 : 0,
        };
    }

    /// <summary>Weekday 08:00-18:00 local time, when shared network and provider capacity is busiest.</summary>
    public static bool IsBusinessHours(DateTimeOffset local) =>
        local.DayOfWeek is not (DayOfWeek.Saturday or DayOfWeek.Sunday) && local.Hour is >= 8 and < 18;

    public static TransferModelInput FromRecord(TransferRecord record, float weight = 1f)
    {
        var input = FromRequest(record.Request, record.StartedAt, record.GrantedDtus, record.PoolUtilizationAtStart);
        input.LogDuration = (float)Math.Log(record.DurationSeconds);
        input.Failed = record.Failed;
        input.LogThroughput = (float)Math.Log(Math.Max(1, record.ThroughputBytesPerSecond));
        input.Weight = weight;
        return input;
    }

    /// <summary>Exponential recency weight so newer behaviour dominates when providers change over time.</summary>
    public static float RecencyWeight(DateTimeOffset endedAt, DateTimeOffset now, double halfLifeDays) =>
        halfLifeDays <= 0 ? 1f : (float)Math.Pow(0.5, Math.Max(0, (now - endedAt).TotalDays) / halfLifeDays);
}
