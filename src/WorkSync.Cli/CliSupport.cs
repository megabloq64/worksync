using System.CommandLine;
using System.Globalization;
using WorkSync.Domain;

namespace WorkSync.Cli;

/// <summary>Options shared by every command that describes a transfer.</summary>
internal sealed class TransferRequestOptions
{
    public Option<CloudProvider> Source { get; } = new("--source", "-s") { Description = "Source provider", Required = true };
    public Option<CloudRegion> SourceRegion { get; } = new("--source-region") { Description = "Source region", Required = true };
    public Option<CloudProvider> Destination { get; } = new("--dest", "-d") { Description = "Destination provider", Required = true };
    public Option<CloudRegion> DestinationRegion { get; } = new("--dest-region") { Description = "Destination region", Required = true };
    public Option<string> Size { get; } = new("--size") { Description = "Total size, e.g. 750MB, 50GB, 1.5TB", Required = true };
    public Option<int> Files { get; } = new("--files") { Description = "Number of files", Required = true };
    public Option<int> Concurrency { get; } = new("--concurrency") { Description = "Parallel streams", DefaultValueFactory = _ => 4 };
    public Option<AccountTier> Tier { get; } = new("--tier") { Description = "Account tier", DefaultValueFactory = _ => AccountTier.Standard };

    public void AddTo(Command command)
    {
        if (Size.Validators.Count == 0)
        {
            Size.Validators.Add(r =>
            {
                if (r.GetValueOrDefault<string>() is { } s && !ByteSize.TryParse(s, out _)) r.AddError($"Invalid size '{s}'. Use e.g. 750MB, 50GB, 1.5TB or a byte count.");
            });
        }
        foreach (var o in new Option[] { Source, SourceRegion, Destination, DestinationRegion, Size, Files, Concurrency, Tier })
        {
            command.Options.Add(o);
        }
    }

    public TransferRequest Bind(ParseResult r)
    {
        var request = new TransferRequest(r.GetValue(Source), r.GetValue(SourceRegion), r.GetValue(Destination), r.GetValue(DestinationRegion),
            ByteSize.Parse(r.GetValue(Size)!), r.GetValue(Files), r.GetValue(Concurrency), r.GetValue(Tier));
        request.Validate();
        return request;
    }
}

internal static class ByteSize
{
    private static readonly (string Suffix, double Factor)[] s_units =
    [
        ("TB", 1L << 40), ("GB", 1L << 30), ("MB", 1L << 20), ("KB", 1L << 10), ("B", 1),
    ];

    public static bool TryParse(string text, out long bytes)
    {
        try
        {
            bytes = Parse(text);
            return bytes > 0;
        }
        catch (Exception ex) when (ex is FormatException or OverflowException)
        {
            bytes = 0;
            return false;
        }
    }

    public static long Parse(string text)
    {
        var s = text.Trim().ToUpperInvariant().Replace("IB", "B", StringComparison.Ordinal);
        foreach (var (suffix, factor) in s_units)
        {
            if (s.EndsWith(suffix, StringComparison.Ordinal))
            {
                return (long)(double.Parse(s[..^suffix.Length].Trim(), CultureInfo.InvariantCulture) * factor);
            }
        }
        return long.Parse(s, CultureInfo.InvariantCulture);
    }

    public static string Format(double bytes)
    {
        foreach (var (suffix, factor) in s_units)
        {
            if (bytes >= factor) return $"{bytes / factor:0.##} {suffix}";
        }
        return $"{bytes:0} B";
    }
}

internal static class Fmt
{
    public static string Duration(TimeSpan t) => t.TotalDays >= 1
        ? $"{(int)t.TotalDays}d {t.Hours}h {t.Minutes}m"
        : t.TotalHours >= 1 ? $"{(int)t.TotalHours}h {t.Minutes}m {t.Seconds}s"
        : t.TotalMinutes >= 1 ? $"{t.Minutes}m {t.Seconds}s"
        : $"{t.TotalSeconds:0.#}s";

    public static void Forecast(TransferForecast f)
    {
        Console.WriteLine($"Model version        v{f.ModelVersion}");
        Console.WriteLine($"Start                {f.StartAt:u}");
        Console.WriteLine($"Expected duration    {Duration(f.ExpectedDuration)}  (P10 {Duration(f.DurationP10)} – P90 {Duration(f.DurationP90)})");
        Console.WriteLine($"Estimated completion {f.EstimatedCompletion:u}");
        Console.WriteLine($"Failure probability  {f.FailureProbability:P1}");
        Console.WriteLine($"Expected throughput  {ByteSize.Format(f.ExpectedThroughputBytesPerSecond)}/s");
    }

    public static void Advice(BestTimeAdvice a)
    {
        Console.WriteLine($"Starting now:  {Duration(a.Now.ExpectedDuration)}, failure risk {a.Now.FailureProbability:P1}");
        Console.WriteLine("Best start times:");
        foreach (var s in a.BestSlots)
        {
            Console.WriteLine($"  {s.StartUtc:u}  (source local {s.StartSourceLocal:ddd HH:mm})  {Duration(s.ExpectedDuration),-12} risk {s.FailureProbability,6:P1}  saves {Duration(s.SavingsVersusNow)}");
        }
        Console.WriteLine("Average by source-local hour:");
        foreach (var h in a.HourlyHeatmap)
        {
            var bar = new string('#', (int)Math.Clamp(h.AverageExpectedDuration.TotalSeconds / a.HourlyHeatmap.Max(x => x.AverageExpectedDuration.TotalSeconds) * 40, 1, 40));
            Console.WriteLine($"  {h.SourceLocalHour:00}:00 {bar,-40} {Duration(h.AverageExpectedDuration)}");
        }
    }

    public static void Metrics(WorkSync.ML.ModelMetrics m)
    {
        Console.WriteLine($"  Duration   R² {m.Duration.RSquared:F3}  MAE(log) {m.Duration.MaeLog:F3}  median |err| {m.Duration.MedianAbsPercentError:P1}");
        Console.WriteLine($"  Failure    AUC {m.Failure.Auc:F3}  F1 {m.Failure.F1:F3}  log-loss {m.Failure.LogLoss:F3}  base rate {m.Failure.PositiveRate:P1}");
        Console.WriteLine($"  Throughput R² {m.Throughput.RSquared:F3}  MAE(log) {m.Throughput.MaeLog:F3}");
        Console.WriteLine($"  Holdout rows {m.HoldoutRows}");
    }
}
