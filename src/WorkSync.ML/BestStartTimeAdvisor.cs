using WorkSync.Domain;

namespace WorkSync.ML;

/// <summary>
/// Answers "when should I start this transfer?" by re-scoring the duration and failure models for every hourly
/// start slot in a horizon. Time features are computed in each region's local time, so the advisor naturally
/// learns per-region business-hour congestion.
/// </summary>
public static class BestStartTimeAdvisor
{
    public const int MaxHorizonHours = 24 * 14;

    public static BestTimeAdvice Advise(TransferPredictor predictor, BestTimeQuery query, DateTimeOffset nowUtc)
    {
        query.Request.Validate();
        if (query.HorizonHours is < 1 or > MaxHorizonHours)
            throw new ArgumentOutOfRangeException(nameof(query), $"HorizonHours must be between 1 and {MaxHorizonHours}.");
        if (query.Top < 1) throw new ArgumentOutOfRangeException(nameof(query), "Top must be at least 1.");
        if (query.FailurePenalty < 0) throw new ArgumentOutOfRangeException(nameof(query), "FailurePenalty must be non-negative.");

        var from = (query.From ?? nowUtc).ToUniversalTime();
        var now = Slot(predictor, query, from, baselineSeconds: null);

        var firstHour = new DateTimeOffset(from.Year, from.Month, from.Day, from.Hour, 0, 0, TimeSpan.Zero).AddHours(1);
        var slots = Enumerable.Range(0, query.HorizonHours)
            .Select(h => Slot(predictor, query, firstHour.AddHours(h), now.ExpectedDuration.TotalSeconds))
            .ToArray();

        var best = slots.Append(now).OrderBy(s => s.Score).Take(query.Top).ToArray();

        var heatmap = slots
            .GroupBy(s => s.StartSourceLocal.Hour)
            .OrderBy(g => g.Key)
            .Select(g => new HourlyOutlook(g.Key,
                TimeSpan.FromSeconds(g.Average(s => s.ExpectedDuration.TotalSeconds)),
                g.Average(s => s.FailureProbability)))
            .ToArray();

        return new BestTimeAdvice(now, best, heatmap, predictor.Version);
    }

    private static StartSlot Slot(TransferPredictor predictor, BestTimeQuery query, DateTimeOffset startUtc, double? baselineSeconds)
    {
        var (logDuration, pFail) = predictor.ScoreFast(query.Request, startUtc);
        var seconds = Math.Exp(logDuration);
        var score = seconds * (1 + query.FailurePenalty * pFail);
        return new StartSlot(
            startUtc,
            Regions.ToLocalTime(query.Request.SourceRegion, startUtc),
            TimeSpan.FromSeconds(seconds),
            pFail,
            score,
            TimeSpan.FromSeconds((baselineSeconds ?? seconds) - seconds));
    }
}
