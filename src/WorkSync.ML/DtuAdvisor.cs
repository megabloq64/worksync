using WorkSync.Domain;

namespace WorkSync.ML;

/// <summary>
/// Answers "how many DTUs should this transfer get?" by re-scoring the models for each candidate allocation.
/// Extra DTUs hit diminishing returns (coordination overhead, provider throttling, route caps), so the advisor
/// reports the fastest allocation, the cheapest in DTU-hours, and a recommended middle ground.
/// </summary>
public static class DtuAdvisor
{
    /// <summary>The recommendation is the smallest allocation whose expected duration is within this factor of the fastest.</summary>
    public const double RecommendationTolerance = 1.10;

    /// <param name="poolSize">Total DTUs in the account pool.</param>
    /// <param name="poolAvailable">DTUs free in the pool right now.</param>
    public static DtuAdvice Advise(TransferPredictor predictor, TransferRequest request, DateTimeOffset startUtc, int poolSize, int poolAvailable)
    {
        request.Validate();
        if (poolSize < 1) throw new ArgumentOutOfRangeException(nameof(poolSize), "Pool size must be at least 1.");
        poolAvailable = Math.Clamp(poolAvailable, 0, poolSize);
        var cap = Math.Min(poolSize, Dtu.MaxPerTransfer);
        var utilization = 1 - (double)poolAvailable / poolSize;

        var candidates = Dtu.StandardSizes.Where(n => n <= cap)
            .Append(Math.Min(request.RequestedDtus, cap))
            .Append(cap)
            .Distinct()
            .Order()
            .ToArray();

        var m = predictor.Bundle.Manifest;
        var options = new List<DtuOption>(candidates.Length);
        double? previousSeconds = null;
        foreach (var n in candidates)
        {
            var (logDuration, pFail) = predictor.ScoreFast(request with { RequestedDtus = n }, startUtc, n, utilization);
            var seconds = Math.Exp(logDuration);
            var expected = TimeSpan.FromSeconds(seconds);
            options.Add(new DtuOption(
                n,
                expected,
                TimeSpan.FromSeconds(Math.Exp(logDuration + m.ResidualP90)),
                Dtu.Hours(n, expected),
                pFail,
                previousSeconds is { } prev ? prev / seconds : 1.0,
                n <= poolAvailable));
            previousSeconds = seconds;
        }

        var fastest = options.MinBy(o => o.ExpectedDuration)!;
        var mostEfficient = options.MinBy(o => o.DtuHours)!;
        var recommended = options.First(o => o.ExpectedDuration.TotalSeconds <= fastest.ExpectedDuration.TotalSeconds * RecommendationTolerance);

        return new DtuAdvice(options, fastest, mostEfficient, recommended, poolSize, poolAvailable,
            Note(recommended, fastest, options, poolAvailable), predictor.Version);
    }

    private static string Note(DtuOption recommended, DtuOption fastest, IReadOnlyList<DtuOption> options, int poolAvailable)
    {
        var note = recommended.Dtus == fastest.Dtus
            ? $"{recommended.Dtus} DTUs is both the recommended and the fastest allocation."
            : $"{recommended.Dtus} DTUs finishes within {(RecommendationTolerance - 1):P0} of the fastest option ({fastest.Dtus} DTUs) " +
              $"for {recommended.DtuHours / fastest.DtuHours:P0} of its DTU-hours.";

        if (recommended.AvailableNow) return note;
        if (poolAvailable == 0) return note + " The pool is fully reserved right now: the transfer would be refused until DTUs are released.";
        var bestNow = options.Where(o => o.AvailableNow).MinBy(o => o.ExpectedDuration);
        return note + $" Only {poolAvailable} DTUs are free right now, so starting immediately gets a partial grant" +
               (bestNow is null ? "." : $" (best available now: {bestNow.Dtus} DTUs, ~{Math.Ceiling(bestNow.ExpectedDuration.TotalMinutes):0} min).");
    }
}
