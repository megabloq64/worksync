using Microsoft.ML;
using WorkSync.Domain;

namespace WorkSync.ML;

/// <summary>Evaluates any bundle on any set of records, so champion and challenger can be compared on identical data.</summary>
public static class ModelEvaluator
{
    public static ModelMetrics Evaluate(ModelBundle bundle, IReadOnlyList<TransferRecord> records)
    {
        var ctx = bundle.Context;
        var completed = records.Where(r => !r.Failed).ToArray();

        DurationMetrics duration;
        ThroughputMetrics throughput;
        if (completed.Length > 1)
        {
            var completedData = ctx.Data.LoadFromEnumerable(completed.Select(r => FeatureBuilder.FromRecord(r)));
            var durPred = ModelTrainer.Score(ctx, bundle.Duration, completedData);
            var durActual = completed.Select(r => Math.Log(r.DurationSeconds)).ToArray();
            var ape = completed.Select((r, i) => Math.Abs(Math.Exp(durPred[i]) - r.DurationSeconds) / r.DurationSeconds).ToArray();
            duration = new DurationMetrics(RSquared(durActual, durPred), Mae(durActual, durPred), Rmse(durActual, durPred), ModelTrainer.Quantile(ape, 0.5));

            var tpPred = ModelTrainer.Score(ctx, bundle.Throughput, completedData);
            var tpActual = completed.Select(r => Math.Log(Math.Max(1, r.ThroughputBytesPerSecond))).ToArray();
            throughput = new ThroughputMetrics(RSquared(tpActual, tpPred), Mae(tpActual, tpPred));
        }
        else
        {
            duration = new DurationMetrics(double.NaN, double.NaN, double.NaN, double.NaN);
            throughput = new ThroughputMetrics(double.NaN, double.NaN);
        }

        var failure = new FailureMetrics(double.NaN, double.NaN, double.NaN, 0);
        if (records.Count > 0)
        {
            var allData = ctx.Data.LoadFromEnumerable(records.Select(r => FeatureBuilder.FromRecord(r)));
            var probs = ctx.Data.CreateEnumerable<BinaryOutput>(bundle.Failure.Transform(allData), reuseRowObject: false)
                .Select(o => (double)o.Probability).ToArray();
            var labels = records.Select(r => r.Failed).ToArray();
            failure = new FailureMetrics(Auc(labels, probs), BestF1(labels, probs), LogLoss(labels, probs), labels.Count(l => l) / (double)labels.Length);
        }

        return new ModelMetrics(duration, failure, throughput, records.Count);
    }

    internal static double Mae(double[] y, double[] p) => y.Select((v, i) => Math.Abs(v - p[i])).Average();

    internal static double Rmse(double[] y, double[] p) => Math.Sqrt(y.Select((v, i) => (v - p[i]) * (v - p[i])).Average());

    internal static double RSquared(double[] y, double[] p)
    {
        var mean = y.Average();
        var ssTot = y.Sum(v => (v - mean) * (v - mean));
        var ssRes = y.Select((v, i) => (v - p[i]) * (v - p[i])).Sum();
        return ssTot == 0 ? double.NaN : 1 - ssRes / ssTot;
    }

    /// <summary>Rank-based (Mann-Whitney) AUC. NaN when only one class is present.</summary>
    internal static double Auc(bool[] labels, double[] scores)
    {
        var pos = labels.Count(l => l);
        var neg = labels.Length - pos;
        if (pos == 0 || neg == 0) return double.NaN;

        var order = scores.Select((s, i) => (s, i)).OrderBy(t => t.s).ToArray();
        var ranks = new double[order.Length];
        for (var i = 0; i < order.Length;)
        {
            var j = i;
            while (j + 1 < order.Length && order[j + 1].s == order[i].s) j++;
            var avgRank = (i + j) / 2.0 + 1;
            for (var k = i; k <= j; k++) ranks[order[k].i] = avgRank;
            i = j + 1;
        }
        var rankSumPos = labels.Select((l, i) => l ? ranks[i] : 0).Sum();
        return (rankSumPos - pos * (pos + 1) / 2.0) / ((double)pos * neg);
    }

    /// <summary>F1 at the threshold that maximises it, which is more informative than a fixed 0.5 for rare failures.</summary>
    internal static double BestF1(bool[] labels, double[] probs)
    {
        var best = 0.0;
        foreach (var t in probs.Distinct().Order().Where((_, i) => i % Math.Max(1, probs.Length / 200) == 0))
        {
            int tp = 0, fp = 0, fn = 0;
            for (var i = 0; i < labels.Length; i++)
            {
                var predicted = probs[i] >= t;
                if (predicted && labels[i]) tp++;
                else if (predicted) fp++;
                else if (labels[i]) fn++;
            }
            if (tp == 0) continue;
            var precision = tp / (double)(tp + fp);
            var recall = tp / (double)(tp + fn);
            best = Math.Max(best, 2 * precision * recall / (precision + recall));
        }
        return best;
    }

    internal static double LogLoss(bool[] labels, double[] probs) =>
        -labels.Select((l, i) =>
        {
            var p = Math.Clamp(probs[i], 1e-7, 1 - 1e-7);
            return l ? Math.Log(p) : Math.Log(1 - p);
        }).Average();
}

public sealed record PromotionPolicy
{
    /// <summary>Challenger must reduce duration MAE (log space) by at least this fraction.</summary>
    public double MinDurationImprovement { get; init; } = 0.01;
    /// <summary>Challenger may lose at most this much failure AUC.</summary>
    public double MaxAucRegression { get; init; } = 0.02;
}

public sealed record PromotionDecision(bool Promote, string Reason);

public static class ChampionChallenger
{
    public static PromotionDecision Decide(ModelMetrics? champion, ModelMetrics challenger, PromotionPolicy? policy = null)
    {
        policy ??= new PromotionPolicy();
        if (champion is null) return new(true, "No champion; first model promoted.");
        if (double.IsNaN(challenger.Duration.MaeLog)) return new(false, "Challenger has no duration metrics.");
        if (double.IsNaN(champion.Duration.MaeLog)) return new(true, "Champion could not be evaluated on the holdout.");

        var improvement = (champion.Duration.MaeLog - challenger.Duration.MaeLog) / champion.Duration.MaeLog;
        var aucDrop = double.IsNaN(champion.Failure.Auc) || double.IsNaN(challenger.Failure.Auc) ? 0 : champion.Failure.Auc - challenger.Failure.Auc;

        if (aucDrop > policy.MaxAucRegression)
            return new(false, $"Failure AUC regressed by {aucDrop:F3} (> {policy.MaxAucRegression:F3}).");
        if (improvement < policy.MinDurationImprovement)
            return new(false, $"Duration MAE improved {improvement:P1}, below required {policy.MinDurationImprovement:P1}.");
        return new(true, $"Duration MAE improved {improvement:P1}; AUC change {-aucDrop:+0.000;-0.000}.");
    }
}
