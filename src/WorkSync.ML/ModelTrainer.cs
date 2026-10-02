using Microsoft.ML;
using Microsoft.ML.Trainers.FastTree;
using Microsoft.ML.Trainers.LightGbm;
using WorkSync.Domain;

namespace WorkSync.ML;

public sealed record TrainingSettings
{
    /// <summary>Fraction of the newest records held out for evaluation and champion/challenger comparison.</summary>
    public double HoldoutFraction { get; init; } = 0.15;
    public double RecencyHalfLifeDays { get; init; } = 14;
    public int MinimumRecords { get; init; } = 300;
    public int Iterations { get; init; } = 300;
    public int NumberOfLeaves { get; init; } = 31;
    public double LearningRate { get; init; } = 0.05;
    public int Seed { get; init; } = 7;
    /// <summary>Quantile of holdout residual z-scores above which a completed transfer is flagged as unusually slow.</summary>
    public double AnomalyQuantile { get; init; } = 0.99;
    /// <summary>Lower bound for the anomaly threshold so a very clean holdout cannot make the detector trigger-happy.</summary>
    public double MinimumAnomalyZ { get; init; } = 3.0;
}

public sealed record TrainingResult(ModelBundle Bundle, IReadOnlyList<TransferRecord> Holdout);

/// <summary>Trains the duration, failure, throughput and anomaly models from finished transfer records.</summary>
public sealed class ModelTrainer(TrainingSettings? settings = null)
{
    private readonly TrainingSettings _settings = settings ?? new TrainingSettings();

    public TrainingSettings Settings => _settings;

    public TrainingResult Train(IReadOnlyList<TransferRecord> records, DateTimeOffset? now = null, string notes = "")
    {
        if (records.Count < _settings.MinimumRecords)
        {
            throw new InsufficientTrainingDataException(records.Count, _settings.MinimumRecords);
        }

        var clock = now ?? DateTimeOffset.UtcNow;
        var ordered = records.OrderBy(r => r.EndedAt).ToArray();
        var holdoutCount = Math.Max(1, (int)(ordered.Length * _settings.HoldoutFraction));
        var train = ordered[..^holdoutCount];
        var holdout = ordered[^holdoutCount..];

        var ctx = new MLContext(_settings.Seed);
        var allRows = train.Select(r => FeatureBuilder.FromRecord(r, FeatureBuilder.RecencyWeight(r.EndedAt, clock, _settings.RecencyHalfLifeDays))).ToArray();
        var completedRows = allRows.Where(r => !r.Failed).ToArray();
        if (completedRows.Length < 20 || allRows.All(r => r.Failed) || allRows.All(r => !r.Failed))
        {
            throw new InsufficientTrainingDataException(completedRows.Length, 20, "Need both completed and failed transfers.");
        }

        var allData = ctx.Data.LoadFromEnumerable(allRows);
        var completedData = ctx.Data.LoadFromEnumerable(completedRows);

        var duration = Featurize(ctx)
            .Append(ctx.Regression.Trainers.LightGbm(new LightGbmRegressionTrainer.Options
            {
                LabelColumnName = nameof(TransferModelInput.LogDuration),
                FeatureColumnName = "Features",
                ExampleWeightColumnName = nameof(TransferModelInput.Weight),
                NumberOfIterations = _settings.Iterations,
                NumberOfLeaves = _settings.NumberOfLeaves,
                LearningRate = _settings.LearningRate,
                MinimumExampleCountPerLeaf = 10,
                Seed = _settings.Seed,
                Verbose = false,
                Silent = true,
            }))
            .Fit(completedData);

        var failure = Featurize(ctx)
            .Append(ctx.BinaryClassification.Trainers.LightGbm(new LightGbmBinaryTrainer.Options
            {
                LabelColumnName = nameof(TransferModelInput.Failed),
                FeatureColumnName = "Features",
                ExampleWeightColumnName = nameof(TransferModelInput.Weight),
                NumberOfIterations = Math.Max(50, _settings.Iterations / 2),
                NumberOfLeaves = Math.Max(8, _settings.NumberOfLeaves / 2),
                LearningRate = _settings.LearningRate,
                MinimumExampleCountPerLeaf = 20,
                Seed = _settings.Seed,
                Verbose = false,
                Silent = true,
            }))
            .Fit(allData);

        var throughput = Featurize(ctx)
            .Append(ctx.Regression.Trainers.FastTree(new FastTreeRegressionTrainer.Options
            {
                LabelColumnName = nameof(TransferModelInput.LogThroughput),
                FeatureColumnName = "Features",
                ExampleWeightColumnName = nameof(TransferModelInput.Weight),
                NumberOfTrees = Math.Max(50, _settings.Iterations / 2),
                NumberOfLeaves = _settings.NumberOfLeaves,
                LearningRate = _settings.LearningRate * 2,
                MinimumExampleCountPerLeaf = 10,
                Seed = _settings.Seed,
            }))
            .Fit(completedData);

        // Calibration from out-of-sample (holdout) log-duration residuals: in-sample residuals of a boosted model are
        // too tight and would make both the P10/P90 interval and the anomaly detector over-confident.
        var holdoutCompleted = holdout.Where(r => !r.Failed).ToArray();
        var calibrationSet = holdoutCompleted.Length >= 20 ? holdoutCompleted : train.Where(r => !r.Failed).ToArray();
        var calibrationPreds = Score(ctx, duration, ctx.Data.LoadFromEnumerable(calibrationSet.Select(r => FeatureBuilder.FromRecord(r))));
        var residuals = calibrationSet.Select((r, i) => Math.Log(r.DurationSeconds) - calibrationPreds[i]).ToArray();

        // Robust anomaly z-score: median / MAD are not dragged around by the very outliers we want to detect.
        var center = Quantile(residuals, 0.5);
        var scale = Math.Max(1e-3, 1.4826 * Quantile(residuals.Select(r => Math.Abs(r - center)).ToArray(), 0.5));
        var anomalyThreshold = Math.Max(_settings.MinimumAnomalyZ,
            Quantile(residuals.Select(r => (r - center) / scale).ToArray(), _settings.AnomalyQuantile));

        var partial = new ModelBundle(ctx, duration, failure, throughput, new BundleManifest(
            clock, train.Length, ordered[0].EndedAt, ordered[^1].EndedAt,
            Metrics: null!, Quantile(residuals, 0.10), Quantile(residuals, 0.90), center, scale, anomalyThreshold,
            TrainingMeanLogBytes: allRows.Average(r => r.LogBytes), notes, FeatureBuilder.SchemaVersion));

        var metrics = ModelEvaluator.Evaluate(partial, holdout);
        var bundle = new ModelBundle(ctx, duration, failure, throughput, partial.Manifest with { Metrics = metrics });
        return new TrainingResult(bundle, holdout);
    }

    private static IEstimator<ITransformer> Featurize(MLContext ctx) =>
        ctx.Transforms.Categorical.OneHotEncoding(
                FeatureBuilder.CategoricalColumns.Select(c => new InputOutputColumnPair(c + "Encoded", c)).ToArray())
            .Append(ctx.Transforms.Concatenate("Features",
                [.. FeatureBuilder.CategoricalColumns.Select(c => c + "Encoded"), .. FeatureBuilder.NumericColumns]));

    internal static double[] Score(MLContext ctx, ITransformer model, IDataView data) =>
        ctx.Data.CreateEnumerable<RegressionOutput>(model.Transform(data), reuseRowObject: false).Select(o => (double)o.Score).ToArray();

    internal static double Quantile(IReadOnlyList<double> values, double q)
    {
        if (values.Count == 0) return 0;
        var sorted = values.Order().ToArray();
        var pos = Math.Clamp(q, 0, 1) * (sorted.Length - 1);
        var lo = (int)Math.Floor(pos);
        var hi = (int)Math.Ceiling(pos);
        return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
    }
}

public sealed class InsufficientTrainingDataException(int available, int required, string? detail = null)
    : InvalidOperationException($"Not enough training data: {available} available, {required} required. {detail}".Trim())
{
    public int Available { get; } = available;
    public int Required { get; } = required;
}
