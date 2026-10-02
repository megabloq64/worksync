namespace WorkSync.ML;

[GenerateSerializer, Immutable, Alias("worksync.DurationMetrics")]
public sealed record DurationMetrics(double RSquared, double MaeLog, double RmseLog, double MedianAbsPercentError);

[GenerateSerializer, Immutable, Alias("worksync.FailureMetrics")]
public sealed record FailureMetrics(double Auc, double F1, double LogLoss, double PositiveRate);

[GenerateSerializer, Immutable, Alias("worksync.ThroughputMetrics")]
public sealed record ThroughputMetrics(double RSquared, double MaeLog);

[GenerateSerializer, Immutable, Alias("worksync.ModelMetrics")]
public sealed record ModelMetrics(DurationMetrics Duration, FailureMetrics Failure, ThroughputMetrics Throughput, int HoldoutRows);

/// <summary>
/// Everything persisted next to the model files of a bundle. Anomaly scoring is a robust z-score of the log-duration
/// residual: (residual - AnomalyCenter) / AnomalyScale, flagged when above AnomalyThreshold.
/// </summary>
[GenerateSerializer, Immutable, Alias("worksync.BundleManifest")]
public sealed record BundleManifest(
    DateTimeOffset TrainedAt,
    int TrainingRows,
    DateTimeOffset WindowStart,
    DateTimeOffset WindowEnd,
    ModelMetrics Metrics,
    double ResidualP10,
    double ResidualP90,
    double AnomalyCenter,
    double AnomalyScale,
    double AnomalyThreshold,
    double TrainingMeanLogBytes,
    string Notes = "");

[GenerateSerializer, Immutable, Alias("worksync.ModelVersionInfo")]
public sealed record ModelVersionInfo(int Version, DateTimeOffset PublishedAt, BundleManifest Manifest, bool IsCurrent);
