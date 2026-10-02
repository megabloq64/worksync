namespace WorkSync.Domain;

/// <summary>Everything the models can tell a user about a transfer before it starts.</summary>
[GenerateSerializer, Immutable, Alias("worksync.TransferForecast")]
public sealed record TransferForecast(
    DateTimeOffset StartAt,
    TimeSpan ExpectedDuration,
    TimeSpan DurationP10,
    TimeSpan DurationP90,
    DateTimeOffset EstimatedCompletion,
    double FailureProbability,
    double ExpectedThroughputBytesPerSecond,
    int ModelVersion);

/// <summary>Result of scoring a finished transfer for "this was unusually slow".</summary>
[GenerateSerializer, Immutable, Alias("worksync.AnomalyResult")]
public sealed record AnomalyResult(
    bool IsSlowAnomaly,
    double Score,
    double Threshold,
    TimeSpan ExpectedDuration,
    TimeSpan ActualDuration,
    double SlowdownFactor);

[GenerateSerializer, Immutable, Alias("worksync.StartSlot")]
public sealed record StartSlot(
    DateTimeOffset StartUtc,
    DateTimeOffset StartSourceLocal,
    TimeSpan ExpectedDuration,
    double FailureProbability,
    double Score,
    TimeSpan SavingsVersusNow);

[GenerateSerializer, Immutable, Alias("worksync.HourlyOutlook")]
public sealed record HourlyOutlook(int SourceLocalHour, TimeSpan AverageExpectedDuration, double AverageFailureProbability);

[GenerateSerializer, Immutable, Alias("worksync.BestTimeAdvice")]
public sealed record BestTimeAdvice(
    StartSlot Now,
    IReadOnlyList<StartSlot> BestSlots,
    IReadOnlyList<HourlyOutlook> HourlyHeatmap,
    int ModelVersion);

[GenerateSerializer, Immutable, Alias("worksync.BestTimeQuery")]
public sealed record BestTimeQuery(
    TransferRequest Request,
    DateTimeOffset? From = null,
    int HorizonHours = 168,
    int Top = 5,
    double FailurePenalty = 2.0);
