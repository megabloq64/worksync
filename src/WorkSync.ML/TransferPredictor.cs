using Microsoft.Extensions.ObjectPool;
using Microsoft.ML;
using WorkSync.Domain;

namespace WorkSync.ML;

/// <summary>
/// Thread-safe scoring facade over a <see cref="ModelBundle"/>. PredictionEngine is not thread-safe, so each model
/// has its own object pool of engines.
/// </summary>
public sealed class TransferPredictor
{
    private readonly ObjectPool<PredictionEngine<TransferModelInput, RegressionOutput>> _duration;
    private readonly ObjectPool<PredictionEngine<TransferModelInput, BinaryOutput>> _failure;
    private readonly ObjectPool<PredictionEngine<TransferModelInput, RegressionOutput>> _throughput;

    public TransferPredictor(ModelBundle bundle, int version)
    {
        Bundle = bundle;
        Version = version;
        var provider = new DefaultObjectPoolProvider { MaximumRetained = Environment.ProcessorCount * 2 };
        var ctx = bundle.Context;
        _duration = provider.Create(new EnginePolicy<TransferModelInput, RegressionOutput>(() => ctx.Model.CreatePredictionEngine<TransferModelInput, RegressionOutput>(bundle.Duration)));
        _failure = provider.Create(new EnginePolicy<TransferModelInput, BinaryOutput>(() => ctx.Model.CreatePredictionEngine<TransferModelInput, BinaryOutput>(bundle.Failure)));
        _throughput = provider.Create(new EnginePolicy<TransferModelInput, RegressionOutput>(() => ctx.Model.CreatePredictionEngine<TransferModelInput, RegressionOutput>(bundle.Throughput)));
    }

    public ModelBundle Bundle { get; }
    public int Version { get; }

    /// <param name="grantedDtus">DTUs the pool can actually reserve; defaults to a full grant of the requested DTUs.</param>
    /// <param name="poolUtilization">Fraction of the account pool already reserved at start.</param>
    public TransferForecast Forecast(TransferRequest request, DateTimeOffset startUtc, int? grantedDtus = null, double poolUtilization = 0)
    {
        request.Validate();
        var granted = Math.Clamp(grantedDtus ?? request.RequestedDtus, 0, request.RequestedDtus);
        var input = FeatureBuilder.FromRequest(request, startUtc, granted, poolUtilization);
        var logDuration = Predict(_duration, input).Score;
        var pFail = Predict(_failure, input).Probability;
        var logThroughput = Predict(_throughput, input).Score;

        var m = Bundle.Manifest;
        var expected = TimeSpan.FromSeconds(Math.Exp(logDuration));
        var p10 = TimeSpan.FromSeconds(Math.Exp(logDuration + m.ResidualP10));
        var p90 = TimeSpan.FromSeconds(Math.Exp(logDuration + m.ResidualP90));
        return new TransferForecast(
            startUtc,
            expected,
            p10,
            p90,
            startUtc + expected,
            Math.Clamp(pFail, 0, 1),
            Math.Exp(logThroughput),
            Version,
            granted,
            Dtu.Hours(granted, expected),
            Dtu.Hours(granted, p10),
            Dtu.Hours(granted, p90));
    }

    /// <summary>(log duration, failure probability) only. Used by the advisors' hot loops. Assumes a full DTU grant unless told otherwise.</summary>
    public (double LogDuration, double FailureProbability) ScoreFast(TransferRequest request, DateTimeOffset startUtc, int? grantedDtus = null, double poolUtilization = 0)
    {
        var input = FeatureBuilder.FromRequest(request, startUtc, grantedDtus, poolUtilization);
        return (Predict(_duration, input).Score, Math.Clamp(Predict(_failure, input).Probability, 0, 1));
    }

    /// <summary>Scores a finished (or in-flight, using elapsed time) transfer against what the duration model expected.</summary>
    public AnomalyResult ScoreAnomaly(TransferRecord record)
    {
        var input = FeatureBuilder.FromRecord(record);
        var logDuration = (double)Predict(_duration, input).Score;
        var m = Bundle.Manifest;
        var residual = Math.Log(record.DurationSeconds) - logDuration;
        var score = (residual - m.AnomalyCenter) / m.AnomalyScale;
        var expected = TimeSpan.FromSeconds(Math.Exp(logDuration));
        var actual = TimeSpan.FromSeconds(record.DurationSeconds);
        // Only flag the slow direction: a transfer that is unusually fast is not a problem.
        var slow = !record.Failed && score > m.AnomalyThreshold;
        return new AnomalyResult(slow, score, m.AnomalyThreshold, expected, actual, actual / expected);
    }

    private static TOut Predict<TIn, TOut>(ObjectPool<PredictionEngine<TIn, TOut>> pool, TIn input)
        where TIn : class where TOut : class, new()
    {
        var engine = pool.Get();
        try
        {
            return engine.Predict(input);
        }
        finally
        {
            pool.Return(engine);
        }
    }

    private sealed class EnginePolicy<TIn, TOut>(Func<PredictionEngine<TIn, TOut>> factory) : IPooledObjectPolicy<PredictionEngine<TIn, TOut>>
        where TIn : class where TOut : class, new()
    {
        public PredictionEngine<TIn, TOut> Create() => factory();
        public bool Return(PredictionEngine<TIn, TOut> obj) => true;
    }
}
