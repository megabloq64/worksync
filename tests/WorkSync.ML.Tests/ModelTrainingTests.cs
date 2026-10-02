using WorkSync.DataGen;
using WorkSync.Domain;
using WorkSync.ML;

namespace WorkSync.ML.Tests;

public sealed class ModelTrainingTests(TrainedModelFixture fixture) : IClassFixture<TrainedModelFixture>
{
    private static readonly TransferRequest BigRequest = new(
        CloudProvider.Dropbox, CloudRegion.UsEast, CloudProvider.S3, CloudRegion.UsEast,
        TotalBytes: 200L << 30, FileCount: 50_000, Concurrency: 8, AccountTier.Standard);

    [Fact]
    public void Duration_model_explains_most_variance()
    {
        var m = fixture.Result.Bundle.Manifest.Metrics;
        Assert.True(m.Duration.RSquared > 0.85, $"R² was {m.Duration.RSquared:F3}");
        Assert.True(m.Duration.MedianAbsPercentError < 0.35, $"Median APE was {m.Duration.MedianAbsPercentError:P1}");
    }

    [Fact]
    public void Failure_model_beats_random()
    {
        var m = fixture.Result.Bundle.Manifest.Metrics;
        Assert.True(m.Failure.Auc > 0.65, $"AUC was {m.Failure.Auc:F3}");
    }

    [Fact]
    public void Throughput_model_is_predictive()
    {
        Assert.True(fixture.Result.Bundle.Manifest.Metrics.Throughput.RSquared > 0.6);
    }

    [Fact]
    public void Forecast_is_ordered_and_plausible()
    {
        var start = new DateTimeOffset(2026, 9, 2, 3, 0, 0, TimeSpan.Zero);
        var f = fixture.Predictor.Forecast(BigRequest, start);

        Assert.True(f.DurationP10 <= f.ExpectedDuration && f.ExpectedDuration <= f.DurationP90);
        Assert.InRange(f.FailureProbability, 0, 1);
        Assert.True(f.ExpectedThroughputBytesPerSecond > 0);
        Assert.Equal(start + f.ExpectedDuration, f.EstimatedCompletion);

        // Within a factor of 2 of the simulator's own deterministic expectation.
        var physicsSeconds = TransferSimulator.ExpectedDurationSeconds(BigRequest, start);
        var ratio = f.ExpectedDuration.TotalSeconds / physicsSeconds;
        Assert.InRange(ratio, 0.5, 2.0);
    }

    [Fact]
    public void Bigger_transfers_take_longer()
    {
        var start = new DateTimeOffset(2026, 9, 2, 3, 0, 0, TimeSpan.Zero);
        var small = fixture.Predictor.Forecast(BigRequest with { TotalBytes = 1L << 30, FileCount = 200 }, start);
        var big = fixture.Predictor.Forecast(BigRequest, start);
        Assert.True(big.ExpectedDuration > small.ExpectedDuration * 5);
    }

    [Fact]
    public void Best_time_avoids_source_business_hours()
    {
        // Monday 00:00 UTC => Sunday evening in New York; horizon covers the working week.
        var from = new DateTimeOffset(2026, 9, 7, 0, 0, 0, TimeSpan.Zero);
        var advice = BestStartTimeAdvisor.Advise(fixture.Predictor, new BestTimeQuery(BigRequest, from, HorizonHours: 72, Top: 3), from);

        Assert.Equal(3, advice.BestSlots.Count);
        Assert.Equal(24, advice.HourlyHeatmap.Count);
        Assert.All(advice.BestSlots, s => Assert.False(s.StartSourceLocal.Hour is >= 9 and < 18, $"Picked business hour {s.StartSourceLocal}"));

        var peak = advice.HourlyHeatmap.Single(h => h.SourceLocalHour == 15).AverageExpectedDuration;
        var night = advice.HourlyHeatmap.Single(h => h.SourceLocalHour == 3).AverageExpectedDuration;
        Assert.True(peak > night * 1.2, $"peak {peak} vs night {night}");
    }

    [Fact]
    public void Anomaly_flags_very_slow_transfer_but_not_typical_one()
    {
        var typical = fixture.Result.Holdout.First(r => !r.Failed && r.Retries == 0);
        var expected = fixture.Predictor.Forecast(typical.Request, typical.StartedAt).ExpectedDuration;

        var normal = typical with { EndedAt = typical.StartedAt + expected };
        var slow = typical with { EndedAt = typical.StartedAt + expected * 8 };

        Assert.False(fixture.Predictor.ScoreAnomaly(normal).IsSlowAnomaly);
        var result = fixture.Predictor.ScoreAnomaly(slow);
        Assert.True(result.IsSlowAnomaly, $"score {result.Score} threshold {result.Threshold}");
        Assert.InRange(result.SlowdownFactor, 7.5, 8.5);
    }

    [Fact]
    public void Bundle_round_trips_through_disk()
    {
        var dir = Path.Combine(Path.GetTempPath(), "worksync-tests", Guid.NewGuid().ToString("N"));
        try
        {
            fixture.Result.Bundle.Save(dir);
            var loaded = new TransferPredictor(ModelBundle.Load(dir), 2);
            var start = new DateTimeOffset(2026, 9, 2, 12, 0, 0, TimeSpan.Zero);
            var a = fixture.Predictor.Forecast(BigRequest, start);
            var b = loaded.Forecast(BigRequest, start);
            Assert.Equal(a.ExpectedDuration.TotalSeconds, b.ExpectedDuration.TotalSeconds, 3);
            Assert.Equal(a.FailureProbability, b.FailureProbability, 5);
            Assert.Equal(fixture.Result.Bundle.Manifest, ModelBundle.ReadManifest(dir));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Champion_challenger_rules()
    {
        var champion = fixture.Result.Bundle.Manifest.Metrics;
        Assert.True(ChampionChallenger.Decide(null, champion).Promote);
        Assert.False(ChampionChallenger.Decide(champion, champion).Promote);

        var better = champion with { Duration = champion.Duration with { MaeLog = champion.Duration.MaeLog * 0.8 } };
        Assert.True(ChampionChallenger.Decide(champion, better).Promote);

        var worseAuc = better with { Failure = better.Failure with { Auc = champion.Failure.Auc - 0.1 } };
        Assert.False(ChampionChallenger.Decide(champion, worseAuc).Promote);
    }

    [Fact]
    public void Too_little_data_is_rejected()
    {
        Assert.Throws<InsufficientTrainingDataException>(() => new ModelTrainer().Train(fixture.Records.Take(10).ToArray()));
    }
}
