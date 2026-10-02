using WorkSync.DataGen;
using WorkSync.Domain;
using WorkSync.ML;

namespace WorkSync.ML.Tests;

public sealed class ModelTrainingTests(TrainedModelFixture fixture) : IClassFixture<TrainedModelFixture>
{
    private static readonly TransferRequest BigRequest = new(
        CloudProvider.Dropbox, CloudRegion.UsEast, CloudProvider.S3, CloudRegion.UsEast,
        TotalBytes: 200L << 30, FileCount: 50_000, RequestedDtus: 8, AccountTier.Standard);

    // Object store to object store with large files: throughput is DTU-bound, not API-bound.
    private static readonly TransferRequest BulkRequest = new(
        CloudProvider.S3, CloudRegion.UsEast, CloudProvider.AzureBlob, CloudRegion.UsEast,
        TotalBytes: 200L << 30, FileCount: 2_000, RequestedDtus: 8, AccountTier.Enterprise);

    private static readonly DateTimeOffset Night = new(2026, 9, 2, 7, 0, 0, TimeSpan.Zero); // 03:00 in New York

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
    public void More_dtus_are_faster_with_diminishing_returns()
    {
        TransferForecast At(int dtus) => fixture.Predictor.Forecast(BulkRequest with { RequestedDtus = dtus }, Night);
        var (one, eight, sixtyFour) = (At(1), At(8), At(64));

        Assert.True(one.ExpectedDuration > eight.ExpectedDuration * 2, $"1 DTU {one.ExpectedDuration} vs 8 DTUs {eight.ExpectedDuration}");
        Assert.True(eight.ExpectedDuration > sixtyFour.ExpectedDuration * 1.2, $"8 DTUs {eight.ExpectedDuration} vs 64 DTUs {sixtyFour.ExpectedDuration}");
        // Diminishing returns: 8x the DTUs never gives 8x the speed, so the bigger allocation burns more DTU-hours.
        Assert.True(sixtyFour.DtuHoursExpected > eight.DtuHoursExpected, $"{sixtyFour.DtuHoursExpected} vs {eight.DtuHoursExpected}");
        Assert.Equal(64, sixtyFour.GrantedDtus);
        Assert.Equal(64 * sixtyFour.ExpectedDuration.TotalHours, sixtyFour.DtuHoursExpected, 6);
        Assert.True(sixtyFour.DtuHoursP10 <= sixtyFour.DtuHoursExpected && sixtyFour.DtuHoursExpected <= sixtyFour.DtuHoursP90);
    }

    [Fact]
    public void Partial_grant_is_slower_than_full_grant()
    {
        var request = BulkRequest with { RequestedDtus = 64 };
        var full = fixture.Predictor.Forecast(request, Night);
        var partial = fixture.Predictor.Forecast(request, Night, grantedDtus: 4, poolUtilization: 0.9);
        Assert.Equal(4, partial.GrantedDtus);
        Assert.True(partial.ExpectedDuration > full.ExpectedDuration * 2, $"partial {partial.ExpectedDuration} vs full {full.ExpectedDuration}");
    }

    [Fact]
    public void Dtu_advice_is_consistent()
    {
        var advice = DtuAdvisor.Advise(fixture.Predictor, BulkRequest, Night, poolSize: 256, poolAvailable: 256);

        Assert.Equal(advice.Options.Select(o => o.Dtus).Order(), advice.Options.Select(o => o.Dtus));
        Assert.Contains(advice.Options, o => o.Dtus == 1);
        Assert.Contains(advice.Options, o => o.Dtus == 256);
        Assert.Equal(advice.Options.Min(o => o.ExpectedDuration), advice.Fastest.ExpectedDuration);
        Assert.Equal(advice.Options.Min(o => o.DtuHours), advice.MostEfficient.DtuHours);
        Assert.True(advice.Recommended.ExpectedDuration.TotalSeconds <= advice.Fastest.ExpectedDuration.TotalSeconds * DtuAdvisor.RecommendationTolerance);
        Assert.True(advice.Recommended.Dtus <= advice.Fastest.Dtus);
        Assert.True(advice.MostEfficient.Dtus < advice.Fastest.Dtus, "the cheapest allocation should not also be the fastest");
        Assert.All(advice.Options, o => Assert.True(o.AvailableNow));
    }

    [Fact]
    public void Dtu_advice_is_capped_by_pool_and_explains_contention()
    {
        var advice = DtuAdvisor.Advise(fixture.Predictor, BulkRequest with { Tier = AccountTier.Standard }, Night, poolSize: 16, poolAvailable: 2);

        Assert.Equal(16, advice.Options.Max(o => o.Dtus));
        Assert.Equal(2, advice.PoolAvailable);
        Assert.Equal(advice.Options.Where(o => o.Dtus <= 2).Select(o => o.Dtus), advice.Options.Where(o => o.AvailableNow).Select(o => o.Dtus));
        if (!advice.Recommended.AvailableNow) Assert.Contains("partial grant", advice.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void Bundles_from_an_older_feature_schema_are_refused()
    {
        var dir = Path.Combine(Path.GetTempPath(), "worksync-tests", Guid.NewGuid().ToString("N"));
        try
        {
            var b = fixture.Result.Bundle;
            new ModelBundle(b.Context, b.Duration, b.Failure, b.Throughput, b.Manifest with { FeatureSchemaVersion = 1 }).Save(dir);
            Assert.False(ModelBundle.IsCompatible(ModelBundle.ReadManifest(dir)));
            Assert.Throws<IncompatibleModelException>(() => ModelBundle.Load(dir));
            Assert.Equal(FeatureBuilder.SchemaVersion, b.Manifest.FeatureSchemaVersion);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Too_little_data_is_rejected()
    {
        Assert.Throws<InsufficientTrainingDataException>(() => new ModelTrainer().Train(fixture.Records.Take(10).ToArray()));
    }
}
