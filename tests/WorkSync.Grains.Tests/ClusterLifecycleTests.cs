using Orleans.Runtime;
using WorkSync.DataGen;
using WorkSync.Domain;
using WorkSync.Grains;

namespace WorkSync.Grains.Tests;

public sealed class ClusterLifecycleTests(ClusterFixture fixture) : IClassFixture<ClusterFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(90);

    private static readonly TransferRequest Request =
        new(CloudProvider.S3, CloudRegion.UsEast, CloudProvider.AzureBlob, CloudRegion.EuWest, 20L << 30, 2_000, RequestedDtus: 8, AccountId: "acct-lifecycle");

    /// <summary>
    /// One end-to-end scenario (the steps depend on each other): no model → train → model on every silo →
    /// run a transfer through the event pipeline → retrain → rollback.
    /// </summary>
    [Fact]
    public async Task Train_predict_transfer_and_rollback_across_two_silos()
    {
        var client = fixture.Client;
        var coordinator = client.GetGrain<ITrainingCoordinatorGrain>(WellKnownKeys.Coordinator);
        var predictor = client.GetGrain<IPredictionGrain>(0);

        // 1. Nothing trained yet.
        await Assert.ThrowsAsync<ModelNotReadyException>(() => predictor.ForecastAsync(Request));
        Assert.Null(await coordinator.GetChampionVersionAsync());

        // 2. Seed history and train. The first model is always promoted.
        var now = DateTimeOffset.UtcNow;
        var gen = new WorkloadGenerator(new WorkloadOptions { Seed = 7 });
        foreach (var seeded in gen.GenerateRecords(1_500, now.AddDays(-20), now))
        {
            await fixture.Store.SaveRecordAsync(seeded, TestContext.Current.CancellationToken);
        }
        await coordinator.RequestTrainingAsync("test: initial", force: true);
        var champion = await ClusterFixture.WaitForAsync(coordinator.GetChampionVersionAsync, v => v is not null, Timeout, "first champion");
        Assert.Equal(1, champion);

        // 3. Every silo (including the non-trainer) loads it.
        var hosts = await client.GetGrain<IManagementGrain>(0).GetHosts(onlyActive: true);
        Assert.Equal(2, hosts.Count);
        foreach (var silo in hosts.Keys)
        {
            var loaded = await client.GetGrain<IModelSyncGrain>(silo.ToParsableString()).ActivateVersionAsync(1);
            Assert.Equal(1, loaded);
        }

        var forecast = await predictor.ForecastAsync(Request);
        Assert.Equal(1, forecast.ModelVersion);
        Assert.True(forecast.DurationP10 <= forecast.ExpectedDuration && forecast.ExpectedDuration <= forecast.DurationP90);
        Assert.InRange(forecast.FailureProbability, 0, 1);
        Assert.True(forecast.ExpectedThroughputBytesPerSecond > 0);
        Assert.Equal(8, forecast.GrantedDtus);
        Assert.True(forecast.DtuHoursP10 <= forecast.DtuHoursExpected && forecast.DtuHoursExpected <= forecast.DtuHoursP90);

        var dtuAdvice = await predictor.AdviseDtusAsync(Request);
        Assert.Equal(16, dtuAdvice.PoolSize);
        Assert.All(dtuAdvice.Options, o => Assert.InRange(o.Dtus, 1, 16));
        Assert.Contains(dtuAdvice.Recommended, dtuAdvice.Options);

        var advice = await predictor.BestTimeAsync(new BestTimeQuery(Request, HorizonHours: 48, Top: 3));
        Assert.Equal(3, advice.BestSlots.Count);
        Assert.Equal(24, advice.HourlyHeatmap.Count);

        // 4. A simulated transfer streams its events through the ingestor into a finished record.
        var transferId = Guid.NewGuid();
        var transfer = client.GetGrain<ITransferGrain>(transferId);
        var started = await transfer.StartAsync(Request);
        Assert.NotNull(started.Forecast);
        Assert.NotNull(started.Dtus);
        Assert.Equal(8, started.Dtus.Granted);
        Assert.Equal(started.Dtus.Granted, started.Forecast.GrantedDtus);
        var finished = await ClusterFixture.WaitForAsync(transfer.GetStatusAsync,
            s => s.State is TransferState.Completed or TransferState.Failed, Timeout, "transfer to finish");
        var pool = client.GetGrain<IDtuPoolGrain>(Request.AccountId);
        await ClusterFixture.WaitForAsync(() => pool.GetStatusAsync(), p => p.Reserved == 0, Timeout, "DTUs to be released");
        Assert.Equal(1.0, finished.ProgressFraction, 3);
        var record = await ClusterFixture.WaitForAsync(() => fixture.Store.GetRecordAsync(transferId, TestContext.Current.CancellationToken),
            r => r is not null, Timeout, "ingested record");
        Assert.Equal(Request.TotalBytes, record!.Request.TotalBytes);
        Assert.Equal(8, record.GrantedDtus);
        Assert.NotEmpty(await fixture.Store.GetEventsAsync(transferId, TestContext.Current.CancellationToken));

        var stats = await ClusterFixture.WaitForAsync(coordinator.GetStatusAsync,
            s => s.RecordsSinceLastTraining >= 1, Timeout, "stats to reach the coordinator");
        Assert.Equal(1, stats.ChampionVersion);

        // 5. A second run always publishes a version (promoted or kept); rollback moves the pointer back.
        await coordinator.RequestTrainingAsync("test: retrain", force: true);
        await ClusterFixture.WaitForAsync(coordinator.ListModelsAsync, m => m.Count == 2, Timeout, "second version");
        await ClusterFixture.WaitForAsync(coordinator.GetStatusAsync, s => !s.IsTraining, Timeout, "training to finish");

        await coordinator.RollbackAsync(1);
        Assert.Equal(1, await coordinator.GetChampionVersionAsync());
        Assert.Equal(1, await fixture.Registry.GetCurrentVersionAsync(TestContext.Current.CancellationToken));
        var status = await coordinator.GetStatusAsync();
        Assert.Contains(status.History, h => h.Reason == "test: initial" && h.Promoted);
        Assert.Contains(status.History, h => h.Reason == "test: retrain" && h.Version == 2);

        await Assert.ThrowsAnyAsync<Exception>(() => coordinator.RollbackAsync(99));
    }
}
