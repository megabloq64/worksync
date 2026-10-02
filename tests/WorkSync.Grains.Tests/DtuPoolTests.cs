using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;
using WorkSync.Domain;
using WorkSync.Storage;

namespace WorkSync.Grains.Tests;

/// <summary>A clock tests can move forward, to expire DTU leases without waiting.</summary>
public sealed class ManualClock : TimeProvider
{
    private long _offsetTicks;

    public override DateTimeOffset GetUtcNow() => base.GetUtcNow() + TimeSpan.FromTicks(Interlocked.Read(ref _offsetTicks));

    public void Advance(TimeSpan by) => Interlocked.Add(ref _offsetTicks, by.Ticks);
}

/// <summary>A single silo with a manual clock; DTU pool tests don't need a model.</summary>
public sealed class DtuPoolFixture : IAsyncLifetime
{
    public ManualClock Clock { get; } = new();
    public InProcessTestCluster Cluster { get; }
    public IClusterClient Client => Cluster.Client;

    public DtuPoolFixture()
    {
        var builder = new InProcessTestClusterBuilder(1);
        builder.ConfigureSilo((_, silo) =>
        {
            silo.Services.AddSingleton<TimeProvider>(Clock);
            silo.Services.AddSingleton<IEventStore>(new InMemoryEventStore());
            silo.Services.AddSingleton<IModelRegistry>(new LocalFolderModelRegistry(Path.Combine(Path.GetTempPath(), "worksync-tests", Guid.NewGuid().ToString("N"))));
            silo.AddMemoryGrainStorage("worksync");
            silo.AddMemoryGrainStorage("PubSubStore");
            silo.UseInMemoryReminderService();
            silo.AddMemoryStreams(StreamNames.Provider);
            silo.AddWorkSyncGrains(isTrainer: false);
        });
        Cluster = builder.Build();
    }

    public async ValueTask InitializeAsync() => await Cluster.DeployAsync();

    public async ValueTask DisposeAsync() => await Cluster.DisposeAsync();
}

public sealed class DtuPoolTests(DtuPoolFixture fixture) : IClassFixture<DtuPoolFixture>
{
    private static readonly TimeSpan Lease = TimeSpan.FromMinutes(5);

    private IDtuPoolGrain Pool(string name) => fixture.Client.GetGrain<IDtuPoolGrain>($"{name}-{Guid.NewGuid():N}");

    [Fact]
    public async Task Reservations_draw_down_the_pool_and_partial_grants_take_what_is_left()
    {
        var pool = Pool("partial");
        var a = await pool.ReserveAsync(Guid.NewGuid(), 10, AccountTier.Standard, Lease);
        Assert.Equal(new { Requested = 10, Granted = 10, Size = 16 }, new { a.Requested, a.Granted, Size = a.PoolSize });
        Assert.False(a.IsPartial);
        Assert.Equal(0, a.UtilizationBefore);

        var b = await pool.ReserveAsync(Guid.NewGuid(), 10, AccountTier.Standard, Lease);
        Assert.Equal(6, b.Granted);
        Assert.True(b.IsPartial);
        Assert.Equal(10 / 16.0, b.UtilizationBefore, 6);

        var c = await pool.ReserveAsync(Guid.NewGuid(), 4, AccountTier.Standard, Lease);
        Assert.Equal(0, c.Granted);

        var status = await pool.GetStatusAsync();
        Assert.Equal((16, 16, 0, 2), (status.PoolSize, status.Reserved, status.Available, status.Reservations.Count));
        Assert.Equal(1.0, status.Utilization);
    }

    [Fact]
    public async Task Reserve_is_idempotent_and_release_returns_dtus()
    {
        var pool = Pool("idem");
        var id = Guid.NewGuid();
        var first = await pool.ReserveAsync(id, 3, AccountTier.Free, Lease);
        var again = await pool.ReserveAsync(id, 3, AccountTier.Free, Lease);
        Assert.Equal(first, again);
        Assert.Equal(3, (await pool.GetStatusAsync()).Reserved);

        Assert.True(await pool.ReleaseAsync(id));
        Assert.False(await pool.ReleaseAsync(id));
        var status = await pool.GetStatusAsync();
        Assert.Equal((4, 0, 4), (status.PoolSize, status.Reserved, status.Available));
    }

    [Fact]
    public async Task Expired_leases_are_reclaimed_and_renewal_keeps_them()
    {
        var pool = Pool("lease");
        var abandoned = Guid.NewGuid();
        var renewed = Guid.NewGuid();
        await pool.ReserveAsync(abandoned, 8, AccountTier.Standard, TimeSpan.FromMinutes(1));
        await pool.ReserveAsync(renewed, 8, AccountTier.Standard, TimeSpan.FromMinutes(1));
        Assert.True(await pool.RenewAsync(renewed, TimeSpan.FromHours(1)));

        fixture.Clock.Advance(TimeSpan.FromMinutes(2));

        var status = await pool.GetStatusAsync();
        Assert.Equal(8, status.Reserved);
        Assert.Equal(renewed, Assert.Single(status.Reservations).TransferId);
        Assert.False(await pool.RenewAsync(abandoned, Lease));
    }

    [Fact]
    public async Task Pool_size_follows_tier_and_invalid_requests_are_rejected()
    {
        var pool = Pool("tier");
        Assert.Equal(64, (await pool.GetStatusAsync(AccountTier.Premium)).PoolSize);
        var grant = await pool.ReserveAsync(Guid.NewGuid(), 200, AccountTier.Enterprise, Lease);
        Assert.Equal((200, 256), (grant.Granted, grant.PoolSize));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => pool.ReserveAsync(Guid.NewGuid(), 0, AccountTier.Standard, Lease));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => pool.ReserveAsync(Guid.NewGuid(), Dtu.MaxPerTransfer + 1, AccountTier.Standard, Lease));
    }
}
