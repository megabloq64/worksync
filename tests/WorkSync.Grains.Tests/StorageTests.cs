using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;
using WorkSync.DataGen;
using WorkSync.Hosting;
using WorkSync.ML;
using WorkSync.Storage;

namespace WorkSync.Grains.Tests;

public abstract class EventStoreContract
{
    protected abstract IEventStore Store { get; }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Events_are_idempotent_and_ordered()
    {
        var gen = new WorkloadGenerator(new WorkloadOptions { Seed = 3 });
        var record = gen.GenerateRecords(1, DateTimeOffset.UtcNow.AddHours(-2), DateTimeOffset.UtcNow).Single();
        var events = gen.ToEvents(record, progressEvents: 3);

        foreach (var e in events.Reverse()) Assert.True(await Store.AppendAsync(e, Ct));
        Assert.False(await Store.AppendAsync(events[0], Ct));

        var stored = await Store.GetEventsAsync(record.TransferId, Ct);
        Assert.Equal(events.Select(e => e.Sequence), stored.Select(e => e.Sequence));
    }

    [Fact]
    public async Task Records_are_idempotent_and_queryable_by_end_time()
    {
        var now = DateTimeOffset.UtcNow;
        var gen = new WorkloadGenerator(new WorkloadOptions { Seed = 4 });
        var records = gen.GenerateRecords(50, now.AddDays(-10), now.AddDays(-1)).ToList();
        foreach (var r in records) Assert.True(await Store.SaveRecordAsync(r, Ct));
        Assert.False(await Store.SaveRecordAsync(records[0], Ct));

        var from = now.AddDays(-30);
        var to = now.AddDays(30);
        var all = await Store.ReadRecordsToListAsync(from, to, Ct);
        var ours = all.Where(r => records.Any(x => x.TransferId == r.TransferId)).ToList();
        Assert.Equal(records.Count, ours.Count);
        Assert.Equal(ours.OrderBy(r => r.EndedAt).Select(r => r.TransferId), ours.Select(r => r.TransferId));
        Assert.True(await Store.CountRecordsAsync(from, to, Ct) >= records.Count);

        var fetched = await Store.GetRecordAsync(records[5].TransferId, Ct);
        Assert.Equal(records[5].Request, fetched!.Request);
        Assert.Equal(records[5].EndedAt, fetched.EndedAt);
        Assert.Equal(records[5].Failed, fetched.Failed);
        Assert.Null(await Store.GetRecordAsync(Guid.NewGuid(), Ct));
    }
}

public sealed class InMemoryEventStoreTests : EventStoreContract
{
    protected override IEventStore Store { get; } = new InMemoryEventStore();
}

public sealed class GarnetEventStoreTests : EventStoreContract, IDisposable
{
    private readonly EmbeddedGarnetServer _server;
    private readonly ConnectionMultiplexer _connection;

    public GarnetEventStoreTests()
    {
        var port = FreePort();
        _server = new EmbeddedGarnetServer(port, NullLoggerFactory.Instance);
        _connection = ConnectionMultiplexer.Connect($"127.0.0.1:{port}");
        Store = new GarnetEventStore(_connection, new GarnetEventStoreOptions { KeyPrefix = $"t{Guid.NewGuid():N}:" });
    }

    protected override IEventStore Store { get; }

    public void Dispose()
    {
        _connection.Dispose();
        _server.Dispose();
    }

    private static int FreePort()
    {
        using var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        return ((IPEndPoint)l.LocalEndpoint).Port;
    }
}

public sealed class LocalFolderModelRegistryTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "worksync-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch (IOException) { }
    }

    [Fact]
    public async Task Publish_promote_and_reload()
    {
        var ct = TestContext.Current.CancellationToken;
        var now = DateTimeOffset.UtcNow;
        var records = new WorkloadGenerator(new WorkloadOptions { Seed = 5 }).GenerateRecords(600, now.AddDays(-10), now).ToList();
        var bundle = new ModelTrainer(new TrainingSettings { Iterations = 50 }).Train(records, now).Bundle;

        var registry = new LocalFolderModelRegistry(_root);
        Assert.Null(await registry.GetCurrentVersionAsync(ct));
        Assert.Equal(1, await registry.PublishAsync(bundle, ct));
        Assert.Equal(2, await registry.PublishAsync(bundle, ct));
        Assert.Null(await registry.GetCurrentVersionAsync(ct));

        await registry.SetCurrentVersionAsync(2, ct);
        var list = await registry.ListAsync(ct);
        Assert.Equal([2, 1], list.Select(v => v.Version));
        Assert.True(list[0].IsCurrent);

        var reloaded = new TransferPredictor(await registry.LoadAsync(1, ct), version: 1);
        Assert.True(reloaded.Forecast(records[0].Request, now).ExpectedDuration > TimeSpan.Zero);

        await Assert.ThrowsAsync<ModelVersionNotFoundException>(() => registry.LoadAsync(42, ct));
        await Assert.ThrowsAsync<ModelVersionNotFoundException>(() => registry.SetCurrentVersionAsync(42, ct));
    }
}
