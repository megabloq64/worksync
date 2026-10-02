using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using WorkSync.DataGen;
using WorkSync.Domain;

namespace WorkSync.ML.Tests;

public sealed class DomainAndDataTests
{
    private static readonly DateTimeOffset From = new(2026, 6, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset To = From.AddDays(30);

    [Fact]
    public void Generator_is_deterministic_for_a_seed()
    {
        var a = new WorkloadGenerator(new WorkloadOptions { Seed = 9 }).GenerateRecords(50, From, To).ToArray();
        var b = new WorkloadGenerator(new WorkloadOptions { Seed = 9 }).GenerateRecords(50, From, To).ToArray();
        var c = new WorkloadGenerator(new WorkloadOptions { Seed = 10 }).GenerateRecords(50, From, To).ToArray();
        Assert.Equal(a, b);
        Assert.NotEqual(a, c);
    }

    [Fact]
    public void Events_aggregate_back_into_the_same_record_even_with_duplicates_and_reordering()
    {
        var gen = new WorkloadGenerator(new WorkloadOptions { Seed = 3 });
        foreach (var record in gen.GenerateRecords(40, From, To))
        {
            var events = gen.ToEvents(record);
            var shuffled = events.Concat(events.Take(2)).Reverse().ToArray();
            Assert.Equal(record, TransferAggregator.TryAggregate(shuffled));
            Assert.Null(TransferAggregator.TryAggregate(events.Take(events.Count - 1)));
        }
    }

    [Fact]
    public async Task Csv_round_trips()
    {
        var records = new WorkloadGenerator(new WorkloadOptions { Seed = 5 }).GenerateRecords(100, From, To).ToArray();
        var path = Path.Combine(Path.GetTempPath(), $"worksync-{Guid.NewGuid():N}.csv");
        try
        {
            await TransferRecordCsv.WriteAsync(path, records, TestContext.Current.CancellationToken);
            var read = new List<TransferRecord>();
            await foreach (var r in TransferRecordCsv.ReadAsync(path, TestContext.Current.CancellationToken)) read.Add(r);
            Assert.Equal(records, read);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Drift_profile_parses_and_rejects_garbage()
    {
        var drift = DriftProfile.Parse("dropbox=0.5, S3=0.8");
        var r = new TransferRequest(CloudProvider.Dropbox, CloudRegion.UsEast, CloudProvider.S3, CloudRegion.UsEast, 1 << 30, 10);
        Assert.Equal(0.4, drift.For(r), 6);
        Assert.Same(DriftProfile.None, DriftProfile.Parse(""));
        Assert.Throws<FormatException>(() => DriftProfile.Parse("Nope=1"));
        Assert.Throws<FormatException>(() => DriftProfile.Parse("S3=-1"));
    }

    [Fact]
    public void Features_use_source_local_time()
    {
        var request = new TransferRequest(CloudProvider.S3, CloudRegion.AsiaEast, CloudProvider.S3, CloudRegion.UsWest, 1 << 30, 10);
        // 02:00 UTC on a Wednesday is 10:00 in Hong Kong and 19:00 on Tuesday in Los Angeles (PDT).
        var input = FeatureBuilder.FromRequest(request, new DateTimeOffset(2026, 9, 2, 2, 0, 0, TimeSpan.Zero));
        Assert.Equal(10, input.SourceLocalHour, 3);
        Assert.Equal(1, input.SourceIsBusinessHours);
        Assert.Equal(19, input.DestinationLocalHour, 3);
        Assert.Equal(0, input.DestinationIsBusinessHours);
    }

    [Fact]
    public void Events_round_trip_through_polymorphic_json()
    {
        var gen = new WorkloadGenerator(new WorkloadOptions { Seed = 11 });
        var events = gen.ToEvents(gen.GenerateRecords(1, From, To).Single());
        var json = JsonSerializer.Serialize(events);
        Assert.Contains("\"type\":\"started\"", json, StringComparison.Ordinal);
        Assert.Equal(events, JsonSerializer.Deserialize<TransferEvent[]>(json));
    }

    [Fact]
    public void Events_and_records_round_trip_through_orleans_serializer()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer>();
        var gen = new WorkloadGenerator(new WorkloadOptions { Seed = 12 });
        var record = gen.GenerateRecords(1, From, To).Single();

        Assert.Equal(record, serializer.Deserialize<TransferRecord>(serializer.SerializeToArray(record)));
        foreach (var e in gen.ToEvents(record))
        {
            Assert.Equal(e, serializer.Deserialize<TransferEvent>(serializer.SerializeToArray<TransferEvent>(e)));
        }
    }
}
