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
    public void Generator_never_overallocates_a_pool_and_grants_partially_under_contention()
    {
        var records = new WorkloadGenerator(new WorkloadOptions { Seed = 21, AccountCount = 5 }).GenerateRecords(3000, From, To).ToArray();

        foreach (var account in records.GroupBy(r => r.Request.AccountId))
        {
            var pool = Dtu.DefaultPoolSize(account.First().Request.Tier);
            Assert.All(account, r => Assert.Equal(pool, Dtu.DefaultPoolSize(r.Request.Tier)));
            foreach (var r in account)
            {
                var reserved = account.Where(o => o.StartedAt <= r.StartedAt && o.EndedAt > r.StartedAt).Sum(o => o.GrantedDtus);
                Assert.True(reserved <= pool, $"{account.Key} had {reserved}/{pool} DTUs reserved at {r.StartedAt:O}");
            }
        }

        Assert.All(records, r => Assert.InRange(r.GrantedDtus, 0, r.Request.RequestedDtus));
        Assert.All(records, r => Assert.InRange(r.PoolUtilizationAtStart, 0, 1));
        Assert.Contains(records, r => r.GrantedDtus > 0 && r.GrantedDtus < r.Request.RequestedDtus);
        Assert.All(records.Where(r => r.GrantedDtus == 0), r =>
        {
            Assert.True(r.Failed);
            Assert.Equal(FailureReason.QuotaExceeded, r.FailureReason);
        });
    }

    [Fact]
    public void Simulated_dtus_have_diminishing_returns()
    {
        var request = new TransferRequest(CloudProvider.S3, CloudRegion.UsEast, CloudProvider.AzureBlob, CloudRegion.UsEast, 100L << 30, 1000);
        var night = new DateTimeOffset(2026, 9, 2, 7, 0, 0, TimeSpan.Zero);
        double Seconds(int dtus) => TransferSimulator.ExpectedDurationSeconds(request, night, grantedDtus: dtus);
        double[] s = [Seconds(1), Seconds(2), Seconds(4), Seconds(8), Seconds(16), Seconds(32), Seconds(64), Seconds(128), Seconds(256)];
        for (var i = 1; i < s.Length; i++)
        {
            Assert.True(s[i] < s[i - 1], $"more DTUs should be faster (step {i})");
            Assert.True(s[i] > s[i - 1] / 2, $"doubling DTUs must not more than double speed (step {i})");
        }
        Assert.True(TransferSimulator.FailureLogit(request with { RequestedDtus = 256 }, night) > TransferSimulator.FailureLogit(request with { RequestedDtus = 64 }, night));
        Assert.True(TransferSimulator.FailureLogit(request, night, poolUtilization: 0.95) > TransferSimulator.FailureLogit(request, night));
    }

    [Fact]
    public void Request_validation_covers_dtus_and_account()
    {
        var ok = new TransferRequest(CloudProvider.S3, CloudRegion.UsEast, CloudProvider.S3, CloudRegion.UsEast, 1 << 20, 1);
        ok.Validate();
        Assert.Throws<ArgumentOutOfRangeException>(() => (ok with { RequestedDtus = 0 }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() => (ok with { RequestedDtus = Dtu.MaxPerTransfer + 1 }).Validate());
        Assert.Throws<ArgumentException>(() => (ok with { AccountId = "" }).Validate());
        Assert.Throws<ArgumentException>(() => (ok with { AccountId = "bad/id" }).Validate());
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
