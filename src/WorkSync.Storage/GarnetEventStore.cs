using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using StackExchange.Redis;
using WorkSync.Domain;

namespace WorkSync.Storage;

public sealed record GarnetEventStoreOptions
{
    public string KeyPrefix { get; init; } = "wsync";
    /// <summary>Raw events are only needed until the transfer is aggregated (plus replays); records feed training windows.</summary>
    public TimeSpan EventRetention { get; init; } = TimeSpan.FromDays(7);
    public TimeSpan RecordRetention { get; init; } = TimeSpan.FromDays(120);
    public int Database { get; init; } = -1;
}

/// <summary>
/// Event store on Garnet (RESP protocol, so StackExchange.Redis is the client). Uses only hashes, strings and sorted
/// sets, which Garnet fully supports:
/// <list type="bullet">
/// <item><c>{prefix}:evt:{transferId}</c> hash: sequence → event JSON (HSETNX makes appends idempotent).</item>
/// <item><c>{prefix}:rec:{transferId}</c> string: record JSON (SET NX).</item>
/// <item><c>{prefix}:idx:{yyyyMMdd}</c> sorted set: transferId scored by EndedAt (ms) — per-day buckets keep range
/// reads cheap and let retention be a simple EXPIRE.</item>
/// </list>
/// </summary>
public sealed class GarnetEventStore(IConnectionMultiplexer connection, GarnetEventStoreOptions? options = null) : IEventStore
{
    private const int ReadBatch = 256;
    private readonly GarnetEventStoreOptions _options = options ?? new GarnetEventStoreOptions();

    private IDatabase Db => connection.GetDatabase(_options.Database);

    private RedisKey EventsKey(Guid id) => $"{_options.KeyPrefix}:evt:{id:N}";
    private RedisKey RecordKey(Guid id) => $"{_options.KeyPrefix}:rec:{id:N}";
    private RedisKey IndexKey(DateOnly day) => $"{_options.KeyPrefix}:idx:{day:yyyyMMdd}";

    public async Task<bool> AppendAsync(TransferEvent transferEvent, CancellationToken ct = default)
    {
        var key = EventsKey(transferEvent.TransferId);
        var json = JsonSerializer.Serialize(transferEvent, StorageJson.Options);
        var added = await Db.HashSetAsync(key, transferEvent.Sequence, json, When.NotExists).ConfigureAwait(false);
        if (added)
        {
            await Db.KeyExpireAsync(key, _options.EventRetention).ConfigureAwait(false);
        }
        return added;
    }

    public async Task<IReadOnlyList<TransferEvent>> GetEventsAsync(Guid transferId, CancellationToken ct = default)
    {
        var entries = await Db.HashGetAllAsync(EventsKey(transferId)).ConfigureAwait(false);
        return entries
            .Select(e => JsonSerializer.Deserialize<TransferEvent>(e.Value.ToString(), StorageJson.Options)!)
            .OrderBy(e => e.Sequence)
            .ToArray();
    }

    public async Task<bool> SaveRecordAsync(TransferRecord record, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(record, StorageJson.Options);
        var stored = await Db.StringSetAsync(RecordKey(record.TransferId), json, _options.RecordRetention, When.NotExists).ConfigureAwait(false);
        // Always (re)index: if a previous attempt stored the record but crashed before indexing, this repairs it.
        var index = IndexKey(DateOnly.FromDateTime(record.EndedAt.UtcDateTime));
        await Db.SortedSetAddAsync(index, record.TransferId.ToString("N"), record.EndedAt.ToUnixTimeMilliseconds()).ConfigureAwait(false);
        await Db.KeyExpireAsync(index, _options.RecordRetention + TimeSpan.FromDays(1)).ConfigureAwait(false);
        return stored;
    }

    public async Task<TransferRecord?> GetRecordAsync(Guid transferId, CancellationToken ct = default)
    {
        var json = await Db.StringGetAsync(RecordKey(transferId)).ConfigureAwait(false);
        return json.IsNullOrEmpty ? null : JsonSerializer.Deserialize<TransferRecord>(json.ToString(), StorageJson.Options);
    }

    public async IAsyncEnumerable<TransferRecord> ReadRecordsAsync(DateTimeOffset from, DateTimeOffset to, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var db = Db;
        foreach (var (key, min, max) in Buckets(from, to))
        {
            var ids = await db.SortedSetRangeByScoreAsync(key, min, max, Exclude.Stop).ConfigureAwait(false);
            foreach (var chunk in ids.Chunk(ReadBatch))
            {
                ct.ThrowIfCancellationRequested();
                var values = await db.StringGetAsync(chunk.Select(id => RecordKey(Guid.ParseExact(id.ToString(), "N"))).ToArray()).ConfigureAwait(false);
                foreach (var v in values)
                {
                    // Missing values: record expired before its index bucket did.
                    if (!v.IsNullOrEmpty)
                    {
                        yield return JsonSerializer.Deserialize<TransferRecord>(v.ToString(), StorageJson.Options)!;
                    }
                }
            }
        }
    }

    public async Task<long> CountRecordsAsync(DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        long total = 0;
        foreach (var (key, min, max) in Buckets(from, to))
        {
            total += await Db.SortedSetLengthAsync(key, min, max, Exclude.Stop).ConfigureAwait(false);
        }
        return total;
    }

    private IEnumerable<(RedisKey Key, double Min, double Max)> Buckets(DateTimeOffset from, DateTimeOffset to)
    {
        if (to <= from) yield break;
        var min = from.ToUnixTimeMilliseconds();
        var max = to.ToUnixTimeMilliseconds();
        for (var day = DateOnly.FromDateTime(from.UtcDateTime); day <= DateOnly.FromDateTime(to.UtcDateTime); day = day.AddDays(1))
        {
            yield return (IndexKey(day), min, max);
        }
    }

    public override string ToString() => string.Create(CultureInfo.InvariantCulture, $"GarnetEventStore({connection.Configuration})");
}
