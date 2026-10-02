using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace WorkSync.Streams.Redis;

/// <summary>One XADD entry: a batch of events for a single stream, plus the producer's request context.</summary>
[GenerateSerializer, Alias("worksync.RedisStreamBatchContainer")]
public sealed class RedisStreamBatchContainer : IBatchContainer
{
    public RedisStreamBatchContainer(StreamId streamId, List<object> events, Dictionary<string, object>? requestContext)
    {
        StreamId = streamId;
        Events = events;
        RequestContext = requestContext;
    }

    [Id(0)] public StreamId StreamId { get; }
    [Id(1)] public List<object> Events { get; }
    [Id(2)] public Dictionary<string, object>? RequestContext { get; }

    /// <summary>Assigned on read from the Redis entry id; not part of the payload written to Redis.</summary>
    [Id(3)] public EventSequenceTokenV2 Token { get; internal set; } = new(0);

    /// <summary>Redis entry id (e.g. "1719500000000-3"), used for XACK.</summary>
    [Id(4)] public string? EntryId { get; internal set; }

    public StreamSequenceToken SequenceToken => Token;

    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>() =>
        Events.OfType<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, Token.CreateSequenceTokenForEvent(i)));

    public bool ImportRequestContext()
    {
        if (RequestContext is null) return false;
        RequestContextExtensions.Import(RequestContext);
        return true;
    }

    /// <summary>
    /// Maps a Redis entry id "ms-seq" to a monotonically increasing 64-bit sequence number. Redis guarantees ids are
    /// strictly increasing within a stream, and 41 bits of milliseconds + 20 bits of sequence fit in a long.
    /// </summary>
    public static long SequenceFromEntryId(string entryId)
    {
        var dash = entryId.IndexOf('-', StringComparison.Ordinal);
        var ms = long.Parse(entryId.AsSpan(0, dash), System.Globalization.CultureInfo.InvariantCulture);
        var seq = long.Parse(entryId.AsSpan(dash + 1), System.Globalization.CultureInfo.InvariantCulture);
        return (ms << 20) | Math.Min(seq, (1 << 20) - 1);
    }

    public override string ToString() => $"RedisStreamBatchContainer({StreamId}, {Events.Count} events, {EntryId})";
}
