using Microsoft.Extensions.Logging;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;
using StackExchange.Redis;
using RedisPosition = StackExchange.Redis.StreamPosition;

namespace WorkSync.Streams.Redis;

/// <summary>
/// Pulls one Redis stream through a consumer group. The consumer name is the queue id, which is stable across silos:
/// when the queue balancer moves a queue to another silo, the new owner first re-reads that consumer's pending entries
/// list (position "0") before reading new entries ("&gt;"). Entries are XACKed once Orleans has delivered them to all
/// subscribers, giving at-least-once delivery.
/// </summary>
internal sealed partial class RedisStreamQueueReceiver(
    RedisKey key,
    string group,
    string consumer,
    IConnectionMultiplexer connection,
    Serializer<RedisStreamBatchContainer> serializer,
    ILogger<RedisStreamQueueReceiver> logger) : IQueueAdapterReceiver
{
    private bool _drainingPending = true;
    private RedisValue _pendingCursor = "0";

    public async Task Initialize(TimeSpan timeout)
    {
        try
        {
            await connection.GetDatabase().StreamCreateConsumerGroupAsync(key, group, RedisPosition.Beginning, createStream: true).ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP", StringComparison.Ordinal))
        {
            // Group already exists.
        }
        _drainingPending = true;
        _pendingCursor = "0";
    }

    public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
    {
        var count = maxCount is <= 0 or > 1000 ? 100 : maxCount;
        var db = connection.GetDatabase();
        StreamEntry[] entries;
        try
        {
            if (_drainingPending)
            {
                entries = await db.StreamReadGroupAsync(key, group, consumer, _pendingCursor, count).ConfigureAwait(false);
                if (entries.Length == 0)
                {
                    _drainingPending = false;
                    entries = await db.StreamReadGroupAsync(key, group, consumer, RedisPosition.NewMessages, count).ConfigureAwait(false);
                }
                else
                {
                    _pendingCursor = entries[^1].Id;
                }
            }
            else
            {
                entries = await db.StreamReadGroupAsync(key, group, consumer, RedisPosition.NewMessages, count).ConfigureAwait(false);
            }
        }
        catch (RedisServerException ex) when (ex.Message.Contains("NOGROUP", StringComparison.Ordinal))
        {
            // Stream or group was deleted (e.g. FLUSHALL); recreate and try next poll.
            await Initialize(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            return [];
        }

        var result = new List<IBatchContainer>(entries.Length);
        var poison = new List<RedisValue>();
        foreach (var entry in entries)
        {
            var payload = entry[RedisStreamQueueAdapter.PayloadField];
            if (payload.IsNullOrEmpty)
            {
                poison.Add(entry.Id);
                continue;
            }
            try
            {
                var container = serializer.Deserialize((byte[])payload!) ?? throw new InvalidDataException("Empty batch container.");
                container.EntryId = entry.Id.ToString();
                container.Token = new EventSequenceTokenV2(RedisStreamBatchContainer.SequenceFromEntryId(container.EntryId));
                result.Add(container);
            }
            catch (Exception ex)
            {
                LogPoison(ex, entry.Id.ToString(), key.ToString());
                poison.Add(entry.Id);
            }
        }
        if (poison.Count > 0)
        {
            await db.StreamAcknowledgeAsync(key, group, poison.ToArray()).ConfigureAwait(false);
        }
        return result;
    }

    public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
    {
        var ids = messages.OfType<RedisStreamBatchContainer>().Where(m => m.EntryId is not null).Select(m => (RedisValue)m.EntryId).ToArray();
        if (ids.Length > 0)
        {
            await connection.GetDatabase().StreamAcknowledgeAsync(key, group, ids).ConfigureAwait(false);
        }
    }

    public Task Shutdown(TimeSpan timeout) => Task.CompletedTask;

    [LoggerMessage(Level = LogLevel.Error, Message = "Dropping undeserializable entry {EntryId} from {Stream}")]
    private partial void LogPoison(Exception ex, string entryId, string stream);
}
