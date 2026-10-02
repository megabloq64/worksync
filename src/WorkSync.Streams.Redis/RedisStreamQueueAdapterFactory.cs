using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using StackExchange.Redis;

namespace WorkSync.Streams.Redis;

/// <summary>
/// Orleans persistent stream provider backed by Redis Streams (works against Redis Cluster: each queue is a single
/// key, so no cross-slot commands are needed).
/// </summary>
public sealed class RedisStreamQueueAdapterFactory : IQueueAdapterFactory, IDisposable
{
    private readonly string _name;
    private readonly RedisStreamOptions _options;
    private readonly string _consumerGroup;
    private readonly Serializer<RedisStreamBatchContainer> _serializer;
    private readonly ILoggerFactory _loggerFactory;
    private readonly HashRingBasedStreamQueueMapper _mapper;
    private readonly SimpleQueueAdapterCache _cache;
    private readonly SemaphoreSlim _connectLock = new(1, 1);
    private ConnectionMultiplexer? _connection;

    public RedisStreamQueueAdapterFactory(string name, RedisStreamOptions options, ClusterOptions cluster,
        Serializer<RedisStreamBatchContainer> serializer, ILoggerFactory loggerFactory)
    {
        _name = name;
        _options = options;
        _consumerGroup = options.ConsumerGroup ?? $"{cluster.ServiceId}-{name}";
        _serializer = serializer;
        _loggerFactory = loggerFactory;
        _mapper = new HashRingBasedStreamQueueMapper(new HashRingStreamQueueMapperOptions { TotalQueueCount = options.TotalQueueCount }, name);
        _cache = new SimpleQueueAdapterCache(new SimpleQueueCacheOptions { CacheSize = options.CacheSize }, name, loggerFactory);
    }

    public static RedisStreamQueueAdapterFactory Create(IServiceProvider services, string name) =>
        new(name,
            services.GetRequiredService<IOptionsMonitor<RedisStreamOptions>>().Get(name),
            services.GetRequiredService<IOptions<ClusterOptions>>().Value,
            services.GetRequiredService<Serializer<RedisStreamBatchContainer>>(),
            services.GetRequiredService<ILoggerFactory>());

    public async Task<IQueueAdapter> CreateAdapter()
    {
        await _connectLock.WaitAsync().ConfigureAwait(false);
        try
        {
            _connection ??= await ConnectionMultiplexer.ConnectAsync(_options.ConnectionString).ConfigureAwait(false);
        }
        finally
        {
            _connectLock.Release();
        }
        return new RedisStreamQueueAdapter(_name, _options, _consumerGroup, _connection, _mapper, _serializer, _loggerFactory);
    }

    public IQueueAdapterCache GetQueueAdapterCache() => _cache;

    public IStreamQueueMapper GetStreamQueueMapper() => _mapper;

    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId) =>
        Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler(false));

    public void Dispose()
    {
        _connection?.Dispose();
        _connectLock.Dispose();
    }
}

internal sealed class RedisStreamQueueAdapter(
    string name,
    RedisStreamOptions options,
    string consumerGroup,
    IConnectionMultiplexer connection,
    HashRingBasedStreamQueueMapper mapper,
    Serializer<RedisStreamBatchContainer> serializer,
    ILoggerFactory loggerFactory) : IQueueAdapter
{
    internal const string PayloadField = "b";

    public string Name => name;
    public bool IsRewindable => false;
    public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

    internal static RedisKey StreamKey(RedisStreamOptions options, string providerName, QueueId queue) =>
        $"{options.KeyPrefix}:{providerName}:{queue.GetNumericId()}";

    public async Task QueueMessageBatchAsync<T>(StreamId streamId, IEnumerable<T> events, StreamSequenceToken? token, Dictionary<string, object>? requestContext)
    {
        if (token is not null)
        {
            throw new ArgumentException("Redis streams do not support producer-supplied sequence tokens.", nameof(token));
        }
        var container = new RedisStreamBatchContainer(streamId, events.Cast<object>().ToList(), requestContext is { Count: > 0 } ? requestContext : null);
        var key = StreamKey(options, name, mapper.GetQueueForStream(streamId));
        await connection.GetDatabase().StreamAddAsync(key, PayloadField, serializer.SerializeToArray(container),
            maxLength: options.MaxStreamLength, useApproximateMaxLength: true).ConfigureAwait(false);
    }

    public IQueueAdapterReceiver CreateReceiver(QueueId queueId) =>
        new RedisStreamQueueReceiver(StreamKey(options, name, queueId), consumerGroup, queueId.ToString(), connection, serializer,
            loggerFactory.CreateLogger<RedisStreamQueueReceiver>());
}
