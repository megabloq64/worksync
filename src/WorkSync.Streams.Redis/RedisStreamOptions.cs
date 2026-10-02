namespace WorkSync.Streams.Redis;

public sealed class RedisStreamOptions
{
    /// <summary>StackExchange.Redis configuration string, e.g. "redis-0:6379,redis-1:6379,password=...,ssl=true".</summary>
    public string ConnectionString { get; set; } = "localhost:6379";

    public string KeyPrefix { get; set; } = "wsync:q";

    /// <summary>Number of Redis streams (Orleans queues). Each is pulled by exactly one silo at a time.</summary>
    public int TotalQueueCount { get; set; } = 8;

    /// <summary>Approximate MAXLEN for XADD so streams don't grow forever. Must exceed worst-case unacknowledged backlog.</summary>
    public int MaxStreamLength { get; set; } = 200_000;

    /// <summary>Consumer group name. Defaults to the Orleans ServiceId so separate deployments don't steal each other's messages.</summary>
    public string? ConsumerGroup { get; set; }

    /// <summary>Simple queue cache size (messages) per queue.</summary>
    public int CacheSize { get; set; } = 4096;
}
