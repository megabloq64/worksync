using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Providers.Streams.Common;

namespace WorkSync.Streams.Redis;

public static class RedisStreamsHostingExtensions
{
    public static ISiloBuilder AddRedisStreams(this ISiloBuilder builder, string name, Action<RedisStreamOptions> configure,
        Action<ISiloPersistentStreamConfigurator>? configureStream = null)
    {
        builder.Services.AddOptions<RedisStreamOptions>(name).Configure(configure);
        return builder.AddPersistentStreams(name, RedisStreamQueueAdapterFactory.Create, s =>
        {
            s.ConfigureStreamPubSub(Orleans.Streams.StreamPubSubType.ExplicitGrainBasedAndImplicit);
            configureStream?.Invoke(s);
        });
    }

    public static IClientBuilder AddRedisStreams(this IClientBuilder builder, string name, Action<RedisStreamOptions> configure)
    {
        builder.Services.AddOptions<RedisStreamOptions>(name).Configure(configure);
        return builder.AddPersistentStreams(name, RedisStreamQueueAdapterFactory.Create, s =>
            s.ConfigureStreamPubSub(Orleans.Streams.StreamPubSubType.ExplicitGrainBasedAndImplicit));
    }
}
