using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using StackExchange.Redis;
using WorkSync.Grains;
using WorkSync.Storage;
using WorkSync.Streams.Redis;

namespace WorkSync.Hosting;

public static class WorkSyncHostingExtensions
{
    public const string GrainStorageName = "worksync";

    public static WorkSyncOptions GetWorkSyncOptions(this IConfiguration configuration) =>
        configuration.GetSection(WorkSyncOptions.SectionName).Get<WorkSyncOptions>() ?? new WorkSyncOptions();

    /// <summary>Hosts an Orleans silo with every WorkSync grain, the event store and the model registry.</summary>
    public static IHostApplicationBuilder AddWorkSyncSilo(this IHostApplicationBuilder builder)
    {
        var options = builder.Configuration.GetWorkSyncOptions();
        builder.Services.Configure<WorkSyncOptions>(builder.Configuration.GetSection(WorkSyncOptions.SectionName));
        builder.Services.AddWorkSyncStorage(options);

        builder.UseOrleans(silo =>
        {
            silo.Configure<ClusterOptions>(o =>
            {
                o.ClusterId = options.ClusterId;
                o.ServiceId = options.ServiceId;
            });

            if (options.Profile == DeploymentProfile.Development)
            {
                var primary = string.IsNullOrWhiteSpace(options.Silo.PrimarySiloEndpoint)
                    ? null
                    : System.Net.IPEndPoint.Parse(options.Silo.PrimarySiloEndpoint);
                silo.UseLocalhostClustering(options.Silo.SiloPort, options.Silo.GatewayPort, primary, options.ServiceId, options.ClusterId);
                silo.AddMemoryGrainStorage(GrainStorageName);
                silo.AddMemoryGrainStorage("PubSubStore");
                silo.UseInMemoryReminderService();
                silo.AddMemoryStreams(StreamNames.Provider);
            }
            else
            {
                var tables = new TableServiceClient(Require(options.Azure.StorageConnectionString, "WorkSync:Azure:StorageConnectionString"));
                silo.Configure<EndpointOptions>(o =>
                {
                    o.SiloPort = options.Silo.SiloPort;
                    o.GatewayPort = options.Silo.GatewayPort;
                });
                silo.UseAzureStorageClustering(o => o.TableServiceClient = tables);
                silo.AddAzureTableGrainStorage(GrainStorageName, o => o.TableServiceClient = tables);
                silo.AddAzureTableGrainStorage("PubSubStore", o => o.TableServiceClient = tables);
                silo.UseAzureTableReminderService(o => o.TableServiceClient = tables);
                var redis = Require(options.Streams.RedisConnectionString, "WorkSync:Streams:RedisConnectionString");
                silo.AddRedisStreams(StreamNames.Provider, o =>
                {
                    o.ConnectionString = redis;
                    o.TotalQueueCount = options.Streams.QueueCount;
                    o.MaxStreamLength = options.Streams.MaxStreamLength;
                });
            }

            silo.AddWorkSyncGrains(builder.Configuration, options.Silo.IsTrainer);
        });

        // Registered after UseOrleans so it starts after the silo.
        if (options.Seed.Records > 0) builder.Services.AddHostedService<DevDataSeeder>();
        return builder;
    }

    /// <summary>Connects to an existing WorkSync cluster as an Orleans client.</summary>
    public static IHostApplicationBuilder AddWorkSyncClient(this IHostApplicationBuilder builder)
    {
        var options = builder.Configuration.GetWorkSyncOptions();
        builder.Services.Configure<WorkSyncOptions>(builder.Configuration.GetSection(WorkSyncOptions.SectionName));
        builder.UseOrleansClient(client =>
        {
            client.Configure<ClusterOptions>(o =>
            {
                o.ClusterId = options.ClusterId;
                o.ServiceId = options.ServiceId;
            });
            if (options.Profile == DeploymentProfile.Development)
            {
                client.UseLocalhostClustering(options.Silo.ClientGatewayPorts, options.ServiceId, options.ClusterId);
            }
            else
            {
                var tables = new TableServiceClient(Require(options.Azure.StorageConnectionString, "WorkSync:Azure:StorageConnectionString"));
                client.UseAzureStorageClustering(o => o.TableServiceClient = tables);
            }
        });
        return builder;
    }

    /// <summary>Registers <see cref="IEventStore"/> and <see cref="IModelRegistry"/> according to configuration.</summary>
    public static IServiceCollection AddWorkSyncStorage(this IServiceCollection services, WorkSyncOptions options)
    {
        var store = options.EventStore;
        if (store.Kind == EventStoreKind.Garnet)
        {
            if (store.Embedded)
            {
                services.TryAddSingleton(sp => new EmbeddedGarnetServer(store.EmbeddedPort, sp.GetRequiredService<ILoggerFactory>()));
            }
            services.TryAddSingleton<IConnectionMultiplexer>(sp =>
            {
                // Ensure the embedded server is listening before connecting.
                sp.GetService<EmbeddedGarnetServer>();
                var config = ConfigurationOptions.Parse(store.Embedded ? $"127.0.0.1:{store.EmbeddedPort}" : store.ConnectionString);
                config.AbortOnConnectFail = false;
                return ConnectionMultiplexer.Connect(config);
            });
            services.TryAddSingleton<IEventStore>(sp => new GarnetEventStore(sp.GetRequiredService<IConnectionMultiplexer>(),
                new GarnetEventStoreOptions
                {
                    KeyPrefix = store.KeyPrefix,
                    EventRetention = store.EventRetention,
                    RecordRetention = store.RecordRetention,
                }));
        }
        else
        {
            services.TryAddSingleton<IEventStore, InMemoryEventStore>();
        }

        var registry = options.Registry;
        if (registry.Kind == ModelRegistryKind.AzureBlob)
        {
            services.TryAddSingleton<IModelRegistry>(_ =>
            {
                var container = new BlobContainerClient(Require(registry.BlobConnectionString, "WorkSync:Registry:BlobConnectionString"), registry.BlobContainer);
                container.CreateIfNotExists();
                return new BlobModelRegistry(container);
            });
        }
        else
        {
            services.TryAddSingleton<IModelRegistry>(_ => new LocalFolderModelRegistry(registry.Path ?? WorkSyncOptions.DefaultModelPath));
        }
        services.TryAddSingleton(TimeProvider.System);
        return services;
    }

    private static string Require(string? value, string key) =>
        string.IsNullOrWhiteSpace(value) ? throw new OptionsValidationException(key, typeof(WorkSyncOptions), [$"{key} is required in the Production profile."]) : value;
}
