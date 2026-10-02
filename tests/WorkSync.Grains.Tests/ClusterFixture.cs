using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;
using WorkSync.Grains;
using WorkSync.ML;
using WorkSync.Storage;

namespace WorkSync.Grains.Tests;

/// <summary>
/// Two in-process silos sharing one event store and one model registry (as they would share Garnet and Blob storage
/// in production). Only the first silo has the trainer role.
/// </summary>
public sealed class ClusterFixture : IAsyncLifetime
{
    private int _silosConfigured;

    public string RegistryPath { get; } = Path.Combine(Path.GetTempPath(), "worksync-tests", Guid.NewGuid().ToString("N"));
    public InMemoryEventStore Store { get; } = new();
    public LocalFolderModelRegistry Registry { get; }
    public InProcessTestCluster Cluster { get; }
    public IClusterClient Client => Cluster.Client;

    public ClusterFixture()
    {
        Registry = new LocalFolderModelRegistry(RegistryPath);
        var builder = new InProcessTestClusterBuilder(2);
        builder.ConfigureSilo((_, silo) =>
        {
            var isTrainer = Interlocked.Increment(ref _silosConfigured) == 1;
            silo.Services.AddSingleton<IEventStore>(Store);
            silo.Services.AddSingleton<IModelRegistry>(Registry);
            silo.Services.Configure<WorkSyncGrainOptions>(o =>
            {
                o.Training = new TrainingSettings { Iterations = 60, MinimumRecords = 300 };
                o.TriggerCheckInterval = TimeSpan.FromMilliseconds(500);
                o.StatsFlushInterval = TimeSpan.FromMilliseconds(250);
                o.ModelPollInterval = TimeSpan.FromSeconds(2);
                o.RetrainAfterNewRecords = 1_000_000;
                o.SimulationTimeCompression = 1_000_000;
                o.SimulationProgressEvents = 2;
            });
            silo.AddMemoryGrainStorage("worksync");
            silo.AddMemoryGrainStorage("PubSubStore");
            silo.UseInMemoryReminderService();
            silo.AddMemoryStreams(StreamNames.Provider);
            silo.AddWorkSyncGrains(isTrainer: isTrainer);
        });
        Cluster = builder.Build();
    }

    public async ValueTask InitializeAsync() => await Cluster.DeployAsync();

    public async ValueTask DisposeAsync()
    {
        await Cluster.DisposeAsync();
        try { Directory.Delete(RegistryPath, recursive: true); } catch (IOException) { }
    }

    public static async Task<T> WaitForAsync<T>(Func<Task<T>> probe, Func<T, bool> done, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            var value = await probe();
            if (done(value)) return value;
            if (DateTime.UtcNow > deadline) throw new TimeoutException($"Timed out waiting for {what}. Last value: {value}");
            await Task.Delay(200, TestContext.Current.CancellationToken);
        }
    }
}
