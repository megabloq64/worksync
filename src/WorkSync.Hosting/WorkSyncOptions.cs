namespace WorkSync.Hosting;

public enum DeploymentProfile
{
    /// <summary>Single machine: localhost clustering, in-memory Orleans storage/streams, embedded Garnet, local model folder.</summary>
    Development,

    /// <summary>Multi-machine: Azure Table clustering/state/reminders, Redis Streams, external Garnet, Azure Blob model registry.</summary>
    Production,
}

public enum EventStoreKind
{
    InMemory,
    Garnet,
}

public enum ModelRegistryKind
{
    LocalFolder,
    AzureBlob,
}

public sealed class WorkSyncOptions
{
    public const string SectionName = "WorkSync";

    public DeploymentProfile Profile { get; set; } = DeploymentProfile.Development;

    public string ClusterId { get; set; } = "worksync-dev";

    public string ServiceId { get; set; } = "worksync";

    public SiloSettings Silo { get; set; } = new();

    public EventStoreSettings EventStore { get; set; } = new();

    public RegistrySettings Registry { get; set; } = new();

    public AzureSettings Azure { get; set; } = new();

    public StreamSettings Streams { get; set; } = new();

    public SeedSettings Seed { get; set; } = new();

    public sealed class SiloSettings
    {
        public int SiloPort { get; set; } = 11111;
        public int GatewayPort { get; set; } = 30000;

        /// <summary>Development only: endpoint of the first silo when running several silos on one machine.</summary>
        public string? PrimarySiloEndpoint { get; set; }

        /// <summary>Whether this silo may host the trainer grain (CPU-heavy). Give it to a subset of machines.</summary>
        public bool IsTrainer { get; set; } = true;

        /// <summary>Development only: gateway ports clients connect to.</summary>
        public int[] ClientGatewayPorts { get; set; } = [30000];
    }

    public sealed class EventStoreSettings
    {
        public EventStoreKind Kind { get; set; } = EventStoreKind.Garnet;
        public string ConnectionString { get; set; } = "localhost:6380";

        /// <summary>Start an in-process Garnet server on <see cref="EmbeddedPort"/>. Development convenience.</summary>
        public bool Embedded { get; set; } = true;
        public int EmbeddedPort { get; set; } = 6380;

        public string KeyPrefix { get; set; } = "wsync";
        public TimeSpan EventRetention { get; set; } = TimeSpan.FromDays(7);
        public TimeSpan RecordRetention { get; set; } = TimeSpan.FromDays(120);
    }

    public sealed class RegistrySettings
    {
        public ModelRegistryKind Kind { get; set; } = ModelRegistryKind.LocalFolder;

        /// <summary>Folder for <see cref="ModelRegistryKind.LocalFolder"/>; defaults to %LOCALAPPDATA%/WorkSync/models.</summary>
        public string? Path { get; set; }

        public string? BlobConnectionString { get; set; }
        public string BlobContainer { get; set; } = "worksync-models";
    }

    public sealed class AzureSettings
    {
        /// <summary>Azure Storage connection string for Orleans clustering, grain state and reminders (Production).</summary>
        public string? StorageConnectionString { get; set; }
    }

    public sealed class StreamSettings
    {
        /// <summary>Redis (or Redis Cluster) connection string for the Orleans event stream provider (Production).</summary>
        public string? RedisConnectionString { get; set; }
        public int QueueCount { get; set; } = 8;
        public int MaxStreamLength { get; set; } = 200_000;
    }

    public sealed class SeedSettings
    {
        /// <summary>When the event store is empty at startup, generate this many synthetic historical transfers (0 = off).</summary>
        public int Records { get; set; }
        public int Days { get; set; } = 30;
        public int RandomSeed { get; set; } = 42;
    }

    public static string DefaultModelPath =>
        System.IO.Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "WorkSync", "models");
}
