using WorkSync.ML;

namespace WorkSync.Storage;

/// <summary>
/// Versioned store of whole model bundles plus a "current" pointer. Versions are immutable once published; promotion
/// and rollback only move the pointer, so every silo can load any version at any time.
/// </summary>
public interface IModelRegistry
{
    /// <summary>Stores the bundle under a new, monotonically increasing version. Does not make it current.</summary>
    Task<int> PublishAsync(ModelBundle bundle, CancellationToken ct = default);

    Task<int?> GetCurrentVersionAsync(CancellationToken ct = default);

    Task SetCurrentVersionAsync(int version, CancellationToken ct = default);

    Task<ModelBundle> LoadAsync(int version, CancellationToken ct = default);

    Task<BundleManifest?> GetManifestAsync(int version, CancellationToken ct = default);

    /// <summary>All published versions, newest first.</summary>
    Task<IReadOnlyList<ModelVersionInfo>> ListAsync(CancellationToken ct = default);
}

public sealed class ModelVersionNotFoundException(int version) : KeyNotFoundException($"Model version {version} does not exist.")
{
    public int Version { get; } = version;
}
