using System.Globalization;
using WorkSync.ML;

namespace WorkSync.Storage;

/// <summary>
/// Registry on a local or shared file system: <c>{root}/v{n}/</c> bundle directories and a <c>{root}/current</c> file.
/// Publishing writes to a temp directory and renames it into place, so readers never see a half-written bundle and
/// concurrent publishers cannot claim the same version.
/// </summary>
public sealed class LocalFolderModelRegistry(string root) : IModelRegistry
{
    private const string CurrentFile = "current";

    public string Root { get; } = Path.GetFullPath(root);

    public Task<int> PublishAsync(ModelBundle bundle, CancellationToken ct = default)
    {
        Directory.CreateDirectory(Root);
        var staging = Path.Combine(Root, $".staging-{Guid.NewGuid():N}");
        bundle.Save(staging);
        try
        {
            for (var attempt = 0; attempt < 100; attempt++)
            {
                ct.ThrowIfCancellationRequested();
                var version = Versions().DefaultIfEmpty(0).Max() + 1;
                try
                {
                    Directory.Move(staging, VersionDir(version));
                    return Task.FromResult(version);
                }
                catch (IOException) when (Directory.Exists(VersionDir(version)))
                {
                    // Lost the race for this version number; try the next one.
                }
            }
            throw new IOException("Could not allocate a model version after 100 attempts.");
        }
        finally
        {
            if (Directory.Exists(staging)) Directory.Delete(staging, recursive: true);
        }
    }

    public async Task<int?> GetCurrentVersionAsync(CancellationToken ct = default)
    {
        var path = Path.Combine(Root, CurrentFile);
        if (!File.Exists(path)) return null;
        var text = await File.ReadAllTextAsync(path, ct).ConfigureAwait(false);
        return int.TryParse(text.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : null;
    }

    public async Task SetCurrentVersionAsync(int version, CancellationToken ct = default)
    {
        if (!Directory.Exists(VersionDir(version))) throw new ModelVersionNotFoundException(version);
        var tmp = Path.Combine(Root, $".current-{Guid.NewGuid():N}");
        await File.WriteAllTextAsync(tmp, version.ToString(CultureInfo.InvariantCulture), ct).ConfigureAwait(false);
        File.Move(tmp, Path.Combine(Root, CurrentFile), overwrite: true);
    }

    public Task<ModelBundle> LoadAsync(int version, CancellationToken ct = default)
    {
        var dir = VersionDir(version);
        if (!Directory.Exists(dir)) throw new ModelVersionNotFoundException(version);
        return Task.FromResult(ModelBundle.Load(dir));
    }

    public Task<BundleManifest?> GetManifestAsync(int version, CancellationToken ct = default)
    {
        var dir = VersionDir(version);
        return Task.FromResult(File.Exists(Path.Combine(dir, ModelBundle.ManifestFile)) ? ModelBundle.ReadManifest(dir) : null);
    }

    public async Task<IReadOnlyList<ModelVersionInfo>> ListAsync(CancellationToken ct = default)
    {
        var current = await GetCurrentVersionAsync(ct).ConfigureAwait(false);
        return Versions()
            .OrderDescending()
            .Select(v => (v, dir: VersionDir(v)))
            .Where(x => File.Exists(Path.Combine(x.dir, ModelBundle.ManifestFile)))
            .Select(x => new ModelVersionInfo(x.v, Directory.GetCreationTimeUtc(x.dir), ModelBundle.ReadManifest(x.dir), x.v == current))
            .ToArray();
    }

    private string VersionDir(int version) => Path.Combine(Root, $"v{version}");

    private IEnumerable<int> Versions() =>
        Directory.Exists(Root)
            ? Directory.EnumerateDirectories(Root, "v*")
                .Select(d => Path.GetFileName(d)[1..])
                .Select(s => int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : -1)
                .Where(v => v > 0)
            : [];
}
