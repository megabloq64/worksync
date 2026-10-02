using System.Globalization;
using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using WorkSync.ML;

namespace WorkSync.Storage;

/// <summary>
/// Registry in an Azure Blob container shared by all silos:
/// <list type="bullet">
/// <item><c>v{n}/claim</c> — created with If-None-Match: * to atomically reserve version n.</item>
/// <item><c>v{n}/*.zip</c>, <c>v{n}/manifest.json</c> — the bundle (manifest written last = "complete" marker).</item>
/// <item><c>current</c> — the champion version number.</item>
/// </list>
/// Downloaded bundles are cached on local disk because versions are immutable.
/// </summary>
public sealed class BlobModelRegistry(BlobContainerClient container, string? localCacheDirectory = null) : IModelRegistry, IDisposable
{
    private const string CurrentBlob = "current";
    private readonly string _cache = localCacheDirectory ?? Path.Combine(Path.GetTempPath(), "worksync-model-cache", container.Name);
    private readonly SemaphoreSlim _downloadLock = new(1, 1);

    public async Task<int> PublishAsync(ModelBundle bundle, CancellationToken ct = default)
    {
        await container.CreateIfNotExistsAsync(cancellationToken: ct).ConfigureAwait(false);
        var staging = Path.Combine(Path.GetTempPath(), $"worksync-publish-{Guid.NewGuid():N}");
        bundle.Save(staging);
        try
        {
            var version = await ClaimVersionAsync(ct).ConfigureAwait(false);
            foreach (var file in ModelBundle.ModelFiles.Append(ModelBundle.ManifestFile))
            {
                await container.GetBlobClient($"v{version}/{file}")
                    .UploadAsync(Path.Combine(staging, file), overwrite: true, ct).ConfigureAwait(false);
            }
            return version;
        }
        finally
        {
            Directory.Delete(staging, recursive: true);
        }
    }

    private async Task<int> ClaimVersionAsync(CancellationToken ct)
    {
        var next = (await ClaimedVersionsAsync(ct).ConfigureAwait(false)).DefaultIfEmpty(0).Max() + 1;
        for (var attempt = 0; attempt < 100; attempt++, next++)
        {
            try
            {
                await container.GetBlobClient($"v{next}/claim").UploadAsync(
                    BinaryData.FromString(DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture)),
                    new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All } }, ct).ConfigureAwait(false);
                return next;
            }
            catch (RequestFailedException ex) when (ex.Status is 409 or 412)
            {
                // Another publisher took this number.
            }
        }
        throw new InvalidOperationException("Could not claim a model version after 100 attempts.");
    }

    public async Task<int?> GetCurrentVersionAsync(CancellationToken ct = default)
    {
        try
        {
            var content = await container.GetBlobClient(CurrentBlob).DownloadContentAsync(ct).ConfigureAwait(false);
            return int.TryParse(content.Value.Content.ToString().Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : null;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task SetCurrentVersionAsync(int version, CancellationToken ct = default)
    {
        if (await GetManifestAsync(version, ct).ConfigureAwait(false) is null) throw new ModelVersionNotFoundException(version);
        await container.GetBlobClient(CurrentBlob)
            .UploadAsync(BinaryData.FromString(version.ToString(CultureInfo.InvariantCulture)), overwrite: true, ct).ConfigureAwait(false);
    }

    public async Task<ModelBundle> LoadAsync(int version, CancellationToken ct = default)
    {
        var dir = Path.Combine(_cache, $"v{version}");
        await _downloadLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (!File.Exists(Path.Combine(dir, ModelBundle.ManifestFile)))
            {
                if (await GetManifestAsync(version, ct).ConfigureAwait(false) is null) throw new ModelVersionNotFoundException(version);
                var staging = dir + $".{Guid.NewGuid():N}";
                Directory.CreateDirectory(staging);
                foreach (var file in ModelBundle.ModelFiles.Append(ModelBundle.ManifestFile))
                {
                    await container.GetBlobClient($"v{version}/{file}").DownloadToAsync(Path.Combine(staging, file), ct).ConfigureAwait(false);
                }
                if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
                Directory.Move(staging, dir);
            }
        }
        finally
        {
            _downloadLock.Release();
        }
        return ModelBundle.Load(dir);
    }

    public async Task<BundleManifest?> GetManifestAsync(int version, CancellationToken ct = default)
    {
        try
        {
            var content = await container.GetBlobClient($"v{version}/{ModelBundle.ManifestFile}").DownloadContentAsync(ct).ConfigureAwait(false);
            return content.Value.Content.ToObjectFromJson<BundleManifest>(ModelBundle.JsonOptions);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<ModelVersionInfo>> ListAsync(CancellationToken ct = default)
    {
        var current = await GetCurrentVersionAsync(ct).ConfigureAwait(false);
        var result = new List<ModelVersionInfo>();
        await foreach (var blob in container.GetBlobsAsync(new GetBlobsOptions(), ct).ConfigureAwait(false))
        {
            if (!blob.Name.EndsWith("/" + ModelBundle.ManifestFile, StringComparison.Ordinal) || ParseVersion(blob.Name) is not { } v) continue;
            var manifest = await GetManifestAsync(v, ct).ConfigureAwait(false);
            if (manifest is not null)
            {
                result.Add(new ModelVersionInfo(v, blob.Properties.CreatedOn ?? manifest.TrainedAt, manifest, v == current));
            }
        }
        return result.OrderByDescending(i => i.Version).ToArray();
    }

    private async Task<List<int>> ClaimedVersionsAsync(CancellationToken ct)
    {
        var versions = new List<int>();
        await foreach (var item in container.GetBlobsByHierarchyAsync(new GetBlobsByHierarchyOptions { Delimiter = "/" }, ct).ConfigureAwait(false))
        {
            if (item.IsPrefix && ParseVersion(item.Prefix) is { } v) versions.Add(v);
        }
        return versions;
    }

    private static int? ParseVersion(string name)
    {
        if (!name.StartsWith('v')) return null;
        var slash = name.IndexOf('/', StringComparison.Ordinal);
        var digits = slash < 0 ? name[1..] : name[1..slash];
        return int.TryParse(digits, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) && v > 0 ? v : null;
    }

    public void Dispose() => _downloadLock.Dispose();
}
