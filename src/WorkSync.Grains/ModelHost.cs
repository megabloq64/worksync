using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WorkSync.ML;
using WorkSync.Storage;

namespace WorkSync.Grains;

/// <summary>
/// Per-silo holder of the active model. Swapped atomically so in-flight predictions keep using the old predictor.
/// Updated by push (<see cref="ModelSyncGrain"/>) and by a periodic poll of the registry (<see cref="ModelSyncService"/>).
/// </summary>
public sealed partial class ModelHost(IModelRegistry registry, ILogger<ModelHost> logger) : IDisposable
{
    private readonly SemaphoreSlim _loadLock = new(1, 1);
    private readonly HashSet<int> _incompatible = [];
    private volatile TransferPredictor? _current;

    public TransferPredictor? Current => _current;

    public int? Version => _current?.Version;

    public TransferPredictor Require() => _current ?? throw new ModelNotReadyException();

    public event Action<int>? ModelChanged;

    public async Task<int?> EnsureVersionAsync(int version, CancellationToken ct = default)
    {
        if (_current?.Version == version) return version;
        await _loadLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_current?.Version == version) return version;
            if (_incompatible.Contains(version)) return _current?.Version;
            ModelBundle bundle;
            try
            {
                bundle = await registry.LoadAsync(version, ct).ConfigureAwait(false);
            }
            catch (IncompatibleModelException ex)
            {
                // Trained on an older feature schema: keep serving whatever we have until a compatible model is promoted.
                _incompatible.Add(version);
                LogIncompatible(ex, version);
                return _current?.Version;
            }
            _current = new TransferPredictor(bundle, version);
            LogLoaded(version, bundle.Manifest.Metrics.Duration.RSquared);
        }
        finally
        {
            _loadLock.Release();
        }
        ModelChanged?.Invoke(version);
        return version;
    }

    public async Task<int?> SyncWithRegistryAsync(CancellationToken ct = default)
    {
        var current = await registry.GetCurrentVersionAsync(ct).ConfigureAwait(false);
        return current is { } v ? await EnsureVersionAsync(v, ct).ConfigureAwait(false) : null;
    }

    public void Dispose() => _loadLock.Dispose();

    [LoggerMessage(Level = LogLevel.Information, Message = "Loaded model v{Version} (duration R² {RSquared:F3})")]
    private partial void LogLoaded(int version, double rSquared);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Model v{Version} is incompatible with this build and was skipped")]
    private partial void LogIncompatible(Exception ex, int version);
}

/// <summary>Loads the current model at startup and keeps polling as a safety net for missed pushes.</summary>
public sealed partial class ModelSyncService(ModelHost host, IOptions<WorkSyncGrainOptions> options, ILogger<ModelSyncService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.Value.ModelPollInterval);
        do
        {
            try
            {
                await host.SyncWithRegistryAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                LogSyncFailed(ex);
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Model registry sync failed")]
    private partial void LogSyncFailed(Exception ex);
}
