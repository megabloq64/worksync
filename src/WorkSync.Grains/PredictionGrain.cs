using Microsoft.Extensions.Options;
using Orleans.Concurrency;
using WorkSync.Domain;
using WorkSync.ML;

namespace WorkSync.Grains;

[StatelessWorker]
public sealed class PredictionGrain(ModelHost models, IOptions<WorkSyncGrainOptions> options, TimeProvider clock) : Grain, IPredictionGrain
{
    /// <summary>Pool availability only says something about transfers starting about now.</summary>
    private static readonly TimeSpan PoolRelevance = TimeSpan.FromMinutes(5);

    public async Task<TransferForecast> ForecastAsync(TransferRequest request, DateTimeOffset? startUtc = null, bool assumeFullGrant = false)
    {
        request.Validate();
        var predictor = models.Require();
        var now = clock.GetUtcNow();
        var start = startUtc ?? now;
        if (assumeFullGrant || (start - now).Duration() > PoolRelevance) return predictor.Forecast(request, start);

        var pool = await PoolAsync(request);
        return predictor.Forecast(request, start, Math.Min(request.RequestedDtus, pool.Available), pool.Utilization);
    }

    public async Task<DtuAdvice> AdviseDtusAsync(TransferRequest request, DateTimeOffset? startUtc = null, bool assumePoolAvailable = false)
    {
        request.Validate();
        var predictor = models.Require();
        var start = startUtc ?? clock.GetUtcNow();
        if (assumePoolAvailable)
        {
            var size = options.Value.PoolSizeFor(request.Tier);
            return DtuAdvisor.Advise(predictor, request, start, size, size);
        }
        var pool = await PoolAsync(request);
        return DtuAdvisor.Advise(predictor, request, start, pool.PoolSize, pool.Available);
    }

    private Task<DtuPoolStatus> PoolAsync(TransferRequest request) =>
        GrainFactory.GetGrain<IDtuPoolGrain>(request.AccountId).GetStatusAsync(request.Tier);

    public Task<BestTimeAdvice> BestTimeAsync(BestTimeQuery query)
    {
        query.Request.Validate();
        if (query.HorizonHours is < 1 or > 24 * 14) throw new ArgumentOutOfRangeException(nameof(query), "HorizonHours must be 1..336.");
        return Task.FromResult(BestStartTimeAdvisor.Advise(models.Require(), query, clock.GetUtcNow()));
    }

    public Task<AnomalyResult> ScoreAnomalyAsync(TransferRecord record) =>
        Task.FromResult(models.Require().ScoreAnomaly(record));

    public Task<int?> GetLoadedVersionAsync() => Task.FromResult(models.Version);
}

[SiloKeyedPlacement]
public sealed class ModelSyncGrain(ModelHost models) : Grain, IModelSyncGrain
{
    public Task<int?> ActivateVersionAsync(int version) => models.EnsureVersionAsync(version);
}
