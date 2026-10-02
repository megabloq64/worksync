using Orleans.Concurrency;
using WorkSync.Domain;
using WorkSync.ML;

namespace WorkSync.Grains;

[StatelessWorker]
public sealed class PredictionGrain(ModelHost models, TimeProvider clock) : Grain, IPredictionGrain
{
    public Task<TransferForecast> ForecastAsync(TransferRequest request, DateTimeOffset? startUtc = null) =>
        Task.FromResult(models.Require().Forecast(request, startUtc ?? clock.GetUtcNow()));

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
