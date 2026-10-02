using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Configuration;

namespace WorkSync.Grains;

public static class WorkSyncGrainsExtensions
{
    /// <summary>
    /// Registers grain dependencies (model host, executor, placement). The caller must register
    /// <see cref="Storage.IEventStore"/> and <see cref="Storage.IModelRegistry"/> and configure the "worksync" storage,
    /// the <see cref="StreamNames.Provider"/> stream provider (plus "PubSubStore") and reminders.
    /// </summary>
    /// <param name="isTrainer">When false, this silo will never host <see cref="TrainerGrain"/>.</param>
    public static ISiloBuilder AddWorkSyncGrains(this ISiloBuilder silo, IConfiguration? configuration = null, bool isTrainer = true)
    {
        var services = silo.Services;
        var opts = services.AddOptions<WorkSyncGrainOptions>();
        if (configuration is not null) opts.Bind(configuration.GetSection(WorkSyncGrainOptions.SectionName));

        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<ModelHost>();
        services.TryAddSingleton<ITransferExecutor, SimulatedTransferExecutor>();
        services.AddHostedService<ModelSyncService>();

        silo.AddPlacementDirector<SiloKeyedPlacement, SiloKeyedPlacementDirector>();

        if (!isTrainer)
        {
            services.Configure<GrainTypeOptions>(o => o.Classes.Remove(typeof(TrainerGrain)));
        }
        return silo;
    }
}
