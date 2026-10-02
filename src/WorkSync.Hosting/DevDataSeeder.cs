using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WorkSync.DataGen;
using WorkSync.Grains;
using WorkSync.Storage;

namespace WorkSync.Hosting;

/// <summary>
/// Development convenience: when the event store is empty, writes synthetic history (events + records) so the cluster
/// has something to learn from, then asks the coordinator to train the first model.
/// </summary>
public sealed partial class DevDataSeeder(
    IEventStore store,
    IGrainFactory grains,
    IHostApplicationLifetime lifetime,
    TimeProvider clock,
    IOptions<WorkSyncOptions> options,
    ILogger<DevDataSeeder> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var seed = options.Value.Seed;
        if (seed.Records <= 0) return;

        // Wait for the silo to be fully up before calling grains.
        var started = new TaskCompletionSource();
        using (lifetime.ApplicationStarted.Register(() => started.TrySetResult()))
        using (stoppingToken.Register(() => started.TrySetCanceled(stoppingToken)))
        {
            await started.Task.ConfigureAwait(false);
        }

        var now = clock.GetUtcNow();
        if (await store.CountRecordsAsync(now - TimeSpan.FromDays(365), now + TimeSpan.FromDays(30), stoppingToken).ConfigureAwait(false) > 0)
        {
            LogSkipped();
        }
        else
        {
            var generator = new WorkloadGenerator(new WorkloadOptions { Seed = seed.RandomSeed });
            var count = 0;
            foreach (var record in generator.GenerateRecords(seed.Records, now - TimeSpan.FromDays(seed.Days), now))
            {
                stoppingToken.ThrowIfCancellationRequested();
                foreach (var e in generator.ToEvents(record))
                {
                    await store.AppendAsync(e, stoppingToken).ConfigureAwait(false);
                }
                await store.SaveRecordAsync(record, stoppingToken).ConfigureAwait(false);
                count++;
            }
            LogSeeded(count, seed.Days);
        }

        var coordinator = grains.GetGrain<ITrainingCoordinatorGrain>(WellKnownKeys.Coordinator);
        if (await coordinator.GetChampionVersionAsync().ConfigureAwait(false) is null)
        {
            await coordinator.RequestTrainingAsync("initial model (seeded data)").ConfigureAwait(false);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Seeded {Count} synthetic transfers over the last {Days} days")]
    private partial void LogSeeded(int count, int days);

    [LoggerMessage(Level = LogLevel.Information, Message = "Event store already has data; skipping seeding")]
    private partial void LogSkipped();
}
