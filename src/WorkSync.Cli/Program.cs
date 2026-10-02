using System.CommandLine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WorkSync.Cli;
using WorkSync.DataGen;
using WorkSync.Domain;
using WorkSync.Grains;
using WorkSync.Hosting;
using WorkSync.ML;
using WorkSync.Storage;

var registryOption = new Option<string>("--registry", "-r")
{
    Description = "Model registry folder",
    DefaultValueFactory = _ => WorkSyncOptions.DefaultModelPath,
    Recursive = true,
};
var request = new TransferRequestOptions();
var root = new RootCommand("WorkSync — predict cloud-to-cloud transfer duration, failure risk, throughput and the best time to start.");
root.Options.Add(registryOption);

// ---- datagen ---------------------------------------------------------------------------------------------------
var countOption = new Option<int>("--count", "-n") { Description = "Number of transfers", DefaultValueFactory = _ => 20_000 };
var daysOption = new Option<int>("--days") { Description = "Spread start times over the last N days", DefaultValueFactory = _ => 30 };
var seedOption = new Option<int>("--seed") { Description = "Random seed", DefaultValueFactory = _ => 42 };
var driftOption = new Option<string?>("--drift") { Description = "Bandwidth drift per provider, e.g. \"Dropbox=0.5,S3=1.2\"" };
var outOption = new Option<FileInfo>("--out", "-o") { Description = "CSV output file", Required = true };
var datagen = new Command("datagen", "Generate synthetic transfer history as CSV.") { countOption, daysOption, seedOption, driftOption, outOption };
datagen.SetAction(async (r, ct) =>
{
    var generator = new WorkloadGenerator(new WorkloadOptions { Seed = r.GetValue(seedOption), Drift = DriftProfile.Parse(r.GetValue(driftOption)) });
    var now = DateTimeOffset.UtcNow;
    var file = r.GetValue(outOption)!;
    await TransferRecordCsv.WriteAsync(file.FullName, generator.GenerateRecords(r.GetValue(countOption), now.AddDays(-r.GetValue(daysOption)), now), ct);
    Console.WriteLine($"Wrote {r.GetValue(countOption)} transfers to {file.FullName}");
});
root.Subcommands.Add(datagen);

// ---- train -----------------------------------------------------------------------------------------------------
var dataOption = new Option<FileInfo>("--data") { Description = "CSV of finished transfers (see datagen)", Required = true };
var forceOption = new Option<bool>("--force") { Description = "Make the new model current even if it does not beat the champion" };
var train = new Command("train", "Train a model bundle from CSV, publish it, and promote it if it beats the current one.") { dataOption, forceOption };
train.SetAction(async (r, ct) =>
{
    var registry = new LocalFolderModelRegistry(r.GetValue(registryOption)!);
    var records = await ReadCsvAsync(r.GetValue(dataOption)!, ct);
    Console.WriteLine($"Training on {records.Count} transfers…");
    var trained = new ModelTrainer().Train(records, DateTimeOffset.UtcNow, "cli");
    var challenger = trained.Bundle.Manifest.Metrics;
    Console.WriteLine("Challenger (holdout):");
    Fmt.Metrics(challenger);

    ModelMetrics? champion = null;
    if (await registry.GetCurrentVersionAsync(ct) is { } current)
    {
        try
        {
            champion = ModelEvaluator.Evaluate(await registry.LoadAsync(current, ct), trained.Holdout);
            Console.WriteLine($"Champion v{current} on the same holdout:");
            Fmt.Metrics(champion);
        }
        catch (IncompatibleModelException ex)
        {
            Console.WriteLine($"Ignoring champion v{current}: {ex.Message}");
        }
    }
    var decision = ChampionChallenger.Decide(champion, challenger);
    var version = await registry.PublishAsync(trained.Bundle, ct);
    var promote = decision.Promote || r.GetValue(forceOption);
    if (promote) await registry.SetCurrentVersionAsync(version, ct);
    Console.WriteLine($"Published v{version} to {r.GetValue(registryOption)}; {(promote ? "now current" : "not promoted")}: {decision.Reason}");
});
root.Subcommands.Add(train);

// ---- evaluate --------------------------------------------------------------------------------------------------
var versionOption = new Option<int?>("--version", "-v") { Description = "Model version (default: current)" };
var evaluate = new Command("evaluate", "Evaluate a model version against a CSV of finished transfers.") { dataOption, versionOption };
evaluate.SetAction(async (r, ct) =>
{
    var registry = new LocalFolderModelRegistry(r.GetValue(registryOption)!);
    var version = await ResolveVersionAsync(registry, r.GetValue(versionOption), ct);
    var records = await ReadCsvAsync(r.GetValue(dataOption)!, ct);
    Console.WriteLine($"Model v{version} on {records.Count} transfers:");
    Fmt.Metrics(ModelEvaluator.Evaluate(await registry.LoadAsync(version, ct), records));
});
root.Subcommands.Add(evaluate);

// ---- predict / best-time ---------------------------------------------------------------------------------------
var startOption = new Option<DateTimeOffset?>("--start") { Description = "Planned start (default: now)" };
var grantedOption = new Option<int?>("--granted") { Description = "DTUs actually granted if the pool is busy (default: all requested)" };
var utilizationOption = new Option<double>("--pool-utilization") { Description = "Fraction of the pool already reserved at start (0-1)" };
var predict = new Command("predict", "Forecast duration (P10–P90), failure risk, throughput and DTU-hours for a transfer.")
{
    versionOption, startOption, grantedOption, utilizationOption,
};
request.AddTo(predict);
predict.SetAction(async (r, ct) =>
{
    var predictor = await LoadPredictorAsync(r, ct);
    Fmt.Forecast(predictor.Forecast(request.Bind(r), r.GetValue(startOption) ?? DateTimeOffset.UtcNow,
        r.GetValue(grantedOption), Math.Clamp(r.GetValue(utilizationOption), 0, 1)));
});
root.Subcommands.Add(predict);

var availableOption = new Option<int?>("--available") { Description = "DTUs free in the pool right now (default: the whole pool for the tier)" };
var dtuAdvice = new Command("dtu-advice", "Compare DTU allocations: fastest, cheapest in DTU-hours, and recommended.") { versionOption, startOption, availableOption };
request.AddTo(dtuAdvice);
dtuAdvice.SetAction(async (r, ct) =>
{
    var predictor = await LoadPredictorAsync(r, ct);
    var transfer = request.Bind(r);
    var poolSize = Dtu.DefaultPoolSize(transfer.Tier);
    Fmt.DtuAdvice(DtuAdvisor.Advise(predictor, transfer, r.GetValue(startOption) ?? DateTimeOffset.UtcNow, poolSize, r.GetValue(availableOption) ?? poolSize));
});
root.Subcommands.Add(dtuAdvice);

var horizonOption = new Option<int>("--horizon") { Description = "Hours ahead to consider", DefaultValueFactory = _ => 72 };
var topOption = new Option<int>("--top") { Description = "Number of suggestions", DefaultValueFactory = _ => 5 };
var bestTime = new Command("best-time", "Recommend when to start a transfer.") { versionOption, horizonOption, topOption };
request.AddTo(bestTime);
bestTime.SetAction(async (r, ct) =>
{
    var predictor = await LoadPredictorAsync(r, ct);
    var query = new BestTimeQuery(request.Bind(r), null, r.GetValue(horizonOption), r.GetValue(topOption));
    Fmt.Advice(BestStartTimeAdvisor.Advise(predictor, query, DateTimeOffset.UtcNow));
});
root.Subcommands.Add(bestTime);

// ---- models ----------------------------------------------------------------------------------------------------
var models = new Command("models", "Inspect and manage published models (local registry).");
var modelsList = new Command("list", "List model versions.");
modelsList.SetAction(async (r, ct) =>
{
    var list = await new LocalFolderModelRegistry(r.GetValue(registryOption)!).ListAsync(ct);
    if (list.Count == 0) Console.WriteLine("No models published yet.");
    foreach (var m in list)
    {
        var d = m.Manifest.Metrics.Duration;
        Console.WriteLine($"{(m.IsCurrent ? "*" : " ")} v{m.Version,-4} {m.Manifest.TrainedAt:u}  rows {m.Manifest.TrainingRows,7}  R² {d.RSquared:F3}  MAE(log) {d.MaeLog:F3}  AUC {m.Manifest.Metrics.Failure.Auc:F3}  {m.Manifest.Notes}");
    }
});
var rollbackVersion = new Argument<int>("version") { Description = "Version to make current" };
var modelsRollback = new Command("rollback", "Make an earlier version current.") { rollbackVersion };
modelsRollback.SetAction(async (r, ct) =>
{
    var registry = new LocalFolderModelRegistry(r.GetValue(registryOption)!);
    var version = r.GetValue(rollbackVersion);
    _ = await registry.GetManifestAsync(version, ct) ?? throw new ModelVersionNotFoundException(version);
    await registry.SetCurrentVersionAsync(version, ct);
    Console.WriteLine($"v{version} is now current. Running clusters pick it up within the model poll interval; use 'cluster rollback' to push immediately.");
});
models.Subcommands.Add(modelsList);
models.Subcommands.Add(modelsRollback);
root.Subcommands.Add(models);

// ---- cluster (Orleans client) ----------------------------------------------------------------------------------
var cluster = new Command("cluster", "Talk to a running WorkSync cluster (uses appsettings/env WorkSync:* for connection).");

var status = new Command("status", "Training status, drift indicators and recent runs.");
status.SetAction((r, ct) => WithClusterAsync(async grains =>
{
    var s = await grains.GetGrain<ITrainingCoordinatorGrain>(WellKnownKeys.Coordinator).GetStatusAsync();
    Console.WriteLine($"Champion             {(s.ChampionVersion is { } v ? $"v{v}" : "none")}");
    Console.WriteLine($"Training now         {(s.IsTraining ? $"yes ({s.ActiveRun?.Reason})" : "no")}");
    Console.WriteLine($"New records          {s.RecordsSinceLastTraining}");
    Console.WriteLine($"Rolling MAE(log)     {s.RollingMaeLog:F3} (baseline {s.BaselineMaeLog:F3})");
    Console.WriteLine($"Size drift (log)     {s.MeanLogBytesShift:F2}");
    Console.WriteLine($"Last trained         {s.LastTrainedAt:u}");
    foreach (var h in s.History.Take(10))
    {
        Console.WriteLine($"  {h.RequestedAt:u}  {(h.Version is { } hv ? $"v{hv}" : "-"),-5} {(h.Promoted ? "promoted" : "kept"),-9} {h.Reason} → {h.Outcome}");
    }
}, ct));

var runReason = new Option<string>("--reason") { DefaultValueFactory = _ => "manual (cli)" };
var run = new Command("train", "Ask the cluster to train a challenger now.") { runReason };
run.SetAction((r, ct) => WithClusterAsync(async grains =>
{
    var info = await grains.GetGrain<ITrainingCoordinatorGrain>(WellKnownKeys.Coordinator).RequestTrainingAsync(r.GetValue(runReason)!, force: true);
    Console.WriteLine(info is null ? "Trainer busy or unavailable." : $"Run {info.RunId} started ({info.Reason}).");
}, ct));

var clusterRollback = new Command("rollback", "Make an earlier version champion and push it to every silo.") { rollbackVersion };
clusterRollback.SetAction((r, ct) => WithClusterAsync(async grains =>
{
    await grains.GetGrain<ITrainingCoordinatorGrain>(WellKnownKeys.Coordinator).RollbackAsync(r.GetValue(rollbackVersion));
    Console.WriteLine($"Rolled back to v{r.GetValue(rollbackVersion)}.");
}, ct));

var rateOption = new Option<double>("--rate") { Description = "Transfers started per second", DefaultValueFactory = _ => 20 };
var simulate = new Command("simulate", "Start simulated transfers in the cluster; their events drive continuous retraining.") { countOption, rateOption, seedOption, driftOption };
simulate.SetAction((r, ct) => WithClusterAsync(async grains =>
{
    var generator = new WorkloadGenerator(new WorkloadOptions { Seed = r.GetValue(seedOption) });
    var count = r.GetValue(countOption);
    if (r.GetValue(driftOption) is { Length: > 0 })
    {
        Console.WriteLine("Note: --drift applies to offline datagen; set WorkSync:Grains:SimulationDrift on the silos for live drift.");
    }
    using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1 / Math.Max(0.1, r.GetValue(rateOption))));
    var started = 0;
    var anomalies = 0;
    var partial = 0;
    var pending = new List<Guid>();
    while (started < count && await timer.WaitForNextTickAsync(ct))
    {
        var id = Guid.NewGuid();
        var started0 = await grains.GetGrain<ITransferGrain>(id).StartAsync(generator.NextRequest());
        if (started0.Dtus is { IsPartial: true }) partial++;
        pending.Add(id);
        if (++started % 100 == 0) Console.WriteLine($"{started} started");
    }
    foreach (var id in pending)
    {
        var s = await grains.GetGrain<ITransferGrain>(id).GetStatusAsync();
        if (s.Anomaly?.IsSlowAnomaly == true) anomalies++;
    }
    Console.WriteLine($"Started {started} transfers ({partial} got a partial DTU grant); {anomalies} flagged as slow so far (others may still be running).");
    Console.WriteLine($"Simulated accounts: {string.Join(", ", generator.Accounts.Take(5).Select(a => $"{a.AccountId} ({a.Tier})"))}… — inspect with 'cluster dtus <account>'.");
}, ct));

cluster.Subcommands.Add(status);
cluster.Subcommands.Add(run);
cluster.Subcommands.Add(clusterRollback);
var accountArgument = new Argument<string>("account") { Description = "Account id" };
var dtus = new Command("dtus", "Show an account's DTU pool and active reservations.") { accountArgument };
dtus.SetAction((r, ct) => WithClusterAsync(async grains =>
{
    var account = r.GetValue(accountArgument)!;
    if (!TransferRequest.IsValidAccountId(account)) throw new ArgumentException($"Invalid account id '{account}'.");
    Fmt.Pool(await grains.GetGrain<IDtuPoolGrain>(account).GetStatusAsync());
}, ct));

cluster.Subcommands.Add(simulate);
cluster.Subcommands.Add(dtus);
root.Subcommands.Add(cluster);

try
{
    return await root.Parse(args).InvokeAsync(new InvocationConfiguration { EnableDefaultExceptionHandler = false });
}
catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or KeyNotFoundException or FormatException or InsufficientTrainingDataException or IOException)
{
    Console.Error.WriteLine($"error: {ex.Message}");
    return 1;
}
catch (OperationCanceledException)
{
    return 130;
}

// ---- helpers ---------------------------------------------------------------------------------------------------
static async Task<List<TransferRecord>> ReadCsvAsync(FileInfo file, CancellationToken ct)
{
    var list = new List<TransferRecord>();
    await foreach (var r in TransferRecordCsv.ReadAsync(file.FullName, ct)) list.Add(r);
    return list;
}

static async Task<int> ResolveVersionAsync(IModelRegistry registry, int? requested, CancellationToken ct) =>
    requested ?? await registry.GetCurrentVersionAsync(ct)
    ?? throw new InvalidOperationException("No current model. Run 'worksync train --data <csv>' first.");

async Task<TransferPredictor> LoadPredictorAsync(ParseResult r, CancellationToken ct)
{
    var registry = new LocalFolderModelRegistry(r.GetValue(registryOption)!);
    var version = await ResolveVersionAsync(registry, r.GetValue(versionOption), ct);
    return new TransferPredictor(await registry.LoadAsync(version, ct), version);
}

static async Task WithClusterAsync(Func<IGrainFactory, Task> action, CancellationToken ct)
{
    var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { ContentRootPath = AppContext.BaseDirectory });
    builder.Logging.SetMinimumLevel(LogLevel.Warning);
    builder.AddWorkSyncClient();
    using var host = builder.Build();
    await host.StartAsync(ct);
    try
    {
        await action(host.Services.GetRequiredService<IClusterClient>());
    }
    finally
    {
        await host.StopAsync(CancellationToken.None);
    }
}
