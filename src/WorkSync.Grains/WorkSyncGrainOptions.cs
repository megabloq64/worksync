using WorkSync.ML;

namespace WorkSync.Grains;

public sealed class WorkSyncGrainOptions
{
    public const string SectionName = "WorkSync:Grains";

    /// <summary>Sliding window of finished transfers used for (re)training.</summary>
    public TimeSpan TrainingWindow { get; set; } = TimeSpan.FromDays(30);

    /// <summary>Retrain once this many new finished transfers have arrived since the last run.</summary>
    public int RetrainAfterNewRecords { get; set; } = 1000;

    /// <summary>Retrain at least this often while new data keeps arriving.</summary>
    public TimeSpan RetrainInterval { get; set; } = TimeSpan.FromHours(6);

    /// <summary>Retrain early when rolling MAE (log duration) exceeds the champion's holdout MAE by this factor.</summary>
    public double DriftMaeRatio { get; set; } = 1.35;

    /// <summary>Retrain early when the mean log(bytes) of new traffic moves this far from the training mean.</summary>
    public double DriftMeanLogBytesShift { get; set; } = 1.0;

    /// <summary>Minimum scored transfers before drift triggers are trusted.</summary>
    public int DriftMinimumSamples { get; set; } = 200;

    /// <summary>How often the coordinator evaluates triggers while active (a durable reminder backs this up).</summary>
    public TimeSpan TriggerCheckInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>Durable reminder period; Orleans enforces a minimum of one minute by default.</summary>
    public TimeSpan ReminderPeriod { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>A training run that hasn't reported back within this time is considered lost and may be restarted.</summary>
    public TimeSpan TrainingLease { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>How often ingestors push accumulated statistics to the coordinator.</summary>
    public TimeSpan StatsFlushInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Safety-net poll of the registry's current version, in case a push to this silo was missed.</summary>
    public TimeSpan ModelPollInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Simulated transfers run this many times faster than real time (3600 = one simulated hour per second).</summary>
    public double SimulationTimeCompression { get; set; } = 3600;

    /// <summary>Number of progress events a simulated transfer emits.</summary>
    public int SimulationProgressEvents { get; set; } = 4;

    /// <summary>Optional drift injected into simulated transfers, e.g. "Dropbox=0.5".</summary>
    public string? SimulationDrift { get; set; }

    public TrainingSettings Training { get; set; } = new();

    public PromotionPolicy Promotion { get; set; } = new();
}
