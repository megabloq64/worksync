using WorkSync.DataGen;
using WorkSync.Domain;
using WorkSync.ML;

namespace WorkSync.ML.Tests;

/// <summary>Trains once on synthetic data and shares the bundle across tests (training takes a few seconds).</summary>
public sealed class TrainedModelFixture
{
    public static readonly DateTimeOffset Now = new(2026, 9, 1, 0, 0, 0, TimeSpan.Zero);

    public TrainedModelFixture()
    {
        var gen = new WorkloadGenerator(new WorkloadOptions { Seed = 1234 });
        Records = gen.GenerateRecords(4000, Now.AddDays(-30), Now).ToArray();
        Result = new ModelTrainer(new TrainingSettings { Iterations = 200 }).Train(Records, Now);
        Predictor = new TransferPredictor(Result.Bundle, version: 1);
    }

    public IReadOnlyList<TransferRecord> Records { get; }
    public TrainingResult Result { get; }
    public TransferPredictor Predictor { get; }
}
