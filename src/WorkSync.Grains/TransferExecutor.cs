using Microsoft.Extensions.Options;
using WorkSync.DataGen;
using WorkSync.Domain;

namespace WorkSync.Grains;

/// <summary>Executes (or plans) a transfer. Swap in a real engine adapter in place of the simulator.</summary>
public interface ITransferExecutor
{
    SimulatedOutcome Plan(TransferRequest request, DateTimeOffset startUtc);
}

public sealed class SimulatedTransferExecutor(IOptions<WorkSyncGrainOptions> options) : ITransferExecutor
{
    private readonly DriftProfile _drift = DriftProfile.Parse(options.Value.SimulationDrift);

    public SimulatedOutcome Plan(TransferRequest request, DateTimeOffset startUtc) =>
        TransferSimulator.Simulate(request, startUtc, Random.Shared, _drift);
}
