using Microsoft.Extensions.Options;
using WorkSync.DataGen;
using WorkSync.Domain;

namespace WorkSync.Grains;

/// <summary>Executes (or plans) a transfer. Swap in a real engine adapter in place of the simulator.</summary>
public interface ITransferExecutor
{
    /// <param name="grantedDtus">DTUs reserved from the account pool; the executor runs one worker per DTU.</param>
    /// <param name="poolUtilization">Fraction of the account pool already reserved by other transfers at start.</param>
    SimulatedOutcome Plan(TransferRequest request, DateTimeOffset startUtc, int grantedDtus, double poolUtilization);
}

public sealed class SimulatedTransferExecutor(IOptions<WorkSyncGrainOptions> options) : ITransferExecutor
{
    private readonly DriftProfile _drift = DriftProfile.Parse(options.Value.SimulationDrift);

    public SimulatedOutcome Plan(TransferRequest request, DateTimeOffset startUtc, int grantedDtus, double poolUtilization) =>
        TransferSimulator.Simulate(request, startUtc, Random.Shared, _drift, grantedDtus, poolUtilization);
}
