using Orleans.Placement;
using Orleans.Runtime;
using Orleans.Runtime.Placement;

namespace WorkSync.Grains;

/// <summary>
/// Places a grain on the silo whose address is the grain's string key (<see cref="SiloAddress.ToParsableString"/>).
/// Gives us "one activation per silo" for pushing model updates; Orleans' broadcast channels deliver to one
/// subscriber activation per grain type, not to every silo.
/// </summary>
[Serializable, GenerateSerializer, Immutable, SuppressReferenceTracking, Alias("worksync.SiloKeyedPlacement")]
public sealed class SiloKeyedPlacement : PlacementStrategy
{
    public static SiloKeyedPlacement Instance { get; } = new();
}

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public sealed class SiloKeyedPlacementAttribute() : PlacementAttribute(SiloKeyedPlacement.Instance);

public sealed class SiloKeyedPlacementDirector : IPlacementDirector
{
    public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
    {
        var compatible = context.GetCompatibleSilos(target);
        var key = target.GrainIdentity.Key.ToString();
        try
        {
            var wanted = SiloAddress.FromParsableString(key);
            if (Array.IndexOf(compatible, wanted) >= 0)
            {
                return Task.FromResult(wanted);
            }
        }
        catch (FormatException)
        {
        }
        // Target silo is gone or the key isn't a silo address: any silo will do; the target catches up by polling.
        return Task.FromResult(compatible[Random.Shared.Next(compatible.Length)]);
    }
}
