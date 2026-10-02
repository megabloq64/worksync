using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using WorkSync.Domain;

namespace WorkSync.Grains;

[GenerateSerializer, Alias("worksync.DtuPoolState")]
public sealed class DtuPoolState
{
    [Id(0)] public AccountTier? Tier { get; set; }
    [Id(1)] public Dictionary<Guid, DtuReservation> Reservations { get; set; } = [];
}

/// <summary>
/// One account's DTU pool. Calls are serialized by the grain, so concurrent transfers can never over-allocate.
/// Expired leases (a transfer that crashed without releasing) are reclaimed on every call.
/// </summary>
public sealed partial class DtuPoolGrain(
    [PersistentState("dtu-pool", "worksync")] IPersistentState<DtuPoolState> state,
    IOptions<WorkSyncGrainOptions> options,
    TimeProvider clock,
    ILogger<DtuPoolGrain> logger) : Grain, IDtuPoolGrain
{
    private static readonly TimeSpan MinimumLease = TimeSpan.FromSeconds(30);

    private DtuPoolState S => state.State;

    public async Task<DtuGrant> ReserveAsync(Guid transferId, int requestedDtus, AccountTier tier, TimeSpan lease)
    {
        if (requestedDtus is < 1 or > Dtu.MaxPerTransfer)
            throw new ArgumentOutOfRangeException(nameof(requestedDtus), $"Requested DTUs must be between 1 and {Dtu.MaxPerTransfer}.");

        var dirty = Reclaim();
        if (S.Tier != tier)
        {
            S.Tier = tier;
            dirty = true;
        }

        var size = PoolSize;
        if (S.Reservations.TryGetValue(transferId, out var existing))
        {
            if (dirty) await state.WriteStateAsync();
            return new DtuGrant(transferId, existing.Requested, existing.Granted, existing.UtilizationBefore, size);
        }

        var reserved = Reserved;
        var utilization = Math.Clamp((double)reserved / size, 0, 1);
        var granted = Math.Clamp(size - reserved, 0, requestedDtus);
        if (granted > 0)
        {
            var now = clock.GetUtcNow();
            S.Reservations[transferId] = new DtuReservation(transferId, requestedDtus, granted, utilization, now, now + Clamp(lease));
            dirty = true;
        }
        if (granted < requestedDtus) LogPartialGrant(this.GetPrimaryKeyString(), transferId, requestedDtus, granted, size);
        if (dirty) await state.WriteStateAsync();
        return new DtuGrant(transferId, requestedDtus, granted, utilization, size);
    }

    public async Task<bool> RenewAsync(Guid transferId, TimeSpan lease)
    {
        Reclaim();
        if (!S.Reservations.TryGetValue(transferId, out var r)) return false;
        S.Reservations[transferId] = r with { LeaseExpiresAt = clock.GetUtcNow() + Clamp(lease) };
        await state.WriteStateAsync();
        return true;
    }

    public async Task<bool> ReleaseAsync(Guid transferId)
    {
        var dirty = Reclaim();
        var removed = S.Reservations.Remove(transferId);
        if (removed || dirty) await state.WriteStateAsync();
        return removed;
    }

    public async Task<DtuPoolStatus> GetStatusAsync(AccountTier? tier = null)
    {
        if (Reclaim()) await state.WriteStateAsync();
        var effectiveTier = tier ?? S.Tier ?? AccountTier.Standard;
        var size = options.Value.PoolSizeFor(effectiveTier);
        var reserved = Reserved;
        return new DtuPoolStatus(
            this.GetPrimaryKeyString(),
            effectiveTier,
            size,
            reserved,
            Math.Max(0, size - reserved),
            Math.Clamp((double)reserved / size, 0, 1),
            [.. S.Reservations.Values.OrderBy(r => r.ReservedAt)]);
    }

    private int PoolSize => options.Value.PoolSizeFor(S.Tier ?? AccountTier.Standard);

    private int Reserved => S.Reservations.Values.Sum(r => r.Granted);

    private static TimeSpan Clamp(TimeSpan lease) => lease < MinimumLease ? MinimumLease : lease;

    /// <summary>Drops reservations whose lease has expired. Returns true if anything changed.</summary>
    private bool Reclaim()
    {
        var now = clock.GetUtcNow();
        var expired = S.Reservations.Values.Where(r => r.LeaseExpiresAt <= now).ToArray();
        foreach (var r in expired)
        {
            S.Reservations.Remove(r.TransferId);
            LogReclaimed(this.GetPrimaryKeyString(), r.TransferId, r.Granted);
        }
        return expired.Length > 0;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Account {AccountId}: transfer {TransferId} requested {Requested} DTUs but only {Granted} of {PoolSize} were free")]
    private partial void LogPartialGrant(string accountId, Guid transferId, int requested, int granted, int poolSize);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Account {AccountId}: reclaimed {Dtus} DTUs from transfer {TransferId} after its lease expired")]
    private partial void LogReclaimed(string accountId, Guid transferId, int dtus);
}
