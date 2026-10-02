using WorkSync.Domain;

namespace WorkSync.DataGen;

public sealed record WorkloadOptions
{
    public int Seed { get; init; } = 42;
    public long MinBytes { get; init; } = 1L << 20;          // 1 MB
    public long MaxBytes { get; init; } = 500L << 30;        // 500 GB
    public DriftProfile Drift { get; init; } = DriftProfile.None;

    /// <summary>Number of simulated customer accounts. Each owns a DTU pool sized by its tier.</summary>
    public int AccountCount { get; init; } = 40;

    /// <summary>Largest burst of transfers an account submits together (bursts create pool contention).</summary>
    public int MaxBurst { get; init; } = 6;
}

/// <summary>A simulated customer account.</summary>
public sealed record SimulatedAccount(string AccountId, AccountTier Tier)
{
    public int PoolSize => Dtu.DefaultPoolSize(Tier);
}

/// <summary>Produces realistic random transfer requests and their simulated outcomes, deterministically for a given seed.</summary>
public sealed class WorkloadGenerator(WorkloadOptions? options = null)
{
    private static readonly CloudProvider[] s_providers = Enum.GetValues<CloudProvider>();
    private static readonly CloudRegion[] s_regions = Enum.GetValues<CloudRegion>();
    // Users mostly ask for modest allocations; a few ask for everything.
    private static readonly int[] s_requestedDtus = [1, 2, 4, 4, 8, 8, 8, 16, 16, 32, 64, 128, 256];

    private readonly WorkloadOptions _options = options ?? new WorkloadOptions();
    private readonly Random _rng = new((options ?? new WorkloadOptions()).Seed);

    private SimulatedAccount[]? _accounts;

    public WorkloadOptions Options => _options;

    public IReadOnlyList<SimulatedAccount> Accounts => _accounts ??= CreateAccounts();

    /// <summary>A random request from a random simulated account.</summary>
    public TransferRequest NextRequest() => NextRequest(Accounts[_rng.Next(Accounts.Count)]);

    public TransferRequest NextRequest(SimulatedAccount account)
    {
        var source = s_providers[_rng.Next(s_providers.Length)];
        var dest = s_providers[_rng.Next(s_providers.Length)];
        var sourceRegion = s_regions[_rng.Next(s_regions.Length)];
        // Most transfers stay near their source; some go far.
        var destRegion = _rng.NextDouble() switch
        {
            < 0.35 => sourceRegion,
            < 0.70 => PickSameContinent(sourceRegion),
            _ => s_regions[_rng.Next(s_regions.Length)],
        };

        var logMin = Math.Log(_options.MinBytes);
        var logMax = Math.Log(_options.MaxBytes);
        var bytes = (long)Math.Exp(logMin + _rng.NextDouble() * (logMax - logMin));

        // Average file size between 4 KB and 2 GB, skewed toward documents/photos.
        var avgFile = Math.Exp(Math.Log(4096) + Math.Pow(_rng.NextDouble(), 1.4) * (Math.Log(2L << 30) - Math.Log(4096)));
        var files = (int)Math.Clamp(bytes / avgFile, 1, 2_000_000);

        var dtus = Math.Min(account.PoolSize, s_requestedDtus[_rng.Next(s_requestedDtus.Length)]);
        return new TransferRequest(source, sourceRegion, dest, destRegion, bytes, files, dtus, account.Tier, account.AccountId);
    }

    /// <summary>
    /// Generates <paramref name="count"/> finished transfers with start times spread over [from, to). Accounts submit
    /// bursts of transfers, and each transfer reserves DTUs from its account pool for its whole duration, so later
    /// transfers in a burst may only receive a partial grant.
    /// </summary>
    public IEnumerable<TransferRecord> GenerateRecords(int count, DateTimeOffset from, DateTimeOffset to)
    {
        var span = (to - from).Ticks;
        var planned = new List<(DateTimeOffset Start, TransferRequest Request, Guid Id)>(count);
        while (planned.Count < count)
        {
            var account = Accounts[_rng.Next(Accounts.Count)];
            var burstStart = from.AddTicks((long)(_rng.NextDouble() * span));
            var burst = Math.Min(count - planned.Count, 1 + _rng.Next(Math.Max(1, _options.MaxBurst)));
            for (var i = 0; i < burst; i++)
            {
                var start = burstStart.AddSeconds(i == 0 ? 0 : _rng.NextDouble() * 120);
                if (start >= to) start = burstStart;
                planned.Add((start, NextRequest(account), NewGuid()));
            }
        }

        var pools = new Dictionary<string, List<(DateTimeOffset End, int Dtus)>>();
        foreach (var (start, request, id) in planned.OrderBy(p => p.Start))
        {
            var pool = Dtu.DefaultPoolSize(request.Tier);
            if (!pools.TryGetValue(request.AccountId, out var active)) pools[request.AccountId] = active = [];
            active.RemoveAll(a => a.End <= start);
            var reserved = active.Sum(a => a.Dtus);
            var granted = Math.Clamp(Math.Min(request.RequestedDtus, pool - reserved), 0, request.RequestedDtus);
            var record = Simulate(request, start, id, granted, Math.Clamp((double)reserved / pool, 0, 1));
            if (granted > 0) active.Add((record.EndedAt, granted));
            yield return record;
        }
    }

    /// <param name="grantedDtus">DTUs reserved for the transfer; defaults to a full grant of the requested DTUs.</param>
    /// <param name="poolUtilization">Fraction of the account pool already reserved at start.</param>
    public TransferRecord Simulate(TransferRequest request, DateTimeOffset startUtc, Guid transferId, int? grantedDtus = null, double poolUtilization = 0)
    {
        var granted = grantedDtus ?? request.RequestedDtus;
        var outcome = TransferSimulator.Simulate(request, startUtc, _rng, _options.Drift, granted, poolUtilization);
        return new TransferRecord(transferId, request, startUtc, startUtc + outcome.Duration,
            outcome.Failed, outcome.Reason, outcome.BytesTransferred, outcome.Retries, granted, poolUtilization);
    }

    /// <summary>Expands a record back into the event stream that would have produced it.</summary>
    public IReadOnlyList<TransferEvent> ToEvents(TransferRecord record, int progressEvents = 2)
    {
        var events = new List<TransferEvent>
        {
            new TransferStarted(NewGuid(), record.TransferId, 0, record.StartedAt, record.Request, record.PredictedDurationSeconds, record.ModelVersion,
                record.GrantedDtus, record.PoolUtilizationAtStart),
        };
        for (var i = 1; i <= progressEvents; i++)
        {
            var fraction = (double)i / (progressEvents + 1);
            events.Add(new TransferProgress(NewGuid(), record.TransferId, i,
                record.StartedAt + (record.EndedAt - record.StartedAt) * fraction,
                (long)(record.BytesTransferred * fraction), (int)(record.Request.FileCount * fraction)));
        }
        var seq = progressEvents + 1;
        events.Add(record.Failed
            ? new TransferFailed(NewGuid(), record.TransferId, seq, record.EndedAt, record.FailureReason, record.BytesTransferred, record.Retries)
            : new TransferCompleted(NewGuid(), record.TransferId, seq, record.EndedAt, record.BytesTransferred, record.Retries));
        return events;
    }

    private SimulatedAccount[] CreateAccounts()
    {
        // A dedicated RNG keeps the account roster stable regardless of how many requests were drawn before.
        var rng = new Random(_options.Seed ^ 0x5EED);
        return [.. Enumerable.Range(1, Math.Max(1, _options.AccountCount)).Select(i => new SimulatedAccount(
            $"acct-{i:000}",
            rng.NextDouble() switch
            {
                < 0.15 => AccountTier.Free,
                < 0.65 => AccountTier.Standard,
                < 0.88 => AccountTier.Premium,
                _ => AccountTier.Enterprise,
            }))];
    }

    private CloudRegion PickSameContinent(CloudRegion region)
    {
        var continent = Regions.Get(region).Continent;
        var candidates = s_regions.Where(r => Regions.Get(r).Continent == continent).ToArray();
        return candidates[_rng.Next(candidates.Length)];
    }

    private Guid NewGuid()
    {
        Span<byte> bytes = stackalloc byte[16];
        _rng.NextBytes(bytes);
        return new Guid(bytes);
    }
}
