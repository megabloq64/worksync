using WorkSync.Domain;

namespace WorkSync.DataGen;

public sealed record WorkloadOptions
{
    public int Seed { get; init; } = 42;
    public long MinBytes { get; init; } = 1L << 20;          // 1 MB
    public long MaxBytes { get; init; } = 500L << 30;        // 500 GB
    public DriftProfile Drift { get; init; } = DriftProfile.None;
}

/// <summary>Produces realistic random transfer requests and their simulated outcomes, deterministically for a given seed.</summary>
public sealed class WorkloadGenerator(WorkloadOptions? options = null)
{
    private static readonly CloudProvider[] s_providers = Enum.GetValues<CloudProvider>();
    private static readonly CloudRegion[] s_regions = Enum.GetValues<CloudRegion>();
    private static readonly int[] s_concurrency = [1, 2, 4, 4, 8, 8, 16, 32, 64];

    private readonly WorkloadOptions _options = options ?? new WorkloadOptions();
    private readonly Random _rng = new((options ?? new WorkloadOptions()).Seed);

    public WorkloadOptions Options => _options;

    public TransferRequest NextRequest()
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

        var tier = _rng.NextDouble() switch
        {
            < 0.15 => AccountTier.Free,
            < 0.65 => AccountTier.Standard,
            < 0.88 => AccountTier.Premium,
            _ => AccountTier.Enterprise,
        };

        return new TransferRequest(source, sourceRegion, dest, destRegion, bytes, files, s_concurrency[_rng.Next(s_concurrency.Length)], tier);
    }

    /// <summary>Generates <paramref name="count"/> finished transfers with start times spread uniformly over [from, to).</summary>
    public IEnumerable<TransferRecord> GenerateRecords(int count, DateTimeOffset from, DateTimeOffset to)
    {
        var span = (to - from).Ticks;
        for (var i = 0; i < count; i++)
        {
            var start = from.AddTicks((long)(_rng.NextDouble() * span));
            yield return Simulate(NextRequest(), start, NewGuid());
        }
    }

    public TransferRecord Simulate(TransferRequest request, DateTimeOffset startUtc, Guid transferId)
    {
        var outcome = TransferSimulator.Simulate(request, startUtc, _rng, _options.Drift);
        return new TransferRecord(transferId, request, startUtc, startUtc + outcome.Duration,
            outcome.Failed, outcome.Reason, outcome.BytesTransferred, outcome.Retries);
    }

    /// <summary>Expands a record back into the event stream that would have produced it.</summary>
    public IReadOnlyList<TransferEvent> ToEvents(TransferRecord record, int progressEvents = 2)
    {
        var events = new List<TransferEvent>
        {
            new TransferStarted(NewGuid(), record.TransferId, 0, record.StartedAt, record.Request, record.PredictedDurationSeconds, record.ModelVersion),
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
