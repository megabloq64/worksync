using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using WorkSync.Domain;

namespace WorkSync.Storage;

/// <summary>Process-local store for tests and single-process demos.</summary>
public sealed class InMemoryEventStore : IEventStore
{
    private readonly ConcurrentDictionary<Guid, ConcurrentDictionary<long, TransferEvent>> _events = new();
    private readonly ConcurrentDictionary<Guid, TransferRecord> _records = new();

    public Task<bool> AppendAsync(TransferEvent transferEvent, CancellationToken ct = default) =>
        Task.FromResult(_events.GetOrAdd(transferEvent.TransferId, _ => new()).TryAdd(transferEvent.Sequence, transferEvent));

    public Task<IReadOnlyList<TransferEvent>> GetEventsAsync(Guid transferId, CancellationToken ct = default) =>
        Task.FromResult<IReadOnlyList<TransferEvent>>(_events.TryGetValue(transferId, out var e)
            ? e.Values.OrderBy(x => x.Sequence).ToArray()
            : []);

    public Task<bool> SaveRecordAsync(TransferRecord record, CancellationToken ct = default) =>
        Task.FromResult(_records.TryAdd(record.TransferId, record));

    public Task<TransferRecord?> GetRecordAsync(Guid transferId, CancellationToken ct = default) =>
        Task.FromResult(_records.GetValueOrDefault(transferId));

    public async IAsyncEnumerable<TransferRecord> ReadRecordsAsync(DateTimeOffset from, DateTimeOffset to, [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var r in _records.Values.Where(r => r.EndedAt >= from && r.EndedAt < to).OrderBy(r => r.EndedAt))
        {
            ct.ThrowIfCancellationRequested();
            yield return r;
        }
        await Task.CompletedTask.ConfigureAwait(false);
    }

    public Task<long> CountRecordsAsync(DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default) =>
        Task.FromResult((long)_records.Values.Count(r => r.EndedAt >= from && r.EndedAt < to));
}
