using WorkSync.Domain;

namespace WorkSync.Storage;

/// <summary>
/// Durable store for raw transfer events and the finished-transfer records derived from them. All writes are
/// idempotent so stream redelivery (at-least-once) is harmless.
/// </summary>
public interface IEventStore
{
    /// <summary>Appends an event. Returns false when an event with the same transfer + sequence was already stored.</summary>
    Task<bool> AppendAsync(TransferEvent transferEvent, CancellationToken ct = default);

    Task<IReadOnlyList<TransferEvent>> GetEventsAsync(Guid transferId, CancellationToken ct = default);

    /// <summary>Stores a finished transfer. Returns false if it was already stored.</summary>
    Task<bool> SaveRecordAsync(TransferRecord record, CancellationToken ct = default);

    Task<TransferRecord?> GetRecordAsync(Guid transferId, CancellationToken ct = default);

    /// <summary>Records whose <see cref="TransferRecord.EndedAt"/> falls in [from, to), oldest first.</summary>
    IAsyncEnumerable<TransferRecord> ReadRecordsAsync(DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default);

    Task<long> CountRecordsAsync(DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default);
}

public static class EventStoreExtensions
{
    public static async Task<List<TransferRecord>> ReadRecordsToListAsync(this IEventStore store, DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        var list = new List<TransferRecord>();
        await foreach (var r in store.ReadRecordsAsync(from, to, ct).ConfigureAwait(false))
        {
            list.Add(r);
        }
        return list;
    }
}
