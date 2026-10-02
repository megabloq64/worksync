using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Streams;
using WorkSync.Domain;

namespace WorkSync.Grains;

[StatelessWorker]
public sealed class EventIngressGrain : Grain, IEventIngressGrain
{
    public async Task<int> PublishAsync(IReadOnlyList<TransferEvent> events)
    {
        var provider = this.GetStreamProvider(StreamNames.Provider);
        var tasks = events
            .GroupBy(e => StreamNames.PartitionFor(e.TransferId))
            .Select(g => provider.GetStream<TransferEvent>(StreamId.Create(StreamNames.TransferEvents, g.Key))
                .OnNextBatchAsync(g.OrderBy(e => e.Sequence).ToArray()));
        await Task.WhenAll(tasks);
        return events.Count;
    }
}

internal static class TransferEventStreams
{
    public static IAsyncStream<TransferEvent> For(IStreamProvider provider, Guid transferId) =>
        provider.GetStream<TransferEvent>(StreamId.Create(StreamNames.TransferEvents, StreamNames.PartitionFor(transferId)));
}
