using System.Globalization;
using System.Text;
using WorkSync.Domain;

namespace WorkSync.DataGen;

/// <summary>Reads and writes <see cref="TransferRecord"/> rows as CSV (invariant culture, ISO-8601 timestamps).</summary>
public static class TransferRecordCsv
{
    private const string Header =
        "TransferId,SourceProvider,SourceRegion,DestinationProvider,DestinationRegion,TotalBytes,FileCount,Concurrency,Tier," +
        "StartedAt,EndedAt,Failed,FailureReason,BytesTransferred,Retries,PredictedDurationSeconds,ModelVersion";

    public static async Task WriteAsync(string path, IEnumerable<TransferRecord> records, CancellationToken ct = default)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
        await using var writer = new StreamWriter(path, append: false, Encoding.UTF8);
        await writer.WriteLineAsync(Header.AsMemory(), ct);
        foreach (var r in records)
        {
            var q = r.Request;
            var line = string.Join(',',
                r.TransferId.ToString(),
                q.SourceProvider, q.SourceRegion, q.DestinationProvider, q.DestinationRegion,
                q.TotalBytes.ToString(CultureInfo.InvariantCulture),
                q.FileCount.ToString(CultureInfo.InvariantCulture),
                q.Concurrency.ToString(CultureInfo.InvariantCulture),
                q.Tier,
                r.StartedAt.ToString("O", CultureInfo.InvariantCulture),
                r.EndedAt.ToString("O", CultureInfo.InvariantCulture),
                r.Failed ? "true" : "false",
                r.FailureReason,
                r.BytesTransferred.ToString(CultureInfo.InvariantCulture),
                r.Retries.ToString(CultureInfo.InvariantCulture),
                r.PredictedDurationSeconds?.ToString("R", CultureInfo.InvariantCulture) ?? "",
                r.ModelVersion?.ToString(CultureInfo.InvariantCulture) ?? "");
            await writer.WriteLineAsync(line.AsMemory(), ct);
        }
    }

    public static async IAsyncEnumerable<TransferRecord> ReadAsync(string path, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        using var reader = new StreamReader(path, Encoding.UTF8);
        var header = await reader.ReadLineAsync(ct);
        if (header is null) yield break;
        if (!string.Equals(header.Trim(), Header, StringComparison.Ordinal))
        {
            throw new InvalidDataException($"Unexpected CSV header in '{path}'.");
        }

        var lineNo = 1;
        while (await reader.ReadLineAsync(ct) is { } line)
        {
            lineNo++;
            if (string.IsNullOrWhiteSpace(line)) continue;
            yield return Parse(line, lineNo);
        }
    }

    private static TransferRecord Parse(string line, int lineNo)
    {
        var f = line.Split(',');
        if (f.Length != 17) throw new InvalidDataException($"Line {lineNo}: expected 17 fields, found {f.Length}.");
        var inv = CultureInfo.InvariantCulture;
        var request = new TransferRequest(
            Enum.Parse<CloudProvider>(f[1]), Enum.Parse<CloudRegion>(f[2]),
            Enum.Parse<CloudProvider>(f[3]), Enum.Parse<CloudRegion>(f[4]),
            long.Parse(f[5], inv), int.Parse(f[6], inv), int.Parse(f[7], inv), Enum.Parse<AccountTier>(f[8]));
        return new TransferRecord(
            Guid.Parse(f[0]), request,
            DateTimeOffset.Parse(f[9], inv, DateTimeStyles.RoundtripKind),
            DateTimeOffset.Parse(f[10], inv, DateTimeStyles.RoundtripKind),
            bool.Parse(f[11]), Enum.Parse<FailureReason>(f[12]),
            long.Parse(f[13], inv), int.Parse(f[14], inv),
            f[15].Length == 0 ? null : double.Parse(f[15], inv),
            f[16].Length == 0 ? null : int.Parse(f[16], inv));
    }
}
