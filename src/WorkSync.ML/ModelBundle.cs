using System.Text.Json;
using Microsoft.ML;

namespace WorkSync.ML;

/// <summary>The three trained transformers plus their manifest. Saved as a directory of zips + manifest.json.</summary>
public sealed class ModelBundle(MLContext context, ITransformer duration, ITransformer failure, ITransformer throughput, BundleManifest manifest)
{
    public const string ManifestFile = "manifest.json";
    public static readonly string[] ModelFiles = ["duration.zip", "failure.zip", "throughput.zip"];

    public static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    public MLContext Context { get; } = context;
    public ITransformer Duration { get; } = duration;
    public ITransformer Failure { get; } = failure;
    public ITransformer Throughput { get; } = throughput;
    public BundleManifest Manifest { get; } = manifest;

    public void Save(string directory)
    {
        Directory.CreateDirectory(directory);
        var inputSchema = SchemaDefinitions.TransferInputSchema(Context);
        Context.Model.Save(Duration, inputSchema, Path.Combine(directory, ModelFiles[0]));
        Context.Model.Save(Failure, inputSchema, Path.Combine(directory, ModelFiles[1]));
        Context.Model.Save(Throughput, inputSchema, Path.Combine(directory, ModelFiles[2]));
        File.WriteAllText(Path.Combine(directory, ManifestFile), JsonSerializer.Serialize(Manifest, JsonOptions));
    }

    public static ModelBundle Load(string directory, int? seed = null)
    {
        var ctx = new MLContext(seed);
        ITransformer LoadModel(string file) => ctx.Model.Load(Path.Combine(directory, file), out _);
        var manifest = ReadManifest(directory);
        return new ModelBundle(ctx, LoadModel(ModelFiles[0]), LoadModel(ModelFiles[1]), LoadModel(ModelFiles[2]), manifest);
    }

    public static BundleManifest ReadManifest(string directory) =>
        JsonSerializer.Deserialize<BundleManifest>(File.ReadAllText(Path.Combine(directory, ManifestFile)), JsonOptions)
        ?? throw new InvalidDataException($"Invalid manifest in '{directory}'.");
}

internal static class SchemaDefinitions
{
    public static DataViewSchema TransferInputSchema(MLContext ctx) =>
        ctx.Data.LoadFromEnumerable(Array.Empty<TransferModelInput>()).Schema;
}
