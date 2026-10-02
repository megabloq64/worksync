namespace WorkSync.Grains;

/// <summary>Thrown when no model has been trained/loaded yet. The API maps it to 503.</summary>
[GenerateSerializer, Alias("worksync.ModelNotReadyException")]
public sealed class ModelNotReadyException : Exception
{
    public ModelNotReadyException()
        : base("No model is loaded yet. Ingest at least a few hundred finished transfers or run training.")
    {
    }

    public ModelNotReadyException(string message) : base(message)
    {
    }

    public ModelNotReadyException(string message, Exception inner) : base(message, inner)
    {
    }
}
