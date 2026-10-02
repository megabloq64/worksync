using System.Text.Json;
using System.Text.Json.Serialization;

namespace WorkSync.Storage;

public static class StorageJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() },
    };
}
