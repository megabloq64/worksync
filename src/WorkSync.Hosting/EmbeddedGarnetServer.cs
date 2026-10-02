using Garnet;
using Microsoft.Extensions.Logging;

namespace WorkSync.Hosting;

/// <summary>In-process Garnet server for development; production points at an external Garnet deployment.</summary>
public sealed partial class EmbeddedGarnetServer : IDisposable
{
    private readonly GarnetServer _server;

    public EmbeddedGarnetServer(int port, ILoggerFactory loggerFactory)
    {
        Port = port;
        _server = new GarnetServer(["--bind", "127.0.0.1", "--port", port.ToString(System.Globalization.CultureInfo.InvariantCulture), "--memory", "256m"], loggerFactory);
        _server.Start();
        LogStarted(loggerFactory.CreateLogger<EmbeddedGarnetServer>(), port);
    }

    public int Port { get; }

    public void Dispose() => _server.Dispose();

    [LoggerMessage(Level = LogLevel.Information, Message = "Embedded Garnet listening on 127.0.0.1:{Port}")]
    private static partial void LogStarted(ILogger logger, int port);
}
