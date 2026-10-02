using System.Net;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using WorkSync.DataGen;
using WorkSync.Storage;

namespace WorkSync.Api.Tests;

/// <summary>Runs the API with a co-hosted dev silo, an in-memory event store, a temp registry and no auto-seeding.</summary>
public sealed class ApiFactory : WebApplicationFactory<Program>
{
    public string RegistryPath { get; } = Path.Combine(Path.GetTempPath(), "worksync-api-tests", Guid.NewGuid().ToString("N"));

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");
        builder.UseSetting("WorkSync:Profile", "Development");
        builder.UseSetting("WorkSync:Api:CoHostSilo", "true");
        builder.UseSetting("WorkSync:EventStore:Kind", "InMemory");
        builder.UseSetting("WorkSync:Registry:Kind", "LocalFolder");
        builder.UseSetting("WorkSync:Registry:Path", RegistryPath);
        builder.UseSetting("WorkSync:Seed:Records", "0");
        builder.UseSetting("WorkSync:Silo:SiloPort", FreePort().ToString());
        builder.UseSetting("WorkSync:Silo:GatewayPort", FreePort().ToString());
        builder.UseSetting("WorkSync:Grains:Training:Iterations", "60");
        builder.UseSetting("WorkSync:Grains:TriggerCheckInterval", "00:00:00.500");
        builder.UseSetting("WorkSync:Grains:RetrainAfterNewRecords", "1000000");
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        try { Directory.Delete(RegistryPath, recursive: true); } catch (IOException) { }
    }

    private static int FreePort()
    {
        using var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        return ((IPEndPoint)l.LocalEndpoint).Port;
    }
}

public sealed class ApiTests(ApiFactory factory) : IClassFixture<ApiFactory>
{
    private static readonly object Request = new
    {
        sourceProvider = "S3",
        sourceRegion = "UsEast",
        destinationProvider = "AzureBlob",
        destinationRegion = "EuWest",
        totalBytes = 10L << 30,
        fileCount = 500,
    };

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Sequential scenario: the steps depend on training having happened (or not) in the shared silo.</summary>
    [Fact]
    public async Task Predict_is_unavailable_until_trained_then_serves_forecasts()
    {
        var http = factory.CreateClient();

        // Before any model exists: 503 with Retry-After.
        var notReady = await http.PostAsJsonAsync("/predict", Request, Ct);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, notReady.StatusCode);
        Assert.True(notReady.Headers.Contains("Retry-After"));

        // Invalid input: 400 problem details.
        var bad = await http.PostAsJsonAsync("/predict", new { sourceProvider = "S3", sourceRegion = "UsEast", destinationProvider = "S3", destinationRegion = "UsEast", totalBytes = -1, fileCount = 1 }, Ct);
        Assert.Equal(HttpStatusCode.BadRequest, bad.StatusCode);
        var malformed = await http.PostAsync("/predict", new StringContent("{not json", System.Text.Encoding.UTF8, "application/json"), Ct);
        Assert.Equal(HttpStatusCode.BadRequest, malformed.StatusCode);

        // Seed history directly into the silo's store and train.
        var store = factory.Services.GetRequiredService<IEventStore>();
        var now = DateTimeOffset.UtcNow;
        foreach (var r in new WorkloadGenerator(new WorkloadOptions { Seed = 11 }).GenerateRecords(1_200, now.AddDays(-20), now))
        {
            await store.SaveRecordAsync(r, Ct);
        }
        var run = await http.PostAsync("/training/run?reason=api-test", null, Ct);
        Assert.True(run.IsSuccessStatusCode, $"training/run returned {run.StatusCode}");

        var deadline = DateTime.UtcNow.AddSeconds(90);
        HttpResponseMessage forecast;
        while ((forecast = await http.PostAsJsonAsync("/predict", Request, Ct)).StatusCode == HttpStatusCode.ServiceUnavailable)
        {
            Assert.True(DateTime.UtcNow < deadline, "model never became available");
            await Task.Delay(300, Ct);
        }
        Assert.Equal(HttpStatusCode.OK, forecast.StatusCode);
        var body = await forecast.Content.ReadFromJsonAsync<JsonElement>(Ct);
        Assert.Equal(1, body.GetProperty("modelVersion").GetInt32());
        Assert.InRange(body.GetProperty("failureProbability").GetDouble(), 0, 1);

        var best = await http.PostAsJsonAsync("/predict/best-time", new { request = Request, horizonHours = 24, top = 2 }, Ct);
        Assert.Equal(HttpStatusCode.OK, best.StatusCode);
        Assert.Equal(2, (await best.Content.ReadFromJsonAsync<JsonElement>(Ct)).GetProperty("bestSlots").GetArrayLength());

        var models = await http.GetFromJsonAsync<JsonElement>("/models", Ct);
        Assert.Equal(1, models.GetArrayLength());

        // Transfers: start, then fetch; unknown ids are 404.
        var started = await http.PostAsJsonAsync("/transfers", Request, Ct);
        Assert.Equal(HttpStatusCode.Accepted, started.StatusCode);
        var status = await http.GetAsync(started.Headers.Location, Ct);
        Assert.Equal(HttpStatusCode.OK, status.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await http.GetAsync($"/transfers/{Guid.NewGuid()}", Ct)).StatusCode);

        Assert.Equal(HttpStatusCode.NotFound, (await http.PostAsync("/models/99/rollback", null, Ct)).StatusCode);
    }
}
