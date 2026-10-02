using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Http.HttpResults;
using WorkSync.Domain;
using WorkSync.Grains;
using WorkSync.Hosting;
using WorkSync.ML;
using WorkSync.Storage;

var builder = WebApplication.CreateBuilder(args);

// Co-hosting a silo makes the API a full cluster member (simplest dev setup, and fine in prod when API nodes should
// also do work). Otherwise it connects to an existing cluster as a client.
if (builder.Configuration.GetValue("WorkSync:Api:CoHostSilo", true))
{
    builder.AddWorkSyncSilo();
}
else
{
    builder.AddWorkSyncClient();
}

builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));
builder.Services.AddProblemDetails();
builder.Services.AddExceptionHandler<WorkSyncExceptionHandler>();
builder.Services.AddOpenApi();

var app = builder.Build();
app.UseExceptionHandler();
app.MapOpenApi();

var predict = app.MapGroup("/predict").WithTags("Predictions");

predict.MapPost("/", (TransferRequest request, DateTimeOffset? startAt, IGrainFactory grains) =>
{
    request.Validate();
    return grains.GetGrain<IPredictionGrain>(0).ForecastAsync(request, startAt);
})
.WithSummary("Forecast duration (with P10–P90 range), failure probability and throughput for a transfer.");

predict.MapPost("/best-time", (BestTimeQuery query, IGrainFactory grains) =>
    grains.GetGrain<IPredictionGrain>(0).BestTimeAsync(query))
.WithSummary("Recommend the best start time within a horizon, balancing speed and failure risk.");

predict.MapPost("/anomaly", (TransferRecord record, IGrainFactory grains) =>
    grains.GetGrain<IPredictionGrain>(0).ScoreAnomalyAsync(record))
.WithSummary("Score a finished transfer: was it unusually slow for its characteristics?");

var transfers = app.MapGroup("/transfers").WithTags("Transfers");

transfers.MapPost("/", async (TransferRequest request, IGrainFactory grains) =>
{
    var id = Guid.NewGuid();
    var status = await grains.GetGrain<ITransferGrain>(id).StartAsync(request);
    return TypedResults.Accepted($"/transfers/{id}", status);
})
.WithSummary("Start a (simulated) transfer. Its events flow through the pipeline and feed continuous retraining.");

transfers.MapGet("/{id:guid}", async Task<Results<Ok<TransferStatus>, NotFound>> (Guid id, IGrainFactory grains) =>
{
    var status = await grains.GetGrain<ITransferGrain>(id).GetStatusAsync();
    return status.Request is null ? TypedResults.NotFound() : TypedResults.Ok(status);
})
.WithSummary("Live status, progress and ETA of a transfer, plus its anomaly verdict once finished.");

app.MapPost("/events/batch", async (TransferEvent[] events, IGrainFactory grains) =>
{
    if (events.Length == 0) return Results.BadRequest("No events.");
    var count = await grains.GetGrain<IEventIngressGrain>(0).PublishAsync(events);
    return Results.Accepted(value: new { accepted = count });
})
.WithTags("Ingestion")
.WithSummary("Ingest events produced by an external transfer engine.");

var models = app.MapGroup("/models").WithTags("Models");

models.MapGet("/", (IGrainFactory grains) => grains.GetGrain<ITrainingCoordinatorGrain>(WellKnownKeys.Coordinator).ListModelsAsync())
    .WithSummary("All published model versions with their metrics; IsCurrent marks the champion.");

models.MapGet("/current", async Task<Results<Ok<ModelVersionInfo>, NotFound>> (IGrainFactory grains) =>
{
    var list = await grains.GetGrain<ITrainingCoordinatorGrain>(WellKnownKeys.Coordinator).ListModelsAsync();
    return list.FirstOrDefault(m => m.IsCurrent) is { } current ? TypedResults.Ok(current) : TypedResults.NotFound();
})
.WithSummary("The champion model's manifest and holdout metrics.");

models.MapPost("/{version:int}/rollback", async (int version, IGrainFactory grains) =>
{
    await grains.GetGrain<ITrainingCoordinatorGrain>(WellKnownKeys.Coordinator).RollbackAsync(version);
    return TypedResults.NoContent();
})
.WithSummary("Make an earlier version the champion and push it to every silo.");

var training = app.MapGroup("/training").WithTags("Training");

training.MapGet("/status", (IGrainFactory grains) => grains.GetGrain<ITrainingCoordinatorGrain>(WellKnownKeys.Coordinator).GetStatusAsync())
    .WithSummary("Champion, drift indicators, active run and recent training history.");

training.MapPost("/run", async Task<Results<Accepted<TrainingRunInfo>, Conflict<string>>> (IGrainFactory grains, string? reason) =>
{
    var run = await grains.GetGrain<ITrainingCoordinatorGrain>(WellKnownKeys.Coordinator).RequestTrainingAsync(reason ?? "manual", force: true);
    return run is null ? TypedResults.Conflict("Trainer is busy or unavailable.") : TypedResults.Accepted("/training/status", run);
})
.WithSummary("Train a challenger now; it is promoted only if it beats the champion.");

app.MapGet("/", () => Results.Redirect("/openapi/v1.json")).ExcludeFromDescription();

app.Run();

internal sealed class WorkSyncExceptionHandler(IProblemDetailsService problems) : IExceptionHandler
{
    public async ValueTask<bool> TryHandleAsync(HttpContext context, Exception exception, CancellationToken cancellationToken)
    {
        var (status, title) = exception switch
        {
            ModelNotReadyException => (StatusCodes.Status503ServiceUnavailable, "Model not ready"),
            ModelVersionNotFoundException => (StatusCodes.Status404NotFound, "Model version not found"),
            BadHttpRequestException bad => (bad.StatusCode, "Bad request"),
            ArgumentException => (StatusCodes.Status400BadRequest, "Invalid request"),
            _ => (0, null),
        };
        if (status == 0) return false;

        context.Response.StatusCode = status;
        if (status == StatusCodes.Status503ServiceUnavailable) context.Response.Headers.RetryAfter = "30";
        return await problems.TryWriteAsync(new ProblemDetailsContext
        {
            HttpContext = context,
            Exception = exception,
            ProblemDetails = { Status = status, Title = title, Detail = exception.Message },
        });
    }
}

public partial class Program;
