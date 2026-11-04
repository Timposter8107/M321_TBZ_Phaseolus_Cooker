using System.Diagnostics;
using System.Text.Json;
using CookerBeaners.HealthChecks;
using CookerBeaners.Messaging;
using CookerBeaners.Models;
using OpenTelemetry.Trace;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

// otel-tracing plan Step 3/Q4: surfaces the active trace/span ID in log scopes
// (Console:IncludeScopes in appsettings.json renders them) so a trace can be
// cross-referenced against kubectl logs / Loki without a UI click-through.
builder.Logging.Configure(options =>
{
    options.ActivityTrackingOptions = ActivityTrackingOptions.TraceId
        | ActivityTrackingOptions.SpanId
        | ActivityTrackingOptions.ParentId;
});

// otel-tracing plan Step 3: AddSource("CookerBeaners") picks up both the manual
// consume-side span (EventConsumerHostedService.cs, linked to Pay's publish span
// via the propagated traceparent header) and the publish-side spans for
// order.cooking-started/order.cooked (EventPublisher.cs). OTEL_SERVICE_NAME/
// OTEL_EXPORTER_OTLP_ENDPOINT are set as env vars in deploy/deployment.yaml,
// read automatically by AddOtlpExporter.
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()
        .AddSource("CookerBeaners")
        .AddOtlpExporter());

builder.Services.Configure<RabbitMqOptions>(builder.Configuration.GetSection(RabbitMqOptions.SectionName));
builder.Services.AddSingleton<EventPublisher>();

var ordersCookingStarted = Metrics.CreateCounter("orders_cooking_started_total", "Total number of orders that started cooking.");
var ordersCooked = Metrics.CreateCounter("orders_cooked_total", "Total number of orders that finished cooking.");

builder.Services.AddSingleton<IReadOnlyList<ConsumerBinding>>(sp =>
{
    var publisher = sp.GetRequiredService<EventPublisher>();
    var logger = sp.GetRequiredService<ILogger<Program>>();
    return new List<ConsumerBinding>
    {
        new("cooker.paid-orders", "payment.succeeded", envelope => HandlePaymentSucceededAsync(envelope, publisher, ordersCookingStarted, ordersCooked, logger)),
    };
});
builder.Services.AddHostedService<EventConsumerHostedService>();

builder.Services.AddHealthChecks()
    .AddCheck("self", () => Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckResult.Healthy(), tags: ["live"])
    .AddCheck<RabbitMqHealthCheck>("rabbitmq", tags: ["ready"]);

var app = builder.Build();

app.UseHttpMetrics();
app.MapMetrics();

app.MapHealthChecks("/healthz/live", new() { Predicate = c => c.Tags.Contains("live") });
app.MapHealthChecks("/healthz/ready", new() { Predicate = c => c.Tags.Contains("ready") });

app.Run();

static Task HandlePaymentSucceededAsync(EventEnvelope envelope, EventPublisher publisher, Counter ordersCookingStarted, Counter ordersCooked, ILogger<Program> logger)
{
    var payload = envelope.Payload.Deserialize<PaymentSucceededPayload>()!;

    ordersCookingStarted.Inc();

    var startedEnvelope = envelope with
    {
        EventId = Guid.NewGuid(),
        EventType = "order.cooking-started",
        OccurredAt = DateTimeOffset.UtcNow,
        Payload = JsonSerializer.SerializeToElement(new OrderCookingStartedPayload()),
    };
    publisher.Publish("order.cooking-started", startedEnvelope);
    logger.LogInformation("Cooking started for order {OrderId}", envelope.OrderId);

    // Not awaited: the consumer dispatches deliveries on this channel sequentially,
    // so blocking here for the cook time would stop other orders from even starting
    // to cook until this one finishes.
    _ = CookAsync(envelope, publisher, ordersCooked, CookTimes.ForOrder(payload.Items), logger);

    return Task.CompletedTask;
}

static async Task CookAsync(EventEnvelope envelope, EventPublisher publisher, Counter ordersCooked, int cookSeconds, ILogger<Program> logger)
{
    await Task.Delay(TimeSpan.FromSeconds(cookSeconds));

    var cookedEnvelope = envelope with
    {
        EventId = Guid.NewGuid(),
        EventType = "order.cooked",
        OccurredAt = DateTimeOffset.UtcNow,
        Payload = JsonSerializer.SerializeToElement(new OrderCookedPayload()),
    };

    publisher.Publish("order.cooked", cookedEnvelope);
    ordersCooked.Inc();
    logger.LogInformation("Cooking finished for order {OrderId}", envelope.OrderId);
}
