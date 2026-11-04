using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using RabbitMQ.Client;

namespace CookerBeaners.Messaging;

public class EventPublisher : IDisposable
{
    // otel-tracing plan Step 3: manual propagation, not an auto-instrumentation
    // package -- same reasoning as OrderBeans'/PayBeancoins' EventPublisher.cs
    // (RabbitMQ.Client 6.8.1's old synchronous API, 30_open_questions.md Q1).
    private static readonly ActivitySource ActivitySource = new("CookerBeaners");

    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly object _channelLock = new();

    public EventPublisher(IOptions<RabbitMqOptions> options)
    {
        var o = options.Value;
        var factory = new ConnectionFactory
        {
            HostName = o.Host,
            Port = o.Port,
            UserName = o.User,
            Password = o.Pass,
            AutomaticRecoveryEnabled = true,
        };

        _connection = factory.CreateConnection("CookerBeaners-publisher");
        _channel = _connection.CreateModel();
        _channel.ExchangeDeclare(RabbitMqOptions.ExchangeName, ExchangeType.Topic, durable: true);
    }

    public bool IsConnected => _connection.IsOpen && _channel.IsOpen;

    public void Publish(string routingKey, EventEnvelope envelope)
    {
        using var activity = ActivitySource.StartActivity($"publish {routingKey}", ActivityKind.Producer);
        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.destination", routingKey);
        activity?.SetTag("order.id", envelope.OrderId);

        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(envelope));

        // Cooking happens on a per-order delay task (see Program.cs), so unlike the
        // other services' publishers, this one can be called concurrently from
        // multiple threads -- RabbitMQ.Client's IModel isn't safe for that.
        lock (_channelLock)
        {
            var properties = _channel.CreateBasicProperties();
            properties.Persistent = true;
            properties.ContentType = "application/json";

            if (activity is not null)
            {
                properties.Headers ??= new Dictionary<string, object>();
                Propagators.DefaultTextMapPropagator.Inject(
                    new PropagationContext(activity.Context, Baggage.Current),
                    properties.Headers,
                    (headers, key, value) => headers[key] = Encoding.UTF8.GetBytes(value));
            }

            _channel.BasicPublish(
                exchange: RabbitMqOptions.ExchangeName,
                routingKey: routingKey,
                basicProperties: properties,
                body: body);
        }
    }

    public void Dispose()
    {
        _channel.Dispose();
        _connection.Dispose();
    }
}
