using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace NotificationsAPI.Messaging;

public sealed class PurchaseConfirmationConsumer(
    IConfiguration configuration,
    ILogger<PurchaseConfirmationConsumer> logger) : BackgroundService
{
    private const string ExchangeName = "pagamentos";
    private const string QueueName = "pagamentos.notificacoes";
    private const string RoutingKey = "pagamento.processado";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(configuration["RabbitMQ:Uri"]!),
            ClientProvidedName = "fcg-notifications"
        };

        await using var connection = await ConnectAsync(factory, stoppingToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
        await channel.ExchangeDeclareAsync(ExchangeName, ExchangeType.Direct, durable: true, cancellationToken: stoppingToken);
        await channel.QueueDeclareAsync(QueueName, durable: true, exclusive: false, autoDelete: false, cancellationToken: stoppingToken);
        await channel.QueueBindAsync(QueueName, ExchangeName, RoutingKey, cancellationToken: stoppingToken);
        await channel.BasicQosAsync(0, 1, global: false, cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, delivery) => HandleAsync(channel, delivery);
        await channel.BasicConsumeAsync(QueueName, autoAck: false, consumer: consumer, cancellationToken: stoppingToken);
        logger.LogInformation("Consumindo a fila {Queue}", QueueName);

        await channel.ExchangeDeclareAsync("usuarios", ExchangeType.Direct, durable: true, cancellationToken: stoppingToken);
        await channel.QueueDeclareAsync("usuarios.notificacoes", durable: true, exclusive: false, autoDelete: false, cancellationToken: stoppingToken);
        await channel.QueueBindAsync("usuarios.notificacoes", "usuarios", "usuario.criado", cancellationToken: stoppingToken);
        var welcomeConsumer = new AsyncEventingBasicConsumer(channel);
        welcomeConsumer.ReceivedAsync += (_, delivery) => HandleWelcomeAsync(channel, delivery);
        await channel.BasicConsumeAsync("usuarios.notificacoes", autoAck: false, consumer: welcomeConsumer, cancellationToken: stoppingToken);
        logger.LogInformation("Consumindo a fila usuarios.notificacoes");

        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
    }

    private async Task<IConnection> ConnectAsync(ConnectionFactory factory, CancellationToken cancellationToken)
    {
        while (true)
        {
            try
            {
                return await factory.CreateConnectionAsync(cancellationToken);
            }
            catch (BrokerUnreachableException exception) when (!cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning(exception, "RabbitMQ indisponível. Nova tentativa em cinco segundos.");
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }
        }
    }

    private async Task HandleAsync(IChannel channel, BasicDeliverEventArgs delivery)
    {
        PaymentProcessedEvent? payment;
        try
        {
            payment = JsonSerializer.Deserialize<PaymentProcessedEvent>(delivery.Body.Span, JsonOptions);
        }
        catch (JsonException)
        {
            payment = null;
        }

        var cancellationToken = delivery.CancellationToken;
        if (payment is null || payment.OrderId == Guid.Empty || payment.UserId == Guid.Empty ||
            payment.GameId == Guid.Empty || payment.Status is not ("Approved" or "Rejected"))
        {
            logger.LogWarning("Resultado de pagamento inválido recebido em {Queue}", QueueName);
            await channel.BasicRejectAsync(delivery.DeliveryTag, requeue: false, cancellationToken: cancellationToken);
            return;
        }

        if (payment.Status == "Approved")
        {
            logger.LogInformation(
                "E-mail de confirmação simulado para o usuário {UserId}, pedido {OrderId}, jogo {GameId}, valor {Price}",
                payment.UserId, payment.OrderId, payment.GameId, payment.Price);
        }

        await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, cancellationToken: cancellationToken);
    }

    private async Task HandleWelcomeAsync(IChannel channel, BasicDeliverEventArgs delivery)
    {
        UserCreatedEvent? user;
        try
        {
            user = JsonSerializer.Deserialize<UserCreatedEvent>(delivery.Body.Span, JsonOptions);
        }
        catch (JsonException)
        {
            user = null;
        }

        if (user is null || user.UserId == Guid.Empty || string.IsNullOrWhiteSpace(user.Email) || string.IsNullOrWhiteSpace(user.Name))
        {
            logger.LogWarning("Evento de cadastro inválido recebido em usuarios.notificacoes");
            await channel.BasicRejectAsync(delivery.DeliveryTag, requeue: false, cancellationToken: delivery.CancellationToken);
            return;
        }

        logger.LogInformation("E-mail de boas-vindas simulado para {Name} ({Email}), usuário {UserId}",
            user.Name, user.Email, user.UserId);
        await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, cancellationToken: delivery.CancellationToken);
    }

    private sealed record UserCreatedEvent(Guid UserId, string Email, string Name);
    private sealed record PaymentProcessedEvent(Guid OrderId, Guid UserId, Guid GameId, decimal Price, string Status);
}
