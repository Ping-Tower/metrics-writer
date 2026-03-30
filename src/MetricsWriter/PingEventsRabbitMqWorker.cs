using System.Text;
using System.Text.Json;
using Application.DTOs;
using Domain;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace MetricsWriter;

public class PingEventsRabbitMqWorker : BackgroundService
{
    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly ILogger<PingEventsRabbitMqWorker> _logger;
    private readonly IPingRecordWriter _pingRecordWriter;
    private readonly RabbitMqSettings _rabbitMqSettings;
    private IConnection? _connection;
    private IChannel? _channel;

    public PingEventsRabbitMqWorker(
        ILogger<PingEventsRabbitMqWorker> logger,
        IPingRecordWriter pingRecordWriter,
        IOptions<RabbitMqSettings> rabbitMqSettings)
    {
        _logger = logger;
        _pingRecordWriter = pingRecordWriter;
        _rabbitMqSettings = rabbitMqSettings.Value;
    }

    private async Task ConnectToRabbitMq(CancellationToken cancellationToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = _rabbitMqSettings.HostName!,
            Port = _rabbitMqSettings.Port,
            UserName = _rabbitMqSettings.UserName!,
            Password = _rabbitMqSettings.Password!
        };

        _connection = await factory.CreateConnectionAsync(cancellationToken);
        _channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (_channel is null || _channel.IsClosed)
                {
                    await ConnectToRabbitMq(cancellationToken);
                }

                var consumer = new AsyncEventingBasicConsumer(_channel!);
                consumer.ReceivedAsync += async (_, ea) =>
                {
                    try
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        var pingMessage = JsonSerializer.Deserialize<PingRecordedMessageDto>(message, JsonSerializerOptions);

                        if (pingMessage is null || pingMessage.Id == Guid.Empty || string.IsNullOrWhiteSpace(pingMessage.ServerId) || string.IsNullOrWhiteSpace(pingMessage.Protocol))
                        {
                            _logger.LogWarning("Invalid ping metrics message: {Message}", message);
                            await _channel!.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                            return;
                        }

                        var pingRecord = MapToPingRecord(pingMessage);
                        await _pingRecordWriter.WriteAsync(pingRecord, cancellationToken);

                        _logger.LogInformation(
                            "Ping metrics written. Id: {Id}, ServerId: {ServerId}, Protocol: {Protocol}, Success: {IsSuccess}",
                            pingMessage.Id,
                            pingMessage.ServerId,
                            pingMessage.Protocol,
                            pingMessage.IsSuccess);

                        await _channel!.BasicAckAsync(ea.DeliveryTag, multiple: false);
                    }
                    catch (JsonException jsonEx)
                    {
                        _logger.LogError(jsonEx, "Invalid JSON format. Sending message directly to DLQ.");
                        await _channel!.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error while writing ping metrics. Message will be requeued.");
                        await _channel!.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
                    }
                };

                await _channel!.BasicConsumeAsync(
                    queue: _rabbitMqSettings.MainQueue,
                    autoAck: false,
                    consumer: consumer,
                    cancellationToken: cancellationToken);

                while (!_channel!.IsClosed && !cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(1000, cancellationToken);
                }
            }
            catch (OperationInterruptedException ex) when (ex.ShutdownReason?.Initiator == ShutdownInitiator.Peer)
            {
                _logger.LogWarning("RabbitMQ connection lost. Reconnecting in 5 seconds. Reason: {Reason}", ex.ShutdownReason?.ToString());
                await Task.Delay(5000, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in RabbitMQ consumer. Reconnecting in 5 seconds...");
                await Task.Delay(5000, cancellationToken);
            }
            finally
            {
                if (_channel is not null && !_channel.IsClosed)
                {
                    await _channel.CloseAsync(cancellationToken);
                    _channel.Dispose();
                    _channel = null;
                }

                if (_connection is not null && _connection.IsOpen)
                {
                    await _connection.CloseAsync(cancellationToken);
                    _connection.Dispose();
                    _connection = null;
                }
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_channel is not null)
        {
            await _channel.CloseAsync(cancellationToken);
            _channel.Dispose();
        }

        if (_connection is not null)
        {
            await _connection.CloseAsync(cancellationToken);
            _connection.Dispose();
        }

        await base.StopAsync(cancellationToken);
    }

    private static PingRecord MapToPingRecord(PingRecordedMessageDto message)
    {
        return new PingRecord
        {
            Id = message.Id,
            ServerId = message.ServerId,
            Protocol = message.Protocol,
            Timestamp = message.Timestamp,
            IsSuccess = message.IsSuccess,
            LatencyMs = message.LatencyMs,
            ErrorMessage = message.ErrorMessage,
            StatusCode = message.StatusCode,
            CertExpiresAt = message.CertExpiresAt,
            TlsVersion = message.TlsVersion,
            DnsLookupMs = message.DnsLookupMs,
            SentBytes = message.SentBytes,
            ReceivedBytes = message.ReceivedBytes,
            PacketLossPercent = message.PacketLossPercent,
            RttMinMs = message.RttMinMs,
            RttMaxMs = message.RttMaxMs,
            Ttl = message.Ttl
        };
    }
}
