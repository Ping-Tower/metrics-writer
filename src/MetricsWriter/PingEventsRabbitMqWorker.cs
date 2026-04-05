using System.Text;
using System.Text.Json;
using System.Threading.Channels;
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
    private readonly SemaphoreSlim _channelOperationLock = new(1, 1);
    private IConnection? _connection;
    private IChannel? _channel;
    private Channel<PingRecordEnvelope>? _pendingWrites;
    private Task? _flushLoopTask;

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
        var batchSize = NormalizeBatchSize(_rabbitMqSettings.BatchSize);
        var flushInterval = NormalizeFlushInterval(_rabbitMqSettings.FlushIntervalMs);
        var prefetchCount = NormalizePrefetchCount(_rabbitMqSettings.PrefetchCount, batchSize);
        var channelCapacity = NormalizeChannelCapacity(_rabbitMqSettings.ChannelCapacity, batchSize, prefetchCount);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (_channel is null || _channel.IsClosed)
                {
                    await ConnectToRabbitMq(cancellationToken);
                    await ConfigureQosAsync(prefetchCount, cancellationToken);
                }

                StartBatchingSession(batchSize, flushInterval, channelCapacity);

                var consumer = new AsyncEventingBasicConsumer(_channel!);
                consumer.ReceivedAsync += async (_, ea) => await BufferMessageAsync(ea, cancellationToken);

                await StartConsumerAsync(consumer, cancellationToken);

                while (!_channel!.IsClosed && !cancellationToken.IsCancellationRequested)
                {
                    if (_flushLoopTask is { IsCompleted: true })
                        await _flushLoopTask;

                    await Task.Delay(1000, cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
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
                await StopBatchingSessionAsync(CancellationToken.None);
                await CloseRabbitMqAsync(CancellationToken.None);
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await StopBatchingSessionAsync(CancellationToken.None);
        await CloseRabbitMqAsync(cancellationToken);
        await base.StopAsync(cancellationToken);
    }

    private async Task ConfigureQosAsync(ushort prefetchCount, CancellationToken cancellationToken)
    {
        await WithChannelAsync(
            async channel => await channel.BasicQosAsync(
                prefetchSize: 0,
                prefetchCount: prefetchCount,
                global: false,
                cancellationToken: cancellationToken),
            cancellationToken);
    }

    private void StartBatchingSession(int batchSize, TimeSpan flushInterval, int channelCapacity)
    {
        _pendingWrites = Channel.CreateBounded<PingRecordEnvelope>(new BoundedChannelOptions(channelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });
        _flushLoopTask = RunFlushLoopAsync(_pendingWrites.Reader, batchSize, flushInterval);
    }

    private async Task StartConsumerAsync(AsyncEventingBasicConsumer consumer, CancellationToken cancellationToken)
    {
        await WithChannelAsync(
            async channel => await channel.BasicConsumeAsync(
                queue: _rabbitMqSettings.MainQueue,
                autoAck: false,
                consumer: consumer,
                cancellationToken: cancellationToken),
            cancellationToken);
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

    private async Task BufferMessageAsync(BasicDeliverEventArgs delivery, CancellationToken cancellationToken)
    {
        try
        {
            var body = delivery.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            var pingMessage = JsonSerializer.Deserialize<PingRecordedMessageDto>(message, JsonSerializerOptions);

            if (pingMessage is null || pingMessage.Id == Guid.Empty || string.IsNullOrWhiteSpace(pingMessage.ServerId) || string.IsNullOrWhiteSpace(pingMessage.Protocol))
            {
                _logger.LogWarning("Invalid ping metrics message: {Message}", message);
                await NackAsync(delivery.DeliveryTag, requeue: false, cancellationToken);
                return;
            }

            var pendingWrites = _pendingWrites ?? throw new InvalidOperationException("Pending write buffer is not initialized.");
            await pendingWrites.Writer.WriteAsync(
                new PingRecordEnvelope(MapToPingRecord(pingMessage), delivery.DeliveryTag),
                cancellationToken);
        }
        catch (JsonException jsonEx)
        {
            _logger.LogError(jsonEx, "Invalid JSON format. Sending message directly to DLQ.");
            await NackAsync(delivery.DeliveryTag, requeue: false, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (ChannelClosedException)
        {
            await NackAsync(delivery.DeliveryTag, requeue: true, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error while buffering ping metrics. Message will be requeued.");
            await NackAsync(delivery.DeliveryTag, requeue: true, cancellationToken);
        }
    }

    private async Task RunFlushLoopAsync(ChannelReader<PingRecordEnvelope> reader, int batchSize, TimeSpan flushInterval)
    {
        while (await reader.WaitToReadAsync())
        {
            var batch = new List<PingRecordEnvelope>(batchSize);
            while (batch.Count < batchSize && reader.TryRead(out var bufferedPing))
            {
                batch.Add(bufferedPing);
            }

            if (batch.Count == 0)
                continue;

            await FillBatchUntilDeadlineAsync(reader, batch, batchSize, flushInterval);
            await FlushBatchAsync(batch);
        }
    }

    private static async Task FillBatchUntilDeadlineAsync(
        ChannelReader<PingRecordEnvelope> reader,
        List<PingRecordEnvelope> batch,
        int batchSize,
        TimeSpan flushInterval)
    {
        var deadline = DateTime.UtcNow + flushInterval;

        while (batch.Count < batchSize)
        {
            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
                return;

            using var timeoutCts = new CancellationTokenSource(remaining);
            try
            {
                batch.Add(await reader.ReadAsync(timeoutCts.Token));

                while (batch.Count < batchSize && reader.TryRead(out var bufferedPing))
                {
                    batch.Add(bufferedPing);
                }
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
            {
                return;
            }
            catch (ChannelClosedException)
            {
                return;
            }
        }
    }

    private async Task FlushBatchAsync(IReadOnlyList<PingRecordEnvelope> batch)
    {
        try
        {
            await _pingRecordWriter.BulkInsertAsync(batch.Select(x => x.Record).ToArray(), CancellationToken.None);
            await AckBatchAsync(batch, CancellationToken.None);

            _logger.LogInformation("Flushed ping metrics batch. Size: {BatchSize}", batch.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to insert batch into ClickHouse. Nacking {Count} messages.", batch.Count);
            await NackBatchAsync(batch, CancellationToken.None);
            throw;
        }
    }

    private async Task StopBatchingSessionAsync(CancellationToken cancellationToken)
    {
        _pendingWrites?.Writer.TryComplete();

        if (_flushLoopTask is not null)
        {
            try
            {
                await _flushLoopTask.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Metrics flush loop stopped with an error.");
            }
        }

        _flushLoopTask = null;
        _pendingWrites = null;
    }

    private async Task AckAsync(ulong deliveryTag, CancellationToken cancellationToken)
    {
        await WithChannelAsync(
            async channel => await channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken),
            cancellationToken);
    }

    private async Task AckBatchAsync(IReadOnlyList<PingRecordEnvelope> batch, CancellationToken cancellationToken)
    {
        await _channelOperationLock.WaitAsync(cancellationToken);
        try
        {
            if (_channel is null || _channel.IsClosed)
                return;

            foreach (var bufferedPing in batch)
            {
                await _channel.BasicAckAsync(bufferedPing.DeliveryTag, multiple: false, cancellationToken);
            }
        }
        finally
        {
            _channelOperationLock.Release();
        }
    }

    private async Task NackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken)
    {
        await WithChannelAsync(
            async channel => await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: requeue, cancellationToken),
            cancellationToken);
    }

    private async Task NackBatchAsync(IReadOnlyList<PingRecordEnvelope> batch, CancellationToken cancellationToken)
    {
        await _channelOperationLock.WaitAsync(cancellationToken);
        try
        {
            if (_channel is null || _channel.IsClosed)
                return;

            foreach (var bufferedPing in batch)
            {
                await _channel.BasicNackAsync(bufferedPing.DeliveryTag, multiple: false, requeue: true, cancellationToken);
            }
        }
        finally
        {
            _channelOperationLock.Release();
        }
    }

    private async Task WithChannelAsync(Func<IChannel, Task> action, CancellationToken cancellationToken)
    {
        await _channelOperationLock.WaitAsync(cancellationToken);
        try
        {
            if (_channel is null || _channel.IsClosed)
                throw new InvalidOperationException("RabbitMQ channel is not available.");

            await action(_channel);
        }
        finally
        {
            _channelOperationLock.Release();
        }
    }

    private async Task CloseRabbitMqAsync(CancellationToken cancellationToken)
    {
        await _channelOperationLock.WaitAsync(cancellationToken);
        try
        {
            if (_channel is not null)
            {
                if (!_channel.IsClosed)
                    await _channel.CloseAsync(cancellationToken);

                _channel.Dispose();
                _channel = null;
            }

            if (_connection is not null)
            {
                if (_connection.IsOpen)
                    await _connection.CloseAsync(cancellationToken);

                _connection.Dispose();
                _connection = null;
            }
        }
        finally
        {
            _channelOperationLock.Release();
        }
    }

    private static int NormalizeBatchSize(int batchSize) => batchSize > 0 ? batchSize : 500;

    private static TimeSpan NormalizeFlushInterval(int flushIntervalMs) => TimeSpan.FromMilliseconds(flushIntervalMs > 0 ? flushIntervalMs : 1000);

    private static ushort NormalizePrefetchCount(ushort prefetchCount, int batchSize)
    {
        if (prefetchCount <= batchSize)
            throw new InvalidOperationException("RabbitMqSettings.PrefetchCount must be greater than BatchSize.");

        return prefetchCount;
    }

    private static int NormalizeChannelCapacity(int channelCapacity, int batchSize, ushort prefetchCount)
    {
        if (channelCapacity <= 0)
            throw new InvalidOperationException("RabbitMqSettings.ChannelCapacity must be greater than zero.");
        if (channelCapacity < batchSize)
            throw new InvalidOperationException("RabbitMqSettings.ChannelCapacity must be greater than or equal to BatchSize.");
        if (channelCapacity < prefetchCount)
            throw new InvalidOperationException("RabbitMqSettings.ChannelCapacity must be greater than or equal to PrefetchCount.");

        return channelCapacity;
    }
}
