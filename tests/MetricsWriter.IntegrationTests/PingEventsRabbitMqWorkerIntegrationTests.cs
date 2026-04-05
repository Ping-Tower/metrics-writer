using System.Reflection;
using System.Text;
using Domain;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Xunit;

namespace MetricsWriter.IntegrationTests;

public sealed class PingEventsRabbitMqWorkerIntegrationTests
{
    [Fact]
    public async Task BufferMessageAsync_ValidMessages_FlushesBatchAndAcksEachDelivery()
    {
        var writer = new CapturingPingRecordWriter();
        var channel = CreateChannel();
        var sut = CreateWorker(writer, channel);

        InvokePrivateVoid(sut, "StartBatchingSession", 2, TimeSpan.FromSeconds(1), 4);

        try
        {
            await InvokePrivateAsync(sut, "BufferMessageAsync", CreateDelivery(1, """
                {"id":"3483f340-f774-46d5-adba-0b3fc3f88735","serverId":"srv-1","protocol":"https","timestamp":"2026-04-05T10:00:00Z","isSuccess":true,"latencyMs":12.5}
                """), CancellationToken.None);
            await InvokePrivateAsync(sut, "BufferMessageAsync", CreateDelivery(2, """
                {"id":"b2bb5d5b-6abf-4bc3-8af9-35f6c2adb0cf","serverId":"srv-2","protocol":"icmp","timestamp":"2026-04-05T10:00:01Z","isSuccess":false,"errorMessage":"timeout"}
                """), CancellationToken.None);

            var batch = await writer.WaitForBatchAsync().WaitAsync(TimeSpan.FromSeconds(2));
            Assert.Equal(2, batch.Count);
            Assert.Equal("srv-1", batch[0].ServerId);
            Assert.Equal("icmp", batch[1].Protocol);
            AssertCall(channel, nameof(IChannel.BasicAckAsync), 1UL, false);
            AssertCall(channel, nameof(IChannel.BasicAckAsync), 2UL, false);
            AssertNoCall(channel, nameof(IChannel.BasicNackAsync));
        }
        finally
        {
            await InvokePrivateAsync(sut, "StopBatchingSessionAsync", CancellationToken.None);
        }
    }

    [Fact]
    public async Task BufferMessageAsync_InvalidJson_NacksWithoutCallingWriter()
    {
        var writer = new CapturingPingRecordWriter();
        var channel = CreateChannel();
        var sut = CreateWorker(writer, channel);

        await InvokePrivateAsync(sut, "BufferMessageAsync", CreateDelivery(11, "{not-json"), CancellationToken.None);

        Assert.Empty(writer.Batches);
        AssertCall(channel, nameof(IChannel.BasicNackAsync), 11UL, false, false);
        AssertNoCall(channel, nameof(IChannel.BasicAckAsync));
    }

    [Fact]
    public async Task FlushBatchAsync_WhenWriterFails_NacksBatchForRequeue()
    {
        var writer = new CapturingPingRecordWriter(() => throw new InvalidOperationException("ClickHouse unavailable"));
        var channel = CreateChannel();
        var sut = CreateWorker(writer, channel);

        InvokePrivateVoid(sut, "StartBatchingSession", 1, TimeSpan.FromSeconds(1), 2);

        try
        {
            await InvokePrivateAsync(sut, "BufferMessageAsync", CreateDelivery(21, """
                {"id":"474dd957-647e-4057-8a78-b78e802ec432","serverId":"srv-9","protocol":"tcp","timestamp":"2026-04-05T10:00:00Z","isSuccess":true}
                """), CancellationToken.None);

            var flushLoopTask = GetPrivateField<Task>(sut, "_flushLoopTask");
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () => await flushLoopTask.WaitAsync(TimeSpan.FromSeconds(2)));

            Assert.Equal("ClickHouse unavailable", exception.Message);
            AssertCall(channel, nameof(IChannel.BasicNackAsync), 21UL, false, true);
            AssertNoCall(channel, nameof(IChannel.BasicAckAsync));
        }
        finally
        {
            await InvokePrivateAsync(sut, "StopBatchingSessionAsync", CancellationToken.None);
        }
    }

    private static MetricsWriter.PingEventsRabbitMqWorker CreateWorker(IPingRecordWriter writer, IChannel channel)
    {
        var settings = Options.Create(new MetricsWriter.RabbitMqSettings
        {
            HostName = "localhost",
            Port = 5672,
            UserName = "guest",
            Password = "guest",
            MainQueue = "pingQueue",
            BatchSize = 1,
            FlushIntervalMs = 100,
            PrefetchCount = 2,
            ChannelCapacity = 2
        });

        var worker = new MetricsWriter.PingEventsRabbitMqWorker(
            NullLogger<MetricsWriter.PingEventsRabbitMqWorker>.Instance,
            writer,
            settings);

        SetPrivateField(worker, "_channel", channel);
        return worker;
    }

    private static IChannel CreateChannel()
    {
        var channel = Substitute.For<IChannel>();
        channel.IsClosed.Returns(false);
        return channel;
    }

    private static BasicDeliverEventArgs CreateDelivery(ulong deliveryTag, string body)
    {
        return new BasicDeliverEventArgs(
            consumerTag: "test-consumer",
            deliveryTag: deliveryTag,
            redelivered: false,
            exchange: "ping.exchange",
            routingKey: "server.ping.recorded",
            properties: null!,
            body: Encoding.UTF8.GetBytes(body),
            cancellationToken: CancellationToken.None);
    }

    private static async Task InvokePrivateAsync(object target, string methodName, params object[] arguments)
    {
        var method = target.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        try
        {
            var result = method.Invoke(target, arguments);
            if (result is Task task)
                await task;
        }
        catch (TargetInvocationException exception) when (exception.InnerException is not null)
        {
            throw exception.InnerException;
        }
    }

    private static void InvokePrivateVoid(object target, string methodName, params object[] arguments)
    {
        var method = target.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);
        method.Invoke(target, arguments);
    }

    private static T GetPrivateField<T>(object target, string fieldName) where T : class
    {
        var field = target.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        return Assert.IsAssignableFrom<T>(field.GetValue(target));
    }

    private static void SetPrivateField(object target, string fieldName, object value)
    {
        var field = target.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        field.SetValue(target, value);
    }

    private static void AssertCall(IChannel channel, string methodName, params object[] expectedArguments)
    {
        Assert.Contains(channel.ReceivedCalls(), call =>
            call.GetMethodInfo().Name == methodName &&
            call.GetArguments().Take(expectedArguments.Length).SequenceEqual(expectedArguments));
    }

    private static void AssertNoCall(IChannel channel, string methodName)
    {
        Assert.DoesNotContain(channel.ReceivedCalls(), call => call.GetMethodInfo().Name == methodName);
    }

    private sealed class CapturingPingRecordWriter(Func<Task>? onWrite = null) : IPingRecordWriter
    {
        private readonly TaskCompletionSource<IReadOnlyList<PingRecord>> _batchReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public List<IReadOnlyList<PingRecord>> Batches { get; } = [];

        public async Task BulkInsertAsync(IReadOnlyCollection<PingRecord> records, CancellationToken cancellationToken)
        {
            var batch = records.ToArray();
            Batches.Add(batch);
            _batchReceived.TrySetResult(batch);

            if (onWrite is not null)
                await onWrite();
        }

        public Task<IReadOnlyList<PingRecord>> WaitForBatchAsync() => _batchReceived.Task;
    }
}
