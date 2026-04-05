using System.Reflection;
using Application.DTOs;
using Domain;
using Xunit;

namespace MetricsWriter.UnitTests;

public sealed class PingEventsRabbitMqWorkerPrivateBehaviorTests
{
    [Fact]
    public void MapToPingRecord_MapsEveryField()
    {
        var message = new PingRecordedMessageDto
        {
            Id = Guid.Parse("53695e06-b18b-42e2-b930-ebeb6dc5271f"),
            ServerId = "srv-42",
            Protocol = "tcp",
            Timestamp = new DateTime(2026, 4, 5, 12, 0, 0, DateTimeKind.Utc),
            IsSuccess = true,
            LatencyMs = 15.2,
            ErrorMessage = "none",
            StatusCode = 204,
            CertExpiresAt = new DateTime(2026, 5, 1, 0, 0, 0, DateTimeKind.Utc),
            TlsVersion = "TLS1.3",
            DnsLookupMs = 2.4,
            SentBytes = 12,
            ReceivedBytes = 48,
            PacketLossPercent = 1.5,
            RttMinMs = 10.0,
            RttMaxMs = 20.0,
            Ttl = 64
        };

        var record = (PingRecord)InvokePrivateStatic("MapToPingRecord", message)!;

        Assert.Equal(message.Id, record.Id);
        Assert.Equal(message.ServerId, record.ServerId);
        Assert.Equal(message.Protocol, record.Protocol);
        Assert.Equal(message.Timestamp, record.Timestamp);
        Assert.Equal(message.IsSuccess, record.IsSuccess);
        Assert.Equal(message.LatencyMs, record.LatencyMs);
        Assert.Equal(message.ErrorMessage, record.ErrorMessage);
        Assert.Equal(message.StatusCode, record.StatusCode);
        Assert.Equal(message.CertExpiresAt, record.CertExpiresAt);
        Assert.Equal(message.TlsVersion, record.TlsVersion);
        Assert.Equal(message.DnsLookupMs, record.DnsLookupMs);
        Assert.Equal(message.SentBytes, record.SentBytes);
        Assert.Equal(message.ReceivedBytes, record.ReceivedBytes);
        Assert.Equal(message.PacketLossPercent, record.PacketLossPercent);
        Assert.Equal(message.RttMinMs, record.RttMinMs);
        Assert.Equal(message.RttMaxMs, record.RttMaxMs);
        Assert.Equal(message.Ttl, record.Ttl);
    }

    [Theory]
    [InlineData(10, 10)]
    [InlineData(9, 10)]
    public void NormalizePrefetchCount_Throws_WhenPrefetchIsNotGreaterThanBatchSize(ushort prefetchCount, int batchSize)
    {
        var exception = Assert.Throws<TargetInvocationException>(() => InvokePrivateStatic("NormalizePrefetchCount", prefetchCount, batchSize));

        Assert.IsType<InvalidOperationException>(exception.InnerException);
        Assert.Equal("RabbitMqSettings.PrefetchCount must be greater than BatchSize.", exception.InnerException!.Message);
    }

    private static object? InvokePrivateStatic(string methodName, params object[] arguments)
    {
        var method = typeof(MetricsWriter.PingEventsRabbitMqWorker).GetMethod(
            methodName,
            BindingFlags.Static | BindingFlags.NonPublic);

        Assert.NotNull(method);
        return method.Invoke(null, arguments);
    }
}
