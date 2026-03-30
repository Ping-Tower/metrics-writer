using System.Text.Json.Serialization;

namespace Application.DTOs;

public class PingRecordedMessageDto
{
    [JsonPropertyName("id")]
    public Guid Id { get; set; }

    [JsonPropertyName("serverId")]
    public string ServerId { get; set; } = null!;

    [JsonPropertyName("protocol")]
    public string Protocol { get; set; } = null!;

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; }

    [JsonPropertyName("isSuccess")]
    public bool IsSuccess { get; set; }

    [JsonPropertyName("latencyMs")]
    public double? LatencyMs { get; set; }

    [JsonPropertyName("errorMessage")]
    public string? ErrorMessage { get; set; }

    [JsonPropertyName("statusCode")]
    public int? StatusCode { get; set; }

    [JsonPropertyName("certExpiresAt")]
    public DateTime? CertExpiresAt { get; set; }

    [JsonPropertyName("tlsVersion")]
    public string? TlsVersion { get; set; }

    [JsonPropertyName("dnsLookupMs")]
    public double? DnsLookupMs { get; set; }

    [JsonPropertyName("sentBytes")]
    public long? SentBytes { get; set; }

    [JsonPropertyName("receivedBytes")]
    public long? ReceivedBytes { get; set; }

    [JsonPropertyName("packetLossPercent")]
    public double? PacketLossPercent { get; set; }

    [JsonPropertyName("rttMinMs")]
    public double? RttMinMs { get; set; }

    [JsonPropertyName("rttMaxMs")]
    public double? RttMaxMs { get; set; }

    [JsonPropertyName("ttl")]
    public int? Ttl { get; set; }
}
