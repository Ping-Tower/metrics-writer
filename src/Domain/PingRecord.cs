namespace Domain;

public sealed class PingRecord
{
    public Guid Id { get; init; }

    public string ServerId { get; init; } = null!;

    public string Protocol { get; init; } = null!;

    public DateTime Timestamp { get; init; }

    public bool IsSuccess { get; init; }

    public double? LatencyMs { get; init; }

    public string? ErrorMessage { get; init; }

    public int? StatusCode { get; init; }

    public DateTime? CertExpiresAt { get; init; }

    public string? TlsVersion { get; init; }

    public double? DnsLookupMs { get; init; }

    public long? SentBytes { get; init; }

    public long? ReceivedBytes { get; init; }

    public double? PacketLossPercent { get; init; }

    public double? RttMinMs { get; init; }

    public double? RttMaxMs { get; init; }

    public int? Ttl { get; init; }
}
