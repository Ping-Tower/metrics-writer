using ClickHouse.Client.ADO;
using Dapper;
using Domain;
using Microsoft.Extensions.Options;

namespace Infrastructure.ClickHouseManager;

public class ClickHousePingRecordWriter(IOptions<ClickHouseSettings> options) : IPingRecordWriter
{
    private readonly ClickHouseSettings _settings = options.Value;

    public async Task WriteAsync(PingRecord record, CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT INTO server_pings
            (
                id,
                server_id,
                protocol,
                timestamp,
                is_success,
                latency_ms,
                error_message,
                status_code,
                cert_expires_at,
                tls_version,
                dns_lookup_ms,
                sent_bytes,
                received_bytes,
                packet_loss_percent,
                rtt_min_ms,
                rtt_max_ms,
                ttl
            )
            VALUES
            (
                @Id,
                @ServerId,
                @Protocol,
                @Timestamp,
                @IsSuccess,
                @LatencyMs,
                @ErrorMessage,
                @StatusCode,
                @CertExpiresAt,
                @TlsVersion,
                @DnsLookupMs,
                @SentBytes,
                @ReceivedBytes,
                @PacketLossPercent,
                @RttMinMs,
                @RttMaxMs,
                @Ttl
            )
            """;

        await using var connection = new ClickHouseConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        var command = new CommandDefinition(sql, new
        {
            record.Id,
            record.ServerId,
            record.Protocol,
            Timestamp = record.Timestamp.ToUniversalTime(),
            record.IsSuccess,
            record.LatencyMs,
            record.ErrorMessage,
            record.StatusCode,
            CertExpiresAt = record.CertExpiresAt?.ToUniversalTime(),
            record.TlsVersion,
            record.DnsLookupMs,
            record.SentBytes,
            record.ReceivedBytes,
            record.PacketLossPercent,
            record.RttMinMs,
            record.RttMaxMs,
            record.Ttl
        }, cancellationToken: cancellationToken);

        await connection.ExecuteAsync(command);
    }
}
