using ClickHouse.Client.ADO;
using Dapper;
using Domain;
using System.Data.Common;
using System.Text;

namespace Infrastructure.ClickHouseManager;

public class ClickHousePingRecordWriter(IClickHouseConnectionFactory connectionFactory) : IPingRecordWriter
{
    public async Task BulkInsertAsync(IReadOnlyCollection<PingRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0)
            return;

        const string sqlPrefix = """
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
            """;

        var sql = new StringBuilder(sqlPrefix);
        var parameters = new DynamicParameters();
        var index = 0;

        foreach (var record in records)
        {
            if (index > 0)
                sql.Append(',');

            sql.AppendLine();
            sql.Append(
                $"(@Id{index}, @ServerId{index}, @Protocol{index}, @Timestamp{index}, @IsSuccess{index}, @LatencyMs{index}, @ErrorMessage{index}, @StatusCode{index}, @CertExpiresAt{index}, @TlsVersion{index}, @DnsLookupMs{index}, @SentBytes{index}, @ReceivedBytes{index}, @PacketLossPercent{index}, @RttMinMs{index}, @RttMaxMs{index}, @Ttl{index})");

            parameters.Add($"Id{index}", record.Id);
            parameters.Add($"ServerId{index}", record.ServerId);
            parameters.Add($"Protocol{index}", record.Protocol);
            parameters.Add($"Timestamp{index}", record.Timestamp.ToUniversalTime());
            parameters.Add($"IsSuccess{index}", record.IsSuccess);
            parameters.Add($"LatencyMs{index}", record.LatencyMs);
            parameters.Add($"ErrorMessage{index}", record.ErrorMessage);
            parameters.Add($"StatusCode{index}", record.StatusCode);
            parameters.Add($"CertExpiresAt{index}", record.CertExpiresAt?.ToUniversalTime());
            parameters.Add($"TlsVersion{index}", record.TlsVersion);
            parameters.Add($"DnsLookupMs{index}", record.DnsLookupMs);
            parameters.Add($"SentBytes{index}", record.SentBytes);
            parameters.Add($"ReceivedBytes{index}", record.ReceivedBytes);
            parameters.Add($"PacketLossPercent{index}", record.PacketLossPercent);
            parameters.Add($"RttMinMs{index}", record.RttMinMs);
            parameters.Add($"RttMaxMs{index}", record.RttMaxMs);
            parameters.Add($"Ttl{index}", record.Ttl);

            index++;
        }

        await using DbConnection connection = await connectionFactory.OpenConnectionAsync(cancellationToken);

        var command = new CommandDefinition(sql.ToString(), parameters, cancellationToken: cancellationToken);

        await connection.ExecuteAsync(command);
    }
}
