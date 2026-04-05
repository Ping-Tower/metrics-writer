using System.Data.Common;
using ClickHouse.Client.ADO;
using Microsoft.Extensions.Options;

namespace Infrastructure.ClickHouseManager;

public interface IClickHouseConnectionFactory
{
    Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken);
}

public sealed class ClickHouseConnectionFactory(IOptions<ClickHouseSettings> options) : IClickHouseConnectionFactory
{
    private readonly ClickHouseSettings _settings = options.Value;

    public async Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new ClickHouseConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }
}
