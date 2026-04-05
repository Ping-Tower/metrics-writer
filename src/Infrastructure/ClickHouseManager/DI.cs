using Dapper;
using Domain;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Infrastructure.ClickHouseManager;

public static class DI
{
    public static IServiceCollection AddClickHouseManager(this IServiceCollection services, IConfiguration configuration)
    {
        DefaultTypeMap.MatchNamesWithUnderscores = true;

        services.Configure<ClickHouseSettings>(configuration.GetSection("ClickHouseSettings"));
        services.AddTransient<IClickHouseConnectionFactory, ClickHouseConnectionFactory>();
        services.AddTransient<IPingRecordWriter, ClickHousePingRecordWriter>();
        services.Decorate<IPingRecordWriter, ResilientPingRecordWriter>();

        return services;
    }
}
