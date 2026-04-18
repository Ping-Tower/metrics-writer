using ClickHouse.Client.ADO.Parameters;
using Dapper;
using System.Data;

namespace Infrastructure.ClickHouseManager;

/// <summary>
/// Dapper IDynamicParameters that creates ClickHouseDbParameter with explicit ClickHouseType,
/// which is required for NULL values — otherwise ClickHouse receives type Nothing and fails.
/// </summary>
public sealed class ClickHouseTypedParameters : SqlMapper.IDynamicParameters
{
    private readonly List<ClickHouseDbParameter> _params = [];

    public void Add(string name, object? value, string clickHouseType)
    {
        _params.Add(new ClickHouseDbParameter
        {
            ParameterName = name,
            Value = value ?? DBNull.Value,
            ClickHouseType = clickHouseType,
        });
    }

    public void AddParameters(IDbCommand command, SqlMapper.Identity identity)
    {
        foreach (var p in _params)
            command.Parameters.Add(p);
    }
}
