using System.Data;
using System.Data.Common;
using Domain;
using Infrastructure.ClickHouseManager;
using Xunit;

namespace MetricsWriter.UnitTests;

public sealed class ClickHousePingRecordWriterTests
{
    [Fact]
    public async Task BulkInsertAsync_DoesNotOpenConnection_WhenNoRecordsProvided()
    {
        var factory = new RecordingConnectionFactory(() => throw new InvalidOperationException("Connection should not be created."));
        var sut = new ClickHousePingRecordWriter(factory);

        await sut.BulkInsertAsync([], CancellationToken.None);

        Assert.Equal(0, factory.OpenConnectionCalls);
    }

    [Fact]
    public async Task BulkInsertAsync_BuildsSingleInsertWithUtcParameters()
    {
        var connection = new RecordingDbConnection();
        var factory = new RecordingConnectionFactory(() => connection);
        var sut = new ClickHousePingRecordWriter(factory);

        var firstTimestamp = new DateTime(2026, 4, 5, 10, 30, 0, DateTimeKind.Local);
        var secondTimestamp = new DateTime(2026, 4, 5, 11, 45, 0, DateTimeKind.Local);
        var certExpiresAt = new DateTime(2026, 4, 7, 7, 0, 0, DateTimeKind.Local);

        var records = new[]
        {
            new PingRecord
            {
                Id = Guid.Parse("6c65b5b5-6e16-49d5-8f76-5077d7f36950"),
                ServerId = "srv-1",
                Protocol = "https",
                Timestamp = firstTimestamp,
                IsSuccess = true,
                LatencyMs = 12.5,
                StatusCode = 200
            },
            new PingRecord
            {
                Id = Guid.Parse("95f4d842-e057-47f6-b076-c5f0c7b7856b"),
                ServerId = "srv-2",
                Protocol = "icmp",
                Timestamp = secondTimestamp,
                IsSuccess = false,
                ErrorMessage = "timeout",
                CertExpiresAt = certExpiresAt,
                PacketLossPercent = 75.0,
                Ttl = 48
            }
        };

        await sut.BulkInsertAsync(records, CancellationToken.None);

        var command = Assert.Single(connection.ExecutedCommands);
        Assert.Contains("INSERT INTO server_pings", command.CommandText, StringComparison.Ordinal);
        Assert.Contains("@Id0", command.CommandText, StringComparison.Ordinal);
        Assert.Contains("@Id1", command.CommandText, StringComparison.Ordinal);
        Assert.Equal(records[0].Timestamp.ToUniversalTime(), command.GetParameterValue("Timestamp0"));
        Assert.Equal(records[1].Timestamp.ToUniversalTime(), command.GetParameterValue("Timestamp1"));
        Assert.Equal(records[1].CertExpiresAt!.Value.ToUniversalTime(), command.GetParameterValue("CertExpiresAt1"));
        Assert.Equal("srv-2", command.GetParameterValue("ServerId1"));
        Assert.Equal(48, command.GetParameterValue("Ttl1"));
    }

    private sealed class RecordingConnectionFactory(Func<DbConnection> createConnection) : IClickHouseConnectionFactory
    {
        public int OpenConnectionCalls { get; private set; }

        public Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken)
        {
            OpenConnectionCalls++;
            return Task.FromResult(createConnection());
        }
    }

    private sealed class RecordingDbConnection : DbConnection
    {
        private string? _connectionString;

        public List<RecordingDbCommand> ExecutedCommands { get; } = [];

        public override string? ConnectionString
        {
            get => _connectionString;
            set => _connectionString = value;
        }

        public override string Database => "recording";

        public override string DataSource => "recording";

        public override string ServerVersion => "1.0";

        public override ConnectionState State => ConnectionState.Open;

        public override void ChangeDatabase(string databaseName)
        {
        }

        public override void Close()
        {
        }

        public override void Open()
        {
        }

        public override Task OpenAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand()
        {
            var command = new RecordingDbCommand(this);
            ExecutedCommands.Add(command);
            return command;
        }
    }

    private sealed class RecordingDbCommand(RecordingDbConnection connection) : DbCommand
    {
        private readonly RecordingDbParameterCollection _parameters = new();
        private string? _commandText;

        public override string? CommandText
        {
            get => _commandText;
            set => _commandText = value;
        }

        public override int CommandTimeout { get; set; }

        public override CommandType CommandType { get; set; } = CommandType.Text;

        public override bool DesignTimeVisible { get; set; }

        public override UpdateRowSource UpdatedRowSource { get; set; }

        protected override DbConnection? DbConnection
        {
            get => connection;
            set => throw new NotSupportedException();
        }

        protected override DbParameterCollection DbParameterCollection => _parameters;

        protected override DbTransaction? DbTransaction { get; set; }

        public override void Cancel()
        {
        }

        public override int ExecuteNonQuery() => 1;

        public override object? ExecuteScalar() => null;

        public override void Prepare()
        {
        }

        protected override DbParameter CreateDbParameter() => new RecordingDbParameter();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) => Task.FromResult(1);

        public object? GetParameterValue(string name) => _parameters.Items.Single(x => x.ParameterName == name).Value;
    }

    private sealed class RecordingDbParameter : DbParameter
    {
        public override DbType DbType { get; set; }

        public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;

        public override bool IsNullable { get; set; }

        public override string? ParameterName { get; set; }

        public override string? SourceColumn { get; set; }

        public override object? Value { get; set; }

        public override bool SourceColumnNullMapping { get; set; }

        public override int Size { get; set; }

        public override void ResetDbType()
        {
        }
    }

    private sealed class RecordingDbParameterCollection : DbParameterCollection
    {
        private readonly List<DbParameter> _parameters = [];

        public IReadOnlyList<DbParameter> Items => _parameters;

        public override int Count => _parameters.Count;

        public override object SyncRoot => ((System.Collections.ICollection)_parameters).SyncRoot;

        public override int Add(object value)
        {
            _parameters.Add((DbParameter)value);
            return _parameters.Count - 1;
        }

        public override void AddRange(Array values)
        {
            foreach (var value in values)
                Add(value!);
        }

        public override void Clear() => _parameters.Clear();

        public override bool Contains(object value) => _parameters.Contains((DbParameter)value);

        public override bool Contains(string value) => _parameters.Any(parameter => parameter.ParameterName == value);

        public override void CopyTo(Array array, int index) => _parameters.ToArray().CopyTo(array, index);

        public override System.Collections.IEnumerator GetEnumerator() => _parameters.GetEnumerator();

        protected override DbParameter GetParameter(int index) => _parameters[index];

        protected override DbParameter GetParameter(string parameterName) => _parameters.Single(parameter => parameter.ParameterName == parameterName);

        public override int IndexOf(object value) => _parameters.IndexOf((DbParameter)value);

        public override int IndexOf(string parameterName) => _parameters.FindIndex(parameter => parameter.ParameterName == parameterName);

        public override void Insert(int index, object value) => _parameters.Insert(index, (DbParameter)value);

        public override void Remove(object value) => _parameters.Remove((DbParameter)value);

        public override void RemoveAt(int index) => _parameters.RemoveAt(index);

        public override void RemoveAt(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
                _parameters.RemoveAt(index);
        }

        protected override void SetParameter(int index, DbParameter value) => _parameters[index] = value;

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
            {
                _parameters[index] = value;
                return;
            }

            _parameters.Add(value);
        }
    }
}
