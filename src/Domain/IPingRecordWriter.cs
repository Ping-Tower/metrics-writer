namespace Domain;

public interface IPingRecordWriter
{
    Task BulkInsertAsync(IReadOnlyCollection<PingRecord> records, CancellationToken cancellationToken);
}
