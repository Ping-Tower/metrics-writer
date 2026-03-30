namespace Domain;

public interface IPingRecordWriter
{
    Task WriteAsync(PingRecord record, CancellationToken cancellationToken);
}
