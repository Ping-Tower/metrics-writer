namespace MetricsWriter;

public class RabbitMqSettings
{
    public string? HostName { get; set; }
    public int Port { get; set; }
    public string? UserName { get; set; }
    public string? Password { get; set; }
    public string MainQueue { get; set; } = null!;
    public int BatchSize { get; set; }
    public int FlushIntervalMs { get; set; }
    public ushort PrefetchCount { get; set; }
    public int ChannelCapacity { get; set; }
}
