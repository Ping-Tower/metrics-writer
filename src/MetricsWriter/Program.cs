using Infrastructure;

namespace MetricsWriter;

public class Program
{
    public static void Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        builder.Services.AddInfrastructure(builder.Configuration);
        builder.Services.Configure<RabbitMqSettings>(builder.Configuration.GetSection("RabbitMqSettings"));
        builder.Services.AddHostedService<PingEventsRabbitMqWorker>();

        var host = builder.Build();
        host.Run();
    }
}
