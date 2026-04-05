using Domain;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;
using Polly.Timeout;

namespace Infrastructure.ClickHouseManager;

public class ResilientPingRecordWriter : IPingRecordWriter
{
    private readonly IPingRecordWriter _innerWriter;
    private readonly AsyncRetryPolicy _asyncRetryPolicy;
    private readonly AsyncCircuitBreakerPolicy _circuitBreakerPolicy;
    private readonly AsyncTimeoutPolicy _timeoutPolicy;
    private readonly ILogger<ResilientPingRecordWriter> _logger;

    public ResilientPingRecordWriter(IPingRecordWriter innerWriter, ILogger<ResilientPingRecordWriter> logger)
    {
        _innerWriter = innerWriter;
        _logger = logger;

        _asyncRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(
                retryCount: 3,
                sleepDurationProvider: retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                onRetry: (exception, timeSpan, retryCount, _) =>
                {
                    _logger.LogWarning(
                        exception,
                        "Retry {RetryCount} while writing ping metrics. Waiting {Delay}.",
                        retryCount,
                        timeSpan);
                });

        _timeoutPolicy = Policy.TimeoutAsync(TimeSpan.FromSeconds(10));

        _circuitBreakerPolicy = Policy
            .Handle<Exception>()
            .CircuitBreakerAsync(
                exceptionsAllowedBeforeBreaking: 5,
                durationOfBreak: TimeSpan.FromMinutes(2),
                onBreak: (exception, breakDelay) =>
                {
                    _logger.LogWarning(
                        exception,
                        "Metrics writer circuit breaker opened for {BreakDelay}.",
                        breakDelay);
                },
                onReset: () =>
                {
                    _logger.LogInformation("Metrics writer circuit breaker reset.");
                });
    }

    public async Task BulkInsertAsync(IReadOnlyCollection<PingRecord> records, CancellationToken cancellationToken)
    {
        var combinedPolicy = Policy.WrapAsync(_asyncRetryPolicy, _timeoutPolicy, _circuitBreakerPolicy);
        await combinedPolicy.ExecuteAsync(
            async ct => await _innerWriter.BulkInsertAsync(records, ct),
            cancellationToken);
    }
}
