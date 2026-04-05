using Domain;

namespace MetricsWriter;

public readonly record struct PingRecordEnvelope(PingRecord Record, ulong DeliveryTag);
