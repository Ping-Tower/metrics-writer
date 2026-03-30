# Metrics Writer Agent Guide

## Current State

- `metrics-writer` is currently empty in this workspace: no source files, configs, or build files were found.
- This service is intended to be implemented as a `.NET Worker Service`, not as a Go service.
- This guide is based on the verified message contracts and infrastructure files in:
  - `/home/semao0/Projects/PingTower/infra/rabbitmq/asyncapi.yaml`
  - `/home/semao0/Projects/PingTower/infra/rabbitmq/config/definitions.json`
  - `/home/semao0/Projects/PingTower/infra/clickhouse/clickhouse-init/init.sql`
  - `/home/semao0/Projects/PingTower/gitlab-profile/images/schema.png`
  - `/home/semao0/Projects/PingTower/gitlab-profile/images/db_schema.png`

## Service Role

- `metrics-writer` is the service that persists raw ping samples to ClickHouse.
- It is a downstream consumer of `ping-service`.
- It should:
  - consume `server.ping.recorded`
  - validate and normalize payloads
  - write one row per event into ClickHouse
- It should not:
  - aggregate final server state
  - send notifications
  - mutate monitoring target definitions
  - publish `server.status.changed`

## System Boundaries

- `ping-service` owns probe execution and publishes raw ping results.
- `metrics-writer` owns persistence of those raw results into ClickHouse.
- `state-elevator` is a parallel consumer of the same ping event stream and owns aggregation/hot state.
- `api` reads analytics data later; `metrics-writer` should stay write-focused.

## RabbitMQ Contract

- Broker topology source of truth:
  - `/home/semao0/Projects/PingTower/infra/rabbitmq/asyncapi.yaml`
  - `/home/semao0/Projects/PingTower/infra/rabbitmq/config/definitions.json`
- `metrics-writer` consumer queue:
  - `q.metrics-writer.ping-events`
- `metrics-writer` subscribes to:
  - exchange: `pingEventsExchange`
  - routing key: `server.ping.recorded`
- In the current topology, exchange/queue/binding are provisioned centrally in `infra`.
- Do not declare broker topology in service code unless the infrastructure approach is intentionally changed everywhere.

## Incoming Message Shape

- `metrics-writer` consumes `PingRecordedPayload`.
- Required fields:
  - `id`
  - `serverId`
  - `protocol`
  - `timestamp`
  - `isSuccess`
- Optional fields:
  - `latencyMs`
  - `errorMessage`
  - `statusCode`
  - `certExpiresAt`
  - `tlsVersion`
  - `dnsLookupMs`
  - `sentBytes`
  - `receivedBytes`
  - `packetLossPercent`
  - `rttMinMs`
  - `rttMaxMs`
  - `ttl`
- Preserve JSON field names exactly as defined in `asyncapi.yaml`.

## ClickHouse Contract

- ClickHouse schema source of truth:
  - `/home/semao0/Projects/PingTower/infra/clickhouse/clickhouse-init/init.sql`
- Current target table:
  - database: `pingtower_analytics`
  - table: `server_pings`
- Current column mapping:
  - `id` -> `UUID`
  - `server_id` -> `String`
  - `protocol` -> `String`
  - `timestamp` -> `DateTime64(3, 'UTC')`
  - `is_success` -> `Bool`
  - `latency_ms` -> `Nullable(Float64)`
  - `error_message` -> `Nullable(String)`
  - `status_code` -> `Nullable(Int32)`
  - `cert_expires_at` -> `Nullable(DateTime64(3, 'UTC'))`
  - `tls_version` -> `Nullable(String)`
  - `dns_lookup_ms` -> `Nullable(Float64)`
  - `sent_bytes` -> `Nullable(Int64)`
  - `received_bytes` -> `Nullable(Int64)`
  - `packet_loss_percent` -> `Nullable(Float64)`
  - `rtt_min_ms` -> `Nullable(Float64)`
  - `rtt_max_ms` -> `Nullable(Float64)`
  - `ttl` -> `Nullable(Int32)`

## Write Semantics

- Treat each incoming event as an append-only analytics record.
- `metrics-writer` should write exactly one row per successfully handled message.
- Prefer idempotency keyed by event `id` where practical, because RabbitMQ redelivery can happen.
- If exact deduplication is not available yet, document that risk explicitly instead of assuming at-most-once delivery.

## Validation Rules

- Reject malformed messages rather than writing partial garbage rows.
- Validate required fields before insert:
  - `id`
  - `serverId`
  - `protocol`
  - `timestamp`
  - `isSuccess`
- Keep timestamps in UTC.
- Do not coerce protocol names to custom aliases; keep wire values aligned with the contract.

## Ownership Rules

- `metrics-writer` owns storage adaptation, not business interpretation.
- Do not derive final UP/DOWN status inside this service.
- Do not write to PostgreSQL or Redis from this service unless the architecture is intentionally changed.
- Keep message DTOs separate from ClickHouse row models when implementation begins.
- Prefer staying aligned with the existing C# service style already present in `api` and `email-service`.

## Change Rules

- If you change the ping event payload, update all of:
  - `/home/semao0/Projects/PingTower/infra/rabbitmq/asyncapi.yaml`
  - `ping-service`
  - `metrics-writer`
  - any other consumers such as `state-elevator`
- If you change the ClickHouse schema, update both:
  - `/home/semao0/Projects/PingTower/infra/clickhouse/clickhouse-init/init.sql`
  - the `metrics-writer` row mapping and inserts
- If you rename exchange, queue, or routing key, keep `asyncapi.yaml` and `definitions.json` synchronized.

## Execution Rules

- Commands that depend on network access, package registries, external brokers, or remote databases should not be retried repeatedly inside sandbox.
- For such commands, prefer running outside sandbox immediately or request escalation right away.
- Typical examples:
  - dependency installation
  - `dotnet restore`
  - `dotnet build` if packages must be downloaded
  - integration checks against RabbitMQ or ClickHouse
- Pure local actions should still stay inside sandbox:
  - editing files
  - formatting
  - local static inspection
  - builds with all dependencies already available locally

## Implementation Guidance

- Implement this service as a `.NET Worker Service`, preferably aligned with the shape already used by `email-service`.
- Prefer a structure with:
  - `Application` for DTOs and contracts
  - `Infrastructure` for RabbitMQ and ClickHouse integration
  - `Worker` or `MetricsWriter` host project for `Program.cs` and background worker entrypoint
- Reuse the same ecosystem already present in the repository where sensible:
  - `RabbitMQ.Client`
  - `ClickHouse.Client`
  - `Dapper` for insert statements when a thin repository is enough
  - `Microsoft.Extensions.Hosting`, `Options`, `Logging`, `DI`
- Follow the same C# conventions visible in `api` and `email-service`:
  - nullable reference types enabled
  - `net10.0`
  - `appsettings` / env-based configuration through options classes
  - infrastructure registration via DI extension methods
- Prefer batch inserts if throughput becomes important, but keep failure handling explicit.
- Acknowledge RabbitMQ messages only after the ClickHouse write succeeds.
- On malformed payloads, prefer reject/dead-letter behavior over infinite requeue loops.
- Log write failures with `serverId` and event `id`.

## Practical Starting Point

- Before adding code, read:
  - `/home/semao0/Projects/PingTower/infra/rabbitmq/asyncapi.yaml`
  - `/home/semao0/Projects/PingTower/infra/rabbitmq/config/definitions.json`
  - `/home/semao0/Projects/PingTower/infra/clickhouse/clickhouse-init/init.sql`
- Treat those files as the current source of truth for `metrics-writer` behavior until real code appears in this directory.
