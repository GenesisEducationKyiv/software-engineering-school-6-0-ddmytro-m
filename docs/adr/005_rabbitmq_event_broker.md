# ADR 005: RabbitMQ as Event Broker for Domain Events

## Status
Accepted

Amends [ADR 003](003_redis_streams_for_mq.md): Redis Streams remains in place, but its scope is narrowed to point-to-point command delivery.

## Context

ADR 003 chose Redis Streams as the message queue for the notification engine. That decision was made for a system with exactly one producer-consumer pair: the server publishes ready-made email delivery messages, the mailer sends them. For that shape — a point-to-point work queue — Redis Streams remains a good fit.

Two pressures have changed the shape of the problem:

1. **Producers decide notification policy.** Today the scanner (`RepoProcessor`) and the HTTP handler (`SubscriptionHandler`) both construct concrete email delivery messages (`DeliveryMessage`) themselves. The decision of *what notification a domain occurrence should produce* is smeared across producers instead of living in one place. Producers should state facts ("a release was detected"), not issue instructions ("send this email").

2. **One event, many consumers.** Upcoming consumers (e.g., a Telegram notifier, a webhook dispatcher, an audit log) would each need the same domain events. With Redis Streams, the *publisher* must know every stream that wants a copy and write to each one. ADR 003 explicitly listed the lack of routing topologies (exchanges, fan-out by routing key) as a known limitation; that limitation is now load-bearing.

Secondary motivations, also flagged as trade-offs in ADR 003:

- **Retry/DLQ machinery is hand-built.** The mailer carries ~150 lines of `XAUTOCLAIM` reclaim loops, manual dead-letter publishing, and retry counters. A dedicated broker provides dead-letter exchanges, per-queue TTLs, and redelivery flags declaratively.
- **Observability.** Queue depths, consumer counts, ack rates, and unroutable messages are visible in a management UI rather than requiring custom `XINFO`/`XPENDING` tooling.
- **Publish-side delivery guarantees.** Publisher confirms acknowledge durable acceptance by the broker; `XADD` durability depends on Redis persistence configuration (default RDB snapshots can lose recent writes on crash).

Candidates considered: RabbitMQ, Apache Kafka, NATS JetStream. Kafka is operationally heavy and its strengths (replayable distributed log, massive throughput) are not requirements here. NATS JetStream is lightweight but less standard tooling-wise. RabbitMQ offers the needed routing topology, mature Go client (`rabbitmq/amqp091-go`), a management UI, and a testcontainers module for integration tests.

## Decision

We will introduce **RabbitMQ** as a dedicated **event broker**, alongside the existing Redis Streams **command bus**. The two carry different kinds of messages:

| Channel | Carries | Semantics | Example |
|---------|---------|-----------|---------|
| RabbitMQ topic exchange | **Events** — facts, past tense | publish/subscribe, fan-out by routing key | `release.detected` |
| Redis Stream `messages:delivery` | **Commands** — imperatives | point-to-point work queue, single consumer type | "send this email" |

### Topology

- Durable topic exchange `github_scanner.events`.
- Producers publish domain events with routing keys equal to the event type:
  - `release.detected` — the scanner found a new release for a subscribed repository
  - `repository.moved` — the scanner detected a repository ID mismatch (moved/renamed)
  - `subscription.created` — the HTTP API registered a pending subscription needing verification
- Every event is wrapped in a versioned envelope: `{id (uuid), type, occurred_at, version, payload}`. The `id` enables consumer-side deduplication; `version` enables schema evolution.
- A new **Notification service** (`cmd/notifier`) owns a durable queue `notifications` bound to the exchange. It is the single place where notification policy lives: it consumes events, decides what notification they warrant, and publishes `DeliveryMessage` commands to the existing Redis Stream. The mailer is unchanged.

### Reliability measures

- **Durability**: durable exchange and queues, persistent delivery mode; survives broker restart.
- **Publisher confirms**: publishing fails loudly on nack/timeout so callers can retry; a failed scanner publish is additionally self-healing because the repository is rescanned next interval.
- **At-least-once consumption**: manual acks with prefetch (QoS). A message is acked only *after* the resulting command has been successfully published to the Redis Stream. A consumer crash mid-message causes broker-side redelivery.
- **Idempotent consumers**: because at-least-once implies duplicates, the notifier deduplicates on the envelope `id` (Redis `SETNX` with TTL) before emitting commands, preventing duplicate emails.
- **Retry topology**: on transient failure the message is republished to a wait queue (`notifications.retry`, per-queue TTL, dead-letter exchange pointing back to the main queue), with the attempt count tracked in a header. After N attempts the message goes to `notifications.dlq` for inspection and manual replay.
- **Poison messages**: malformed payloads and unknown event types go directly to the DLQ — never requeued, avoiding infinite redelivery loops.
- **Auto-reconnect**: publishers and consumers watch connection/channel close notifications and redial with capped exponential backoff, re-declaring topology on reconnect.
- **Process-level recovery**: all services run with container restart policies (`restart: unless-stopped`); RabbitMQ ships with a healthcheck and downstream services gate on it.

### Out of scope (deliberate)

- **Transactional outbox.** If the broker is unreachable at publish time, an event can be lost between the DB write and the publish. For release notifications this is mitigated by the rescan cycle; for verification emails the user can re-subscribe. A full outbox is noted as a future improvement, not built now.
- **Moving subscriber resolution into the Notification service.** Today the scanner resolves active subscribers and publishes one event per subscriber. A purer design publishes a single `release.detected` per repository and lets the notifier resolve subscribers, but that requires giving the notifier database access — a follow-up, not part of this change.

## Consequences

### Positive

- **Single home for notification policy**: producers emit facts; only the Notification service knows that a fact becomes an email. Adding a new channel (Telegram, webhooks) means adding a consumer queue, with zero producer changes.
- **Declarative reliability**: retries, DLQ, and redelivery come from broker primitives instead of bespoke consumer code.
- **Operational visibility**: the RabbitMQ management UI exposes queue depth, consumer liveness, and ack rates out of the box.
- **Explicit contracts**: the exchange/queue/binding topology and the versioned event envelope make the messaging architecture inspectable infrastructure.

### Negative

- **One more stateful service** to deploy, monitor, back up, and upgrade — exactly the cost ADR 003 avoided. The requirements changed; the cost is now justified.
- **Two messaging systems coexist.** The events-vs-commands split is principled but means developers must know which channel a message belongs on. The table above is the rule.
- **More concepts**: connections vs. channels, confirm modes, exchange types — a steeper learning curve than `XADD`/`XREADGROUP`.
- **At-least-once duplicates** are now an explicit concern requiring idempotency keys, where the previous single-consumer design mostly hid them.