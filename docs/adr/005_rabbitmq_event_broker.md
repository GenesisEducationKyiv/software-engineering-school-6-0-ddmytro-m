# ADR 005: RabbitMQ as Event Broker for Domain Events

## Status
Accepted

Supersedes [ADR 003](003_redis_streams_for_mq.md) for messaging: Redis Streams is removed as the message queue. Both events and commands now flow through RabbitMQ; Redis is retained only as the GitHub response cache and the notifier's idempotency store.

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

We will adopt **RabbitMQ** as the broker for **both** message kinds, carried on two separate exchanges. Redis is removed from the messaging path entirely; it remains only as the GitHub response cache and the notifier's idempotency store.

| Channel | Carries | Semantics | Example |
|---------|---------|-----------|---------|
| RabbitMQ topic exchange `github_scanner.events` | **Events** — facts, past tense | publish/subscribe, fan-out by routing key | `release.detected` |
| RabbitMQ topic exchange `github_scanner.commands` | **Commands** — imperatives | point-to-point work queue, single consumer type | `email.send` |

Both kinds flow through the broker, but the distinction is preserved: events are facts published to a fan-out-capable topic exchange (any number of consumers may bind), while commands are work orders addressed to one consumer type. Producers always publish to an exchange + routing key and never name a queue, so a producer never knows its consumers (see the dedicated commands exchange below).

### Topology

Two durable topic exchanges: `github_scanner.events` and `github_scanner.commands`.

- The server publishes domain events to `github_scanner.events` with routing keys equal to the event type:
  - `release.detected` — the scanner found a new release for a subscribed repository
  - `repository.moved` — the scanner detected a repository ID mismatch (moved/renamed)
  - `subscription.created` — the HTTP API registered a pending subscription needing verification
- Every event is wrapped in a versioned envelope: `{id (uuid), type, occurred_at, version, payload}`. The `id` enables consumer-side deduplication; `version` enables schema evolution.
- The **Notification service** (`cmd/notifier`) owns the durable `notifications` queue bound to the events exchange. It is the single place where notification policy lives: it consumes events, decides what notification they warrant, and publishes `DeliveryMessage` commands to `github_scanner.commands` (routing key `email.send`).
- The **mailer** (`cmd/mailer`) owns the durable `email.delivery` queue bound to the commands exchange. It consumes commands and sends email via SMTP. Because the notifier publishes to the commands *exchange* rather than the queue, adding a second command consumer (e.g. an SMS sender) is a new queue + binding with no notifier change.
- Each consumer endpoint is a queue set — main + tiered retry queues + a dead-letter queue (see Reliability).

### Reliability measures

- **Durability**: durable exchange and queues, persistent delivery mode; survives broker restart.
- **Publisher confirms**: publishing fails loudly on nack/timeout so callers can retry; a failed scanner publish is additionally self-healing because the repository is rescanned next interval.
- **At-least-once consumption**: manual acks with prefetch (QoS). A message is acked only *after* its downstream effect succeeds — the notifier acks an event only after the command is published to the broker; the mailer acks a command only after SMTP send succeeds. A consumer crash mid-message causes broker-side redelivery.
- **Idempotent consumers**: because at-least-once implies duplicates, the notifier deduplicates on the envelope `id` (Redis `SET NX` with TTL) before emitting commands, preventing duplicate emails. If the command publish then fails, the dedup key is rolled back so the retry is not skipped.
- **Tiered exponential backoff retry**: each endpoint has one wait queue per retry tier (`<queue>.retry.0..N-1`) with TTL `base * factor^i`. On transient failure a consumer republishes the message to the tier matching its attempt count, incrementing an `x-attempts` header; the wait queue dead-letters it back to the main queue (via the default exchange) after the delay. After the configured number of tiers the message goes to `<queue>.dlq`. Per-queue TTL is used rather than per-message TTL to avoid head-of-line blocking. The mailer additionally retries SMTP in-process with exponential backoff before falling back to this broker-level retry.
- **Shared retry policy**: the retry tier count and TTLs are shared config (`RABBITMQ_RETRY_*`). Every service declares the full topology on connect, so the policy must be identical across services or RabbitMQ rejects the queue redeclaration with `PRECONDITION_FAILED`.
- **Poison messages**: malformed payloads and unknown event/command types go directly to the DLQ — never requeued, avoiding infinite redelivery loops.
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
- **More concepts**: connections vs. channels, confirm modes, exchange types, tiered wait queues — a steeper learning curve than `XADD`/`XREADGROUP`.
- **At-least-once duplicates** are now an explicit concern requiring idempotency keys, where the previous single-consumer design mostly hid them.
- **Shared retry-topology config**: because every service declares the full topology, the retry policy is global rather than per-consumer; diverging values break startup.