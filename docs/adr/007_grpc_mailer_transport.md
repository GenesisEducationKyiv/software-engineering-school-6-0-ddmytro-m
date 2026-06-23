# ADR 007: gRPC as an Alternative Transport for the Notifier → Mailer Link

## Status
Accepted

Builds on [ADR 005](005_rabbitmq_event_broker.md): it does **not** replace the
broker, it adds a second, opt-in transport for one link so the two can be
compared head to head.

## Context

After ADR 004/005 every cross-service hop rides RabbitMQ. There are three async
links:

| Link | Exchange | Payload | Shape |
| --- | --- | --- | --- |
| server → notifier | `events` | domain facts | fan-out |
| **notifier → mailer** | `commands` | `mq.DeliveryMessage` | command / work order |
| mailer → server (orchestrator) | `events` | saga results | fact |

We want a first-class **gRPC** interface — `.proto` contract, `buf` for linting
and generation — for exactly one of these links, while keeping the original so
throughput can be measured both ways.

The **notifier → mailer** link is the natural target:

- It is already a **command** (one message, one unit of work, one outcome), which
  maps directly onto a unary RPC. The other two links are facts with fan-out
  semantics that fit publish/subscribe better than a point-to-point call.
- `mq.DeliveryMessage` is a small, stable schema, trivial to model in proto.
- The mailer's send core is transport-agnostic (`Mailer.Deliver`), so both
  transports can drive the same logic — a fair comparison, not two code paths.

## Decision

Add a `MailerService` gRPC API as an **alternative** transport for the
notifier → mailer link. The broker path stays the default; gRPC is selected by
configuration.

### Contract (`proto/mailer/v1/mailer.proto`)

```proto
service MailerService {
  rpc Send(DeliveryCommand) returns (SendResult);
  rpc SendStream(stream DeliveryCommand) returns (stream SendResult);
}
```

- `DeliveryCommand` / `EmailType` mirror `internal/infra/mq`.
- **Unary `Send`** keeps parity with the broker's one-message-one-ack model.
- **Bidi `SendStream`** pipelines commands over one connection, so gRPC is
  measured with the same batching advantage the broker gets from prefetch.
- `buf` lints the schema and generates Go via remote managed plugins
  (`buf.gen.yaml`); a CI job runs `buf lint` + breaking-change detection.

### Wiring (coexistence, not replacement)

- The **mailer** runs the gRPC server (`MAILER_GRPC_ADDR`, on by default) *and*
  the RabbitMQ consumer at the same time. Both call the same
  `Mailer.Deliver`; the AMQP `process` handler is now a thin adapter that maps
  `Deliver`'s result to ack/retry/dead-letter.
- The **notifier** selects its outbound transport with
  `NOTIFIER_DELIVERY_TRANSPORT` (`amqp` default, `grpc` opt-in). The gRPC client
  implements the same `CommandPublisher` interface as the broker publisher, so
  the notifier's core logic (dedup, mapping, settling) is unchanged. This is the
  Open/Closed seam: a new transport is added without modifying the consumer.

### Saga interaction

Verification result reporting is unchanged. Regardless of inbound transport, the
mailer still publishes `verification.delivered` / `verification.failed` to the
events exchange for the ADR 006 orchestrator. gRPC only replaces the *inbound*
command hop, not the saga's result channel.

### Measuring throughput

`cmd/loadtest` drives N commands through a chosen transport against a **no-op
sender**, so the numbers reflect transport overhead, not SMTP. Both paths are
also instrumented with `mailer_deliveries_total` and
`mailer_delivery_duration_seconds`, labeled by `transport`, for live comparison
in Grafana. Run `make bench:grpc`, `make bench:grpc-stream`, `make bench:amqp`
(the last needs a running broker).

Indicative in-process loopback figures (10k× warm, no SMTP):

| Transport | Throughput |
| --- | --- |
| gRPC unary | ~9.4k msg/s (p50 ~0.1 ms) |
| gRPC bidi stream | ~17.7k msg/s |

AMQP figures depend on a running broker and are produced with `make bench:amqp`;
the harness reports end-to-end publish→consume throughput for an apples-to-apples
comparison.

## Consequences

### Positive

- **A real gRPC interface** with a versioned, lint-gated `.proto` contract and
  generated stubs, exercising `buf` end to end.
- **Non-destructive.** The default path is unchanged; gRPC is opt-in per service,
  so production behaviour is preserved and the two can be A/B compared without a
  rebuild.
- **One delivery core.** Extracting `Mailer.Deliver` means both transports share
  send/retry/saga-report logic; the transports are thin adapters (SRP), and the
  notifier depends only on the `CommandPublisher` abstraction (DIP/OCP).
- **Lower per-message latency** on the direct path, and meaningfully higher
  throughput when streaming, versus a broker round-trip.

### Negative

- **Loses the broker's guarantees on the gRPC path.** No persistence, tiered
  retry, or DLQ — a failed `Send` is reported synchronously and is the caller's
  problem. This is why `amqp` stays the default; `grpc` trades durability for
  latency and is appropriate only where the caller can tolerate it.
- **Tighter coupling.** The notifier must know the mailer's address and the
  mailer must be reachable when called, unlike the decoupled broker.
- **More to maintain.** A proto module, generated code, `buf` tooling, and a
  second server in the mailer.

This decision adds gRPC as a measured alternative for the one link that is
genuinely a request/response command, while keeping ADR 005's broker as the
durable default for everything else.