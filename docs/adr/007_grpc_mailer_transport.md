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
sender**, so the numbers reflect transport overhead, not SMTP. Every run sends
a warm-up batch first (past cold-start: connection setup, HTTP/2 window
ramp-up) before timing starts, so a single run isn't dominated by first-use
cost or OS scheduling jitter in that window. Both paths are also instrumented
with `mailer_deliveries_total` and `mailer_delivery_duration_seconds`, labeled
by `transport`, for live comparison in Grafana. Run `make bench:grpc`,
`make bench:grpc-stream`, `make bench:amqp` (the last needs a running broker);
results are captured in `loadtest-results.log`, including the CPU model and
core count each run measured on.

TCP-loopback figures (n=20000, warmup=1000, no SMTP, 12-core i5-12500H — see
`loadtest-results.log` for the exact numbers and header):

| Transport | Throughput | p50 latency |
| --- | --- | --- |
| AMQP | ~1k msg/s | - |
| gRPC unary | ~4.3k msg/s | ~200µs |
| gRPC bidi stream (pipelined) | ~130k msg/s | not meaningful, see below |

**Why AMQP is slowest.** The harness (and `rabbitmq.Publisher` in general)
opens a fresh AMQP channel, enables publisher confirms on it, publishes, waits
synchronously for the broker's confirm, and closes the channel — per message.
`channel.open`/`channel.close` are themselves synchronous round trips to the
broker in the AMQP protocol, so this pays for a full extra request/response
on top of the publish-confirm wait, before the *next* message can even start.
None of that is free-standing RabbitMQ overhead — it's an artifact of
publishing without reusing a channel across messages, which is a known
RabbitMQ anti-pattern. It also pays for durable, persisted delivery (`Persistent`
delivery mode) and a full publish→queue→consumer→ack round trip, not just a
request/response.

**Why gRPC unary beats AMQP but trails the stream.** A unary `Send` reuses the
same `grpc.ClientConn` (and its one underlying TCP connection) across calls,
so there's no per-message connection setup, but each call still opens and
closes its own HTTP/2 stream (a `HEADERS` + `END_STREAM` frame pair) and the
benchmark loop waits for each response before issuing the next — no
pipelining. What's left is HTTP/2 stream framing overhead plus one real
network round trip per message, with nothing persisted.

**Why the bidi stream is ~30x faster than unary.** `SendStream` opens one
HTTP/2 stream for the *entire* batch, amortizing stream-setup cost across all
n messages instead of paying it n times. More importantly, the loadtest client
pipelines: a dedicated goroutine keeps calling `Send` without waiting for the
matching `Recv`, so many requests are in flight on the wire simultaneously and
the server's processing time for message *i* overlaps with the network
transfer of message *i+1, i+2, ...* instead of every round trip serializing.
This is the actual advantage bidi streaming has over unary calls — a
send-then-block-on-recv loop on a stream (what an earlier version of this
harness did) gains nothing over unary, since it still serializes one
round trip at a time.

No p50/p99 latency is reported for the pipelined stream: with the sender
racing ahead unthrottled, recv-time minus send-time mostly measures how deep
the in-flight backlog got by the time a given response was read, not
round-trip time — it comes out roughly 1000x the unary p50 for identical
work, which would misrepresent the transport rather than describe it. Only
aggregate throughput is a meaningful number for this benchmark shape; a
production caller wanting real per-message latency under streaming would need
to bound the pipeline depth (a semaphore capping in-flight messages) rather
than let it run unbounded, which is out of scope for this comparison.

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
