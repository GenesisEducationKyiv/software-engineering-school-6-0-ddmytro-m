# github-scanner
Test Case for Genesis & KMA Software Engineering School 6.0

## Architecture

The system runs as **three independent microservices**:

| Binary | Responsibility |
|--------|----------------|
| `cmd/server` | Subscription HTTP API + GitHub repository scanner; publishes domain events |
| `cmd/notifier` | Notification policy consumer; emits email commands to the mailer |
| `cmd/mailer` | Email delivery consumer (RabbitMQ → SMTP) |

Services communicate exclusively through **RabbitMQ topic exchanges**: `github_scanner.events` (domain events: `release.detected`, `repository.moved`, `subscription.created`) and `github_scanner.commands` (email work orders). The server also uses a transactional outbox pattern (see [`docs/adr/006_orchestrated_saga.md`](docs/adr/006_orchestrated_saga.md)) and runs an onboarding-saga orchestrator to guarantee reliable subscription setup. See [`docs/system_design.md`](docs/system_design.md) for the full design.

### Scanner
The scanner is a quota-aware worker that identifies repositories due for a check and manages GitHub API quota. It optimizes requests per second (RPS) to scan all repositories as quickly as possible while respecting API limits and allowing new subscriptions to be added. Cached responses (via Redis) do not consume API tokens, so RPS increases over time. A safety buffer reserves quota for user-facing operations.

### Notifier
The notifier consumes domain events and applies notification policy, deciding which subscribers should receive email for each event. It publishes email commands to RabbitMQ and deduplicates by envelope ID (using Redis) to prevent duplicate emails on redelivery.

### Mailer
The mailer consumes email commands from RabbitMQ and delivers them via SMTP with in-process exponential backoff, falling back to broker-level retry and dead-letter queues for durability.

## Features
1. GitHub ETags are used to reduce API points usage (by a lot)
2. **Redis** caches GitHub API responses and stores the notifier's deduplication keys
3. **RabbitMQ** provides durable, at-least-once messaging with tiered retry and dead-letter queues
4. `/subscriptions` and `/unsubscribe/:token` are protected by API Authorization Token provided in `X-Api-Token` header of `/confirm/:token` response
5. **GitHub CI** runs tests and lints the code
6. **Prometheus** metrics

## Launch
Ensure that env variables are present in the .env or .env.\*APP_ENV\* (APP_ENV is development by default).
```shell
go mod download
make proto:tools  # install buf (once)
make proto:gen    # generate gRPC stubs (required before building; not checked into the repo)
make run          # server (scanner + HTTP) in terminal 1
make run:notifier # notifier in terminal 2
make run:mailer   # mailer in terminal 3
```

### Docker
Starts postgres, redis, rabbitmq, server (app), notifier, and mailer together:
```shell
make docker:up
make docker:down
make docker:logs
```

## Load testing

`cmd/loadtest` is a throughput harness that drives a fixed number of email
delivery commands through the notifier → mailer transport against a **no-op
email sender**, so the numbers reflect transport overhead rather than SMTP
latency. It supports three modes:

| Transport | Description |
|-----------|-------------|
| `grpc` (unary) | one `Send` RPC per command; reports throughput + p50/p99 latency |
| `grpc -stream` | a single bidi `SendStream` RPC |
| `amqp` | publishes a batch to the broker and drains it through a real mailer consumer |

The AMQP mode declares its **own ephemeral, exclusive queue** (bound to the
commands exchange with a per-process routing key) so it drains its own batch
instead of competing with a running `cmd/mailer` on the shared `email.delivery`
queue. The `amqp` mode therefore needs a reachable RabbitMQ (`make docker:up`).

```shell
make bench:grpc          # go run cmd/loadtest/main.go -transport grpc
make bench:grpc-stream   # ... -transport grpc -stream
make bench:amqp          # ... -transport amqp   (needs RabbitMQ)
# flags: -n <count> (default 10000), -warmup <count> (default 1000, capped at n/10),
#        -rabbitmq-url <url>
```

Each run sends a warm-up batch first (untimed) so cold connection setup and
HTTP/2 window ramp-up don't land inside the measured numbers.

### Results

`n=20000`, `warmup=1000` (see [`loadtest-results.log`](loadtest-results.log) for
the exact figures and the CPU model/core count they were measured on):

| Transport | Throughput | p50 latency |
|-----------|------------|-------------|
| amqp                        | ~1k msg/s   | - |
| grpc-unary                  | ~4.3k msg/s | ~200µs |
| grpc-stream (pipelined)     | ~130k msg/s | not meaningful, see ADR 007 |

gRPC unary beats AMQP because AMQP publishing opens, confirms on, and closes a
fresh channel per message (each a synchronous round trip to the broker) on top
of durable delivery, while unary reuses one connection and only pays HTTP/2
per-call framing. The bidi stream is ~30x faster than unary because the
harness pipelines sends and receives on separate goroutines instead of
blocking on each response before sending the next - the actual advantage bidi
streaming has over unary calls. See
[ADR 007](docs/adr/007_grpc_mailer_transport.md#measuring-throughput) for the
full breakdown, including why stream latency isn't reported.

## Testing
see [testing.md](testing.md)

## Linting
```shell
make lint
```
apply autofixes:
```shell
make lint:fix
```