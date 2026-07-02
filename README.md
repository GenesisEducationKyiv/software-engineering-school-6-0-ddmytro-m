# github-scanner
Test Case for Genesis & KMA Software Engineering School 6.0

## Architecture

The system runs as **two independent microservices**:

| Binary | Responsibility |
|--------|----------------|
| `cmd/server` | Subscription HTTP API + GitHub repository scanner |
| `cmd/mailer` | Email delivery consumer (Redis Streams → SMTP) |

Services communicate exclusively through the `messages:delivery` Redis Stream. See [`docs/system_design.md`](docs/system_design.md) for the full design.

### Scanner
Scanner optimizes amount of requests to ensure every repository is scanned in the shortest time possible (while also allowing new repositories to be added). It uses pessimistic approach to calculate **requests per seconds (rps)** for the next batch of repositories. Cached responses don't consume API tokens, so rps is increasing over time.

Secondary limits are ommited by limiting max rps (but also handled correctly).

Safety buffer is used to ensure new subscriptions may be added at any time.

### Notifier / Mailer
Mailer runs as a separate service and consumes events from a Redis Streams MQ to ensure messages are delivered reliably. If the mailer crashes, in-flight messages are reclaimed automatically on restart (`XAUTOCLAIM`).

## Features
1. GitHub ETags are used to reduce API points usage (by a lot)
2. **Redis** is used to cache any requests from GitHub API (except for getting current limits) and to use it's MQ
3. `/subscriptions` and `/unsubscribe/:token` are protected by API Authorization Token provided in `X-API-TOKEN` header of `/confirm/:token` response.
4. **GitHub CI** runs tests and lints the code
5. **Prometheus** metrics

## Launch
Ensure that env variables are present in the .env or .env.\*APP_ENV\* (APP_ENV is development by default).
```shell
go mod download
make run          # server (scanner + HTTP)
make run:mailer   # mailer (separate terminal)
```

### Docker
Starts postgres, redis, server, and mailer together:
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