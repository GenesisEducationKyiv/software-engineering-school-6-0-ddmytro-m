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
# flags: -n <count> (default 10000), -rabbitmq-url <url>
```

### Results

`n=20000`, go 1.26.1, x86_64 (see [`loadtest-results.log`](loadtest-results.log)):

| Transport | Elapsed | Throughput |
|-----------|---------|------------|
| grpc-unary  | 1.793s | ~11,150 msg/s (p50 84µs, p99 223µs) |
| grpc-stream | 0.934s | ~21,400 msg/s |
| amqp        | 9.386s | ~2,130 msg/s |

gRPC (especially streaming) is roughly an order of magnitude faster than the
broker path, which pays for JSON encoding, publisher confirms, and per-message
acks.

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