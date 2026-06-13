# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```shell
make run              # go run cmd/server/main.go  (alias: run:server)
make run:mailer       # go run cmd/mailer/main.go
make run:notifier     # go run cmd/notifier/main.go
make build            # build server, mailer, and notifier binaries
make build:server     # go build -o bin/server cmd/server/main.go
make build:mailer     # go build -o bin/mailer cmd/mailer/main.go
make build:notifier   # go build -o bin/notifier cmd/notifier/main.go
make lint             # golangci-lint run
make lint:fix         # golangci-lint run --fix

make test:unit        # go test -v -tags="unit" ./...
make test:integration # go test -v -tags="integration" ./...  (requires Docker)
make test             # both unit + integration

make docker:up        # docker compose --profile app up -d  (postgres, redis, rabbitmq, app, notifier, mailer)
make docker:down      # docker compose --profile app down --remove-orphans
make docker:logs      # docker compose --profile app logs -f
make docker:test      # docker compose run --rm test
```

To run a single test: `go test -v -tags="unit" -run TestName ./internal/path/to/pkg/...`

## Environment

Copy `.env.example` to `.env` and fill in values. Config loads `.env.<APP_ENV>` then `.env` at startup via `godotenv`. `APP_ENV` defaults to `development`; `production` makes `GITHUB_TOKEN` required.

Required env vars: `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `SMTP_HOST`, `SMTP_USER`, `SMTP_PASS`. See `.env.example` for all options.

## Architecture

Three separate binaries are built and deployed, communicating only through the RabbitMQ broker:
- **`cmd/server/main.go`** — runs the Scanner and the HTTP server (two goroutines); publishes domain events
- **`cmd/notifier/main.go`** — consumes events, applies notification policy, publishes email commands
- **`cmd/mailer/main.go`** — consumes email commands and sends them via SMTP

Messaging uses two RabbitMQ topic exchanges (see ADR 005): `github_scanner.events` (facts) and `github_scanner.commands` (work orders). Redis is no longer a message queue — it serves only the GitHub response cache and the notifier's dedup keys.

### HTTP layer (`internal/transport/http/`)
Gin router with a Prometheus middleware. `SubscriptionHandler` owns four routes:
- `POST /subscribe` — creates/resets a pending subscription, queues a verification email
- `GET /confirm/:token` — activates the subscription, returns `X-Api-Token` header
- `GET /unsubscribe/:token` — requires `Authorization: Bearer <api_token>` to unsubscribe
- `GET /subscriptions?email=...` — requires Bearer token, lists non-unsubscribed subscriptions

`SubscriptionHandler` depends on three interfaces (defined in `handlers/store.go`):
- `SubscriptionRepository` — all DB access; implemented by `gormSubscriptionStore`
- `RepoResolver` — GitHub repo lookup; satisfied structurally by `*github.Client`
- `EmailSender` — publishes a `subscription.created` event; satisfied by `*eventpublisher.Publisher`

### GitHub API transport stack (`internal/api/github/`)
Requests flow through a layered `http.RoundTripper` chain built in `main.go`:
```
AuthTransport → RateLimitTransport → CacheTransport → net/http
```
- **AuthTransport**: injects `Authorization: Bearer <token>` header
- **RateLimitTransport**: reads `X-RateLimit-*` and `Retry-After` response headers to maintain observed limits; exposes `GetRateLimits()` used by the scanner quota manager
- **CacheTransport**: Redis-backed HTTP response cache (full response serialized to JSON); uses `github_cache:<url>` as key; error responses use a shorter TTL
- **github.Client**: thin wrapper exposing `GetRepository` and `GetLatestRelease`; uses ETags via `If-None-Match` header

### Scanner worker (`internal/worker/scanner/`)
The scanner is a quota-aware fanout pipeline:
- **QuotaManager** (`quota.go`): derives a `rate.Limiter` from the last observed GitHub limits with a safety buffer (default 10%), respects `Retry-After` for secondary limits
- **RepoProducer** (`producer.go`): every `SCANNER_PRODUCER_INTERVAL_SECONDS`, queries repos due for scanning (idle, last scanned > `SCANNER_MIN_INTERVAL_SECONDS` ago) and enqueues them; uses pessimistic RPS so the queue empties before the next batch
- **WorkerPool** (`worker.go`): N goroutines drain the channel; each calls `quota.Wait()` before processing
- **RepoProcessor** (`processor.go`): fetches the latest release via GitHub API, compares with stored `LastRelease`; on change, notifies all active subscribers via `Notifier`
- **Notifier** (`notifier.go`): interface implemented by `*eventpublisher.Publisher`, which emits `release.detected` / `repository.moved` domain events to the broker

On startup, the scanner calls `store.RecoverStuckRepos()` to reset any repos left in `processing` state from a previous crash.

### Broker (`internal/infra/rabbitmq/`, `internal/events/`, `internal/infra/eventpublisher/`)
RabbitMQ is the message broker. `internal/events` defines the versioned event envelope and payloads. `internal/infra/rabbitmq` provides: a self-reconnecting `Connection` that re-declares topology; a `Publisher` (publisher confirms, persistent); a `Consumer` (manual ack, prefetch, `Settler` for ack/retry/dead-letter); and `topology.go` declaring both exchanges and per-consumer `QueueSet`s (main + tiered `*.retry.N` wait queues + `*.dlq`). `RetryPolicy` defines tiered exponential backoff; it is shared config so all services declare identical topology. `eventpublisher` adapts the scanner `Notifier` / handler `EmailSender` interfaces onto the events exchange. Command schema (`new_release`, `repo_moved`, `email_verification`) lives in `internal/infra/mq` (`DeliveryMessage`).

### Notifier worker (`internal/worker/notifier/`)
Consumes the `notifications` queue, deduplicates events by envelope ID (`internal/infra/redis` `Dedup`, `SET NX`), maps each event to a `DeliveryMessage`, and publishes it to the commands exchange. Acks only after a successful publish; rolls back the dedup key and retries on failure; dead-letters poison/unknown events.

### Mailer worker (`internal/worker/mailer/`)
Consumes the `email.delivery` queue (N concurrent consumers). Builds an email per `DeliveryMessage` event type and sends via `internal/infra/smtp` with in-process exponential backoff, falling back to broker retry/DLQ.

### Database (`internal/infra/db/`)
GORM + PostgreSQL. Schema is auto-migrated on startup. Two models:
- `Repository`: tracks `GitHubID`, `Owner/Name`, `LastRelease` (embedded), `Status` (idle/processing), `LastScannedAt`
- `Subscription`: tracks `Email`, `RepositoryID`, `Status` (pending/active/unsubscribed), `ConfirmToken`, `APIToken`

`db.Get()` is a singleton initialized with `sync.Once`. Config is loaded via `config.LoadServerConfig()` (server), `config.LoadNotifierConfig()` (notifier), or `config.LoadMailerConfig()` (mailer) — each reads only the env vars its service needs. The shared retry-tier settings live on `RabbitMQConfig` (`RABBITMQ_RETRY_*`) so all services declare matching topology.

## Testing

Tests use build tags. Unit tests are self-contained; integration tests use **testcontainers** (requires Docker) to spin up real RabbitMQ, Redis, and PostgreSQL instances. Tag patterns:
- `//go:build unit`
- `//go:build integration`

## Documentation

Architecture docs and ADRs are in `docs/`:
- `docs/system_design.md` — high-level system design
- `docs/adr/` — Architecture Decision Records (ETags strategy, scanner design, Redis Streams for MQ, modular microservices, RabbitMQ event broker)

## Linting

`golangci-lint` v2 with `golangci.yml`. Notable enabled linters: `gosec`, `errcheck`, `staticcheck`, `contextcheck`, `dupl`, `nestif`. `goimports` enforces that project-local imports (`github.com/GenesisEducationKyiv/...`) are grouped separately from stdlib and third-party.
