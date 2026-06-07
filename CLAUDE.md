# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```shell
make run              # go run cmd/server/main.go  (alias: run:server)
make run:mailer       # go run cmd/mailer/main.go
make build            # build both server and mailer binaries
make build:server     # go build -o bin/server cmd/server/main.go
make build:mailer     # go build -o bin/mailer cmd/mailer/main.go
make lint             # golangci-lint run
make lint:fix         # golangci-lint run --fix

make test:unit        # go test -v -tags="unit" ./...
make test:integration # go test -v -tags="integration" ./...  (requires Docker)
make test             # both unit + integration

make docker:up        # docker compose --profile app up -d  (postgres, redis, app, mailer)
make docker:down      # docker compose --profile app down --remove-orphans
make docker:logs      # docker compose --profile app logs -f
make docker:test      # docker compose run --rm test
```

To run a single test: `go test -v -tags="unit" -run TestName ./internal/path/to/pkg/...`

## Environment

Copy `.env.example` to `.env` and fill in values. Config loads `.env.<APP_ENV>` then `.env` at startup via `godotenv`. `APP_ENV` defaults to `development`; `production` makes `GITHUB_TOKEN` required.

Required env vars: `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `SMTP_HOST`, `SMTP_USER`, `SMTP_PASS`. See `.env.example` for all options.

## Architecture

Two separate binaries are built and deployed:
- **`cmd/server/main.go`** — runs the Scanner and the HTTP server (two goroutines)
- **`cmd/mailer/main.go`** — runs the Mailer consumer (standalone service)

### HTTP layer (`internal/transport/http/`)
Gin router with a Prometheus middleware. `SubscriptionHandler` owns four routes:
- `POST /subscribe` — creates/resets a pending subscription, queues a verification email
- `GET /confirm/:token` — activates the subscription, returns `X-Api-Token` header
- `GET /unsubscribe/:token` — requires `Authorization: Bearer <api_token>` to unsubscribe
- `GET /subscriptions?email=...` — requires Bearer token, lists non-unsubscribed subscriptions

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
- **Notifier** (`notifier.go`): publishes typed `DeliveryMessage` events to the Redis stream

On startup, the scanner calls `store.RecoverStuckRepos()` to reset any repos left in `processing` state from a previous crash.

### Message queue (`internal/infra/mq/`, `internal/infra/redis/`)
Redis Streams are used as the MQ. `redis.Stream` is a generic JSON publisher/consumer. `mq.EmailMQ` wraps it with domain event types: `new_release`, `repo_moved`, `email_verification`.

### Mailer worker (`internal/worker/mailer/`)
Consumes from `messages:delivery` stream (consumer group `mailer_group`, N concurrent consumers). Dispatches to `internal/infra/smtp` based on event type.

### Database (`internal/infra/db/`)
GORM + PostgreSQL. Schema is auto-migrated on startup. Two models:
- `Repository`: tracks `GitHubID`, `Owner/Name`, `LastRelease` (embedded), `Status` (idle/processing), `LastScannedAt`
- `Subscription`: tracks `Email`, `RepositoryID`, `Status` (pending/active/unsubscribed), `ConfirmToken`, `APIToken`

`db.Get()` is a singleton initialized with `sync.Once`. Config is loaded via `config.LoadServerConfig()` (server) or `config.LoadMailerConfig()` (mailer) — each reads only the env vars its service needs.

## Testing

Tests use build tags. Unit tests are self-contained; integration tests use **testcontainers** (requires Docker) to spin up real Redis and PostgreSQL instances. Tag patterns:
- `//go:build unit`
- `//go:build integration`

## Documentation

Architecture docs and ADRs are in `docs/`:
- `docs/system_design.md` — high-level system design
- `docs/adr/` — Architecture Decision Records (ETags strategy, scanner design, Redis Streams for MQ)

## Linting

`golangci-lint` v2 with `golangci.yml`. Notable enabled linters: `gosec`, `errcheck`, `staticcheck`, `contextcheck`, `dupl`, `nestif`. `goimports` enforces that project-local imports (`github.com/GenesisEducationKyiv/...`) are grouped separately from stdlib and third-party.
