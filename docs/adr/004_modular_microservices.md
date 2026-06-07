# ADR 004: Modular Architecture with Microservice Extraction

## Status
Accepted

## Context

The initial system was implemented as a single deployable unit where all concerns — subscription management, GitHub scanning, and email delivery — ran in one process. As the system matured, two architectural problems emerged:

1. **Deployment coupling**: Deploying a change to the email templates required a full restart of the scanner and HTTP server, causing unnecessary downtime.
2. **Lack of domain boundaries**: The HTTP handler layer held direct references to `*gorm.DB` and `*github.Client`, concrete infrastructure types. This made the domain logic hard to test in isolation and blurred responsibility between layers.

## Decision

Two changes were made together:

### 1. Extract the Mailer as a standalone microservice

The `mailer` worker was moved to its own binary (`cmd/mailer/main.go`). It communicates with the server exclusively through the existing Redis Streams message queue (`messages:delivery`). The server publishes events; the mailer consumes them.

This gives each service an independent deployment lifecycle, and makes the communication contract explicit — the stream schema is the only shared interface between them.

### 2. Introduce interface boundaries in the subscription handler

The `SubscriptionHandler` previously held `*gorm.DB` (direct GORM handle) and `*github.Client` (concrete type). Both were replaced with interfaces defined in the handler package:

- `SubscriptionRepository` — all data access the handler needs; implemented by a `gormStore` in `handlers/store.go`
- `RepoResolver` — resolves a GitHub repo by owner/name; satisfied structurally by `*github.Client` with no adapter needed

This matches the existing pattern already used in the scanner (`RepositoryStore` in `scanner/store.go`), making the interface-at-boundary approach consistent across all domain modules.

## Consequences

### Positive

- **Independent deployment**: Server and mailer can be built, deployed, and scaled independently.
- **Testability**: The handler can be unit-tested by substituting mock implementations of `SubscriptionRepository` and `RepoResolver` without a real database or GitHub connection.
- **Explicit contracts**: All cross-module communication is mediated by interfaces or the Redis stream schema — no concrete infrastructure type leaks across boundaries.
- **Consistency**: The handler now follows the same structural pattern as the scanner worker, making the codebase uniform.

### Negative

- **Operational overhead**: Two processes to deploy, monitor, and configure instead of one.
- **Distributed failure modes**: If the mailer is down, emails queue up in Redis Streams rather than failing fast. This is acceptable and handled by `XAUTOCLAIM`, but requires operators to monitor the consumer group lag.
