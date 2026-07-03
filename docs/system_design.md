# System design

## 1. System Architecture

The system is designed as **three independent microservices** with clearly demarcated domain boundaries. Services never call each other directly; all cross-service communication is asynchronous messaging over two channels with distinct semantics:

- **RabbitMQ topic exchange `github_scanner.events`** — carries **domain events** (facts, past tense: `release.detected`, `repository.moved`, `subscription.created`), published by the Server and fanned out by routing key to any interested consumer.
- **RabbitMQ topic exchange `github_scanner.commands`** — carries **commands** (imperatives: "send this email"), a point-to-point work queue produced by the Notifier and consumed by the Mailer. The Notifier publishes to the exchange (routing key `email.send`), never to the queue, so it does not know its consumers.

Redis is no longer part of the messaging path — it serves only the GitHub response cache and the Notifier's deduplication keys.

See [ADR 005](adr/005_rabbitmq_event_broker.md) for the events-vs-commands rationale.

### Microservices

| Service | Binary | Responsibility |
|---------|--------|----------------|
| **Server** | `cmd/server/main.go` | Subscription HTTP API + GitHub repository scanner; publishes domain events |
| **Notifier** | `cmd/notifier/main.go` | Notification policy: consumes domain events and emits email commands (both via RabbitMQ) |
| **Mailer** | `cmd/mailer/main.go` | Email delivery consumer (RabbitMQ commands → SMTP) |

### Core Components (Server)
- **API Server**: Handles user requests for subscriptions and verification. It secures sensitive endpoints using an API Authorization Token (X-API-TOKEN). The `SubscriptionHandler` depends on `SubscriptionRepository` and `RepoResolver` interfaces — no direct GORM or GitHub client coupling.
- **Scanner (Background Worker)**: An adaptive engine that identifies repositories due for a check and manages GitHub API quota.

### Core Component (Notifier)
- **Notification Service (Event Consumer)**: The single home of notification policy. Consumes domain events from the RabbitMQ `notifications` queue, decides what notification each event warrants, and publishes `DeliveryMessage` commands to the `github_scanner.commands` exchange. Deduplicates events by envelope ID (at-least-once delivery implies duplicates), acks only after the command is durably published, and routes poison/exhausted messages to a dead-letter queue.

### Core Component (Mailer)
- **Mailer (Background Worker)**: Consumes email commands from the RabbitMQ `email.delivery` queue and sends emails via SMTP, with in-process retry plus broker-level retry/DLQ.

### Storage & Infrastructure
- **PostgreSQL/GORM**: Stores subscriptions, repository metadata (including ETags), and scan history.
- **Redis**: A high-speed cache for GitHub API responses and the store for the Notifier's deduplication keys. (No longer a message queue.)
- **RabbitMQ**: Two durable topic exchanges (events, commands) with publisher confirms, tiered exponential-backoff retry queues + dead-letter queues per consumer, and a management UI for queue observability.

## 2. Modular Boundaries

Each module exposes its behaviour through interfaces, not concrete types. Cross-cutting concerns are never shared by direct struct reference.

| Module | Interface | Implemented by |
|--------|-----------|----------------|
| `handlers` | `SubscriptionRepository` | `gormSubscriptionStore` in `handlers/store.go` |
| `handlers` | `RepoResolver` | `*github.Client` (structural — no wrapper needed) |
| `handlers` | `EmailSender` | RabbitMQ-backed event publisher (`infra/rabbitmq`) |
| `worker/scanner` | `RepositoryStore` | `gormStore` in `scanner/store.go` |
| `worker/scanner` | `RepoProcessor` | `*processor` |
| `worker/scanner` | `Notifier` | RabbitMQ-backed event publisher (`infra/rabbitmq`) |
| `worker/notifier` | `CommandPublisher`, `DedupStore` | RabbitMQ command publisher + Redis `Dedup` |
| `worker/mailer` | `EmailSender` | `*smtp.Client` (consumes the RabbitMQ command queue) |

## 3. Functional Requirements

- **FR1: Subscription Management**: The system must provide an HTTP API for users to subscribe to public GitHub repositories.
- **FR2: New Release Detection**: The system must periodically scan subscribed repositories to check for new software releases.
- **FR3: Notification Delivery**: Upon detecting a new release (i.e., a new Git tag that differs from the last known one), the system must send an email notification to all active subscribers of that repository.
- **FR4: ETag-Based Conditional Requests**: The system must use GitHub's ETag mechanism for all relevant API calls. The ETag is stored and sent in subsequent requests using the `If-None-Match` header to avoid re-fetching unchanged data.
- **FR5: Data Persistence**: All core data, including repository metadata (GitHub ID, name), subscription details (user email, status), and the latest ETags for both repository info and releases, must be persisted in a PostgreSQL database.
- **FR6: Repository Relocation Handling**: The system must detect if a repository has been moved or renamed by comparing its unique GitHub ID against the stored value. If a mismatch occurs, it must notify subscribers and mark their subscriptions accordingly.
- **FR7: Crash Recovery**: On startup, the scanner must automatically identify and reset the status of any repositories that were in a "processing" state, ensuring they can be scanned again in the next cycle.
- **FR8: Selective Notifications**: Notifications are sent only to subscribers with an "active" status.
- **FR9: Domain Event Publication**: The system must publish domain events (`release.detected`, `repository.moved`, `subscription.created`) to the message broker as facts, wrapped in a versioned envelope with a unique ID. The decision of what notification an event warrants belongs exclusively to the Notification service.

## 4. Non-Functional Requirements

The system is designed to meet the following non-functional requirements:

- **NFR1: Performance and Efficiency**
    - **API Quota Conservation**: The system must be highly efficient in its use of the GitHub API quota. This is primarily achieved through ETag-based conditional requests, which often do not count against the rate limit if the resource is unchanged.
    - **Response Caching**: API responses (both successful and error states like 404s) are cached in Redis with appropriate TTLs to minimize latency and redundant network calls.

- **NFR2: Scalability**
    - **Concurrent Processing**: The scanner uses a producer-consumer pattern with multiple concurrent workers to process a large number of repositories in parallel, ensuring the system can scale as the number of subscriptions grows.

- **NFR3: Reliability and Resilience**
    - **Stateful Recovery**: The scanner is designed to be self-healing. By resetting "processing" states on startup, it ensures that a crash does not leave repositories in an un-scannable state.
    - **Graceful Error Handling**: The system must gracefully handle network failures, API errors (e.g., `404 Not Found`), and unexpected response formats without crashing.
    - **Fault-Tolerant Notifications**: The notification engine uses durable RabbitMQ messaging end to end — durable exchanges/queues, persistent messages, publisher confirms, manual acks, and per-consumer tiered-retry + dead-letter queues — so notifications are not lost if any worker crashes.
    - **At-Least-Once with Idempotency**: Both exchanges deliver at-least-once; consumers must tolerate redelivery. The Notification service deduplicates events by envelope ID before emitting commands, preventing duplicate emails.
    - **Process Auto-Restart**: All services run under container restart policies (`restart: unless-stopped`) and reconnect to brokers with capped exponential backoff, so transient infrastructure failures heal without operator action.

- **NFR4: API Compliance**
    - **Adaptive Rate Limiting**: The scanner dynamically adjusts its request rate based on the `X-RateLimit-Remaining` and `X-RateLimit-Reset` headers from the GitHub API.
    - **Hibernation on Rate Limit**: If a `403 Forbidden` or `429 Too Many Requests` status is received, the scanner immediately "hibernates" (sets its rate to zero) until the rate limit window resets, preventing further API violations.
    - **Safety Buffer**: A configurable percentage of the API quota is reserved and not used by the scanner to ensure that user-facing operations (like new subscriptions) can always be served.

- **NFR5: Observability**
    - **Metrics Exposition**: The API server must expose key operational metrics (e.g., request latency, API rate limit status) in a Prometheus-compatible format via a `/metrics` endpoint for monitoring and alerting.

## 5. GitHub Scanner Logic

The scanner uses a Producer-Consumer pattern to handle high volumes of repositories efficiently without triggering GitHub's secondary rate limits.

### Adaptive Rate Limiting
To avoid 429 errors and secondary limit bans, the scanner dynamically calculates its Requests Per Second (RPS) before every batch:
- **Quota Assessment**: It fetches the current X-RateLimit-Remaining and X-RateLimit-Reset from GitHub.
- **Safety Buffer**: It reserves a portion of the quota (e.g., 20%) to ensure API capacity remains for new user-driven subscription requests.
- **RPS Calculation**: The usable limit is spread over the time remaining until the next reset.
- **Hibernation**: If a 403 or 429 status is received, the scanner "hibernates" by setting the RPS to 0 until the environment is safe.

### ETag Optimization (FR4, NFR1)
To minimize quota consumption, the system stores the ETag of every repository and release. By sending these in the If-None-Match header, the service can receive a 304 Not Modified response, which consumes significantly fewer API points and zero processing time for unchanged data.

## 6. Notification Engine

The notification pipeline has three stages — **detect facts → decide what to notify → deliver** — each owned by a different service and connected by durable messaging. No stage can lose a notification even if a worker crashes or a downstream dependency (broker, SMTP) is temporarily unavailable.

### Stage 1: Event publication (Server)

- The scanner and HTTP API publish domain events to the durable RabbitMQ topic exchange `github_scanner.events`, routing key = event type.
- Events are wrapped in a versioned envelope `{id, type, occurred_at, version, payload}`.
- **Publisher Confirms**: a publish only succeeds once the broker has durably accepted the message; on nack/timeout the caller retries. A lost `release.detected` is additionally self-healing — the repository is rescanned next interval.

### Stage 2: Notification policy (Notifier)

- Consumes the durable `notifications` queue with manual acks and a prefetch limit (backpressure).
- **Idempotency**: deduplicates on envelope `id` (Redis `SETNX` + TTL) before acting — at-least-once delivery means redeliveries are expected, and users must not receive duplicate emails.
- **Ack Ordering**: an event is acked only after the resulting `DeliveryMessage` command is durably published to the commands exchange; a crash in between causes broker redelivery, never loss. If the dedup key was set but the publish fails, the key is rolled back so the retry is not skipped.
- **Retry Topology**: transient failures republish the event to the wait queue for its attempt tier (`notifications.retry.<i>`, per-queue TTL `base * factor^i`) which dead-letters it back to the main queue after the delay; the attempt count rides in an `x-attempts` header. After the configured number of tiers the event lands in `notifications.dlq`.
- **Poison Messages**: malformed payloads and unknown event types go straight to the DLQ — never requeued.

### Stage 3: Email delivery (Mailer)

- Consumes the `email.delivery` queue (bound to `github_scanner.commands`) with manual acks and prefetch.
- **In-process Exponential Backoff**: failed SMTP sends are first retried in-process with increasing delays.
- **Broker Retry/DLQ**: once in-process retries are exhausted the command is handed back to the broker's tiered retry queues; after the tiers are exhausted it lands in `email.delivery.dlq`. Invalid/unknown commands are dead-lettered immediately.
- **Crash Recovery**: a consumer crash leaves the in-flight command unacked, so RabbitMQ redelivers it to another consumer — no manual reclaim needed.
