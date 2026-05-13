# System design

## 1. System Architecture

The system is designed as a **modular monolith** to meet the project requirements while maintaining a clear separation of concerns between the API, the scanning engine, and the notification system.

### Core Components
- **API Server (HTTP/gRPC)**: Handles user requests for subscriptions and verification. It secures sensitive endpoints using an API Authorization Token (X-API-TOKEN).
- **Scanner (Background Worker)**: An adaptive engine that identifies repositories due for a check and manages GitHub API quota.
- **Notifier/Mailer (Background Worker)**: Consumes events from a Redis-based Message Queue (MQ) to send emails via SMTP with retry logic.

### Storage & Infrastructure:
- **PostgreSQL/GORM**: Stores subscriptions, repository metadata (including ETags), and scan history.
- **Redis**: Acts as both a high-speed cache for GitHub API responses and the backbone for the reliable Message Queue (Redis Streams).

## 2. Functional Requirements

- **FR1: Subscription Management**: The system must provide an HTTP API for users to subscribe to public GitHub repositories.
- **FR2: New Release Detection**: The system must periodically scan subscribed repositories to check for new software releases.
- **FR3: Notification Delivery**: Upon detecting a new release (i.e., a new Git tag that differs from the last known one), the system must send an email notification to all active subscribers of that repository.
- **FR4: ETag-Based Conditional Requests**: The system must use GitHub's ETag mechanism for all relevant API calls. The ETag is stored and sent in subsequent requests using the `If-None-Match` header to avoid re-fetching unchanged data.
- **FR5: Data Persistence**: All core data, including repository metadata (GitHub ID, name), subscription details (user email, status), and the latest ETags for both repository info and releases, must be persisted in a PostgreSQL database.
- **FR6: Repository Relocation Handling**: The system must detect if a repository has been moved or renamed by comparing its unique GitHub ID against the stored value. If a mismatch occurs, it must notify subscribers and mark their subscriptions accordingly.
- **FR7: Crash Recovery**: On startup, the scanner must automatically identify and reset the status of any repositories that were in a "processing" state, ensuring they can be scanned again in the next cycle.
- **FR8: Selective Notifications**: Notifications are sent only to subscribers with an "active" status.

## 3. Non-Functional Requirements

The system is designed to meet the following non-functional requirements:

- **NFR1: Performance and Efficiency**
    - **API Quota Conservation**: The system must be highly efficient in its use of the GitHub API quota. This is primarily achieved through ETag-based conditional requests, which often do not count against the rate limit if the resource is unchanged.
    - **Response Caching**: API responses (both successful and error states like 404s) are cached in Redis with appropriate TTLs to minimize latency and redundant network calls.

- **NFR2: Scalability**
    - **Concurrent Processing**: The scanner uses a producer-consumer pattern with multiple concurrent workers to process a large number of repositories in parallel, ensuring the system can scale as the number of subscriptions grows.

- **NFR3: Reliability and Resilience**
    - **Stateful Recovery**: The scanner is designed to be self-healing. By resetting "processing" states on startup, it ensures that a crash does not leave repositories in an un-scannable state.
    - **Graceful Error Handling**: The system must gracefully handle network failures, API errors (e.g., `404 Not Found`), and unexpected response formats without crashing.
    - **Fault-Tolerant Notifications**: The notification engine uses a reliable message queue (Redis Streams) with auto-claim features to ensure that notifications are not lost if a mailer worker crashes.

- **NFR4: API Compliance**
    - **Adaptive Rate Limiting**: The scanner dynamically adjusts its request rate based on the `X-RateLimit-Remaining` and `X-RateLimit-Reset` headers from the GitHub API.
    - **Hibernation on Rate Limit**: If a `403 Forbidden` or `429 Too Many Requests` status is received, the scanner immediately "hibernates" (sets its rate to zero) until the rate limit window resets, preventing further API violations.
    - **Safety Buffer**: A configurable percentage of the API quota is reserved and not used by the scanner to ensure that user-facing operations (like new subscriptions) can always be served.

- **NFR5: Observability**
    - **Metrics Exposition**: The API server must expose key operational metrics (e.g., request latency, API rate limit status) in a Prometheus-compatible format via a `/metrics` endpoint for monitoring and alerting.

## 4. GitHub Scanner Logic

The scanner uses a Producer-Consumer pattern to handle high volumes of repositories efficiently without triggering GitHub's secondary rate limits.

### Adaptive Rate Limiting
To avoid 429 errors and secondary limit bans, the scanner dynamically calculates its Requests Per Second (RPS) before every batch:
- **Quota Assessment**: It fetches the current X-RateLimit-Remaining and X-RateLimit-Reset from GitHub.
- **Safety Buffer**: It reserves a portion of the quota (e.g., 20%) to ensure API capacity remains for new user-driven subscription requests.
- **RPS Calculation**: The usable limit is spread over the time remaining until the next reset.
- **Hibernation**: If a 403 or 429 status is received, the scanner "hibernates" by setting the RPS to 0 until the environment is safe.

### ETag Optimization (FR4, NFR1)
To minimize quota consumption, the system stores the ETag of every repository and release. By sending these in the If-None-Match header, the service can receive a 304 Not Modified response, which consumes significantly fewer API points and zero processing time for unchanged data.

## 5. Notification Engine

The notification system is designed to be fault-tolerant, ensuring that no release notification is lost even if a worker crashes or the SMTP server is temporarily unavailable.
- **Redis Streams MQ**: Notifications are published as events (e.g., EventNewRelease).
- **Exponential Backoff**: If an email fails to send, the worker retries the operation with increasing delays.
- **Crash Recovery (AutoClaim)**: On startup, the Mailer uses Redis's XAUTOCLAIM feature to find messages that were "pending" for a long time (e.g., due to a crashed worker) and reprocesses them.
- **Dead Letter Queue (DLQ)**: Messages that fail after maximum retries or have invalid formats are moved to a DLQ for manual inspection.
