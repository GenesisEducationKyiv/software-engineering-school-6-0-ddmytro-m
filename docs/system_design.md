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

## 2. GitHub Scanner Logic

The scanner uses a Producer-Consumer pattern to handle high volumes of repositories efficiently without triggering GitHub's secondary rate limits.

### Adaptive Rate Limiting
To avoid 429 errors and secondary limit bans, the scanner dynamically calculates its Requests Per Second (RPS) before every batch:
- **Quota Assessment**: It fetches the current X-RateLimit-Remaining and X-RateLimit-Reset from GitHub.
- **Safety Buffer**: It reserves a portion of the quota (e.g., 20%) to ensure API capacity remains for new user-driven subscription requests.
- **RPS Calculation**: The usable limit is spread over the time remaining until the next reset.
- **Hibernation**: If a 403 or 429 status is received, the scanner "hibernates" by setting the RPS to 0 until the environment is safe.

### ETag Optimization
To minimize quota consumption, the system stores the ETag of every repository and release. By sending these in the If-None-Match header, the service can receive a 304 Not Modified response, which consumes significantly fewer API points and zero processing time for unchanged data.

## 3. Notification Engine

The notification system is designed to be fault-tolerant, ensuring that no release notification is lost even if a worker crashes or the SMTP server is temporarily unavailable.
- **Redis Streams MQ**: Notifications are published as events (e.g., EventNewRelease).
- **Exponential Backoff**: If an email fails to send, the worker retries the operation with increasing delays.
- **Crash Recovery (AutoClaim)**: On startup, the Mailer uses Redis's XAUTOCLAIM feature to find messages that were "pending" for a long time (e.g., due to a crashed worker) and reprocesses them.
- **Dead Letter Queue (DLQ)**: Messages that fail after maximum retries or have invalid formats are moved to a DLQ for manual inspection.
