# ADR 002: Adaptive Rate-Limited Repository Scanner

## Status
Accepted

## Context
We need a background process to monitor GitHub repositories for new releases. However, there are few challenges:

1. **GitHub API Rate Limits**: Primary limits (5,000 requests/hour for authenticated users) and secondary limits (concurrency) can lead to temporary bans.
2. **Scalability**: As the number of tracked repositories grows, a sequential loop becomes too slow.
3. **Reliability**: If the scanner crashes, repositories currently being processed might get stuck in a non-scannable state.
4. **Efficiency**: Re-downloading data for repositories that haven't changed wastes API quota.

## Decision
Implement a **Producer-Consumer** architecture with **Dynamic Rate Limiting**, and **Stateful Recovery**.

### Producer-Consumer Pattern
The scanner is split into a single producer and multiple concurrent workers.
- **Producer**: batch searches for the oldest scanned "idle" repositories. Batch size is calculated based on current API health. IDs of the repositories are pushed into a buffered channel.
- **Workers**: Configurable pool of workers that pull from the channel, execute the HTTP requests, and handle the notification logic.

### Dynamic Rate Limiting

Instead of a fixed interval, the scanner dynamically adjusts its speed:
- **API-Awareness**: It fetches GitHub's X-RateLimit-Remaining and X-RateLimit-Reset headers to calculate a safe Requests Per Second (RPS).
- **Safety Buffer**: A percentage (e.g., 20%) of the quota is reserved to prevent total exhaustion.
- **Secondary Limit Protection**: A hard ceiling (10 RPS) is enforced to avoid triggering GitHub's anti-abuse mechanisms.
- **Hibernation**: If a 403 (Forbidden) or 429 (Too Many Requests) is encountered, the rate.Limiter is frozen (RPS = 0) until the environment is safe.

### Resilience and Atomicity

- **Recovery Logic**: On startup, the scanner runs a recover() function to reset any repositories stuck in the processing state (likely due to a previous crash).
- **Transactional Updates**: We use a DB transaction to "claim" a batch of repositories, moving them from idle to processing atomically to ensure no two worker instances process the same repo.
- **Stale Data Handling**: If response from the server and recorded repo ids mismatch, users are notified that repo has been moved (and another repo took its place). Premature notifications are omitted to prevent notifications about visibility changes (and following unsubscriptions) 

## Consequences

### Pros
- **High Throughput**: Concurrent workers allow us to process thousands of repositories quickly.
- **API Compliance**: Dynamic calculation minimizes the risk of being blocked by GitHub.
- **Low Overhead**: ETags significantly reduce the data processing load.
- **Crash Recovery**: The system is self-healing after a restart.

### Cons
- **Complexity**: The state machine (idle -> processing -> idle) requires careful database management.
- **Channel Blocking**: If workers are too slow or the notifier service (e.g., email) hangs, the producer may block, though the buffered channel mitigates this.