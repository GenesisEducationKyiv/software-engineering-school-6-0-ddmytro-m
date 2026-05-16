# ADR 001: Efficient API Consumption via HTTP Conditional Requests (ETags)

## Status
Accepted

## Context
The GitHub API imposes strict rate limits (typically 5,000 requests per hour for authenticated users). For a repository monitoring service, frequent polling is necessary to ensure timely notifications. Without optimization, every check—even if the repository has not changed—consumes one full point of the rate limit.

GitHub supports ETags, which allow clients to make conditional requests. If the resource hasn't changed, the server returns a 304 Not Modified status code, which:

- **Saves Quota**: Only counts as a fraction of a rate limit point (or sometimes zero, depending on the specific endpoint and GitHub's current policy).
- **Reduces Bandwidth**: Returns an empty body, saving egress costs and processing time.
- **Ensures Consistency**: Provides a reliable way to detect updates without downloading large JSON payloads.

## Decision
We will implement ETag-based conditional polling as a core feature of the `github.Client`.

### 1. ETag Storage and Propagation
The `github.Client` will accept an `etag` parameter in its primary data-fetching methods (`GetRepository`, `GetLatestRelease`).
- If an ETag is provided, the client must include the `If-None-Match` header in the outgoing HTTP request.
- The response wrapper (`Response[T]`) will capture the ETag header from the GitHub response and return it to the caller for persistence.

### 2. Successful Status Code Handling
The client's internal `get` function and response handlers must explicitly distinguish between:
- **200 OK**: Full payload received; update the stored ETag and process data.
- **304 Not Modified**: Resource is unchanged; skip decoding and notify the caller that existing local data is still valid.

### 3. Integration with Local Persistence
While the client handles the HTTP transport of ETags, the Scanner (the consumer) is responsible for:
- Storing the ETag in the database alongside the repository record.
- Retrieving and passing this ETag back to the client during the next scan cycle.

### 4. Synergy with Redis Caching
The client implements a two-tier optimization strategy:
- **In-Memory/Redis Cache**: If the data is within the `cacheTTL`, the client returns the cached object without a network call.
- **ETag Revalidation**: If the cache is expired but we have an ETag, we perform a network call with `If-None-Match`. If we get a 304, we refresh the cache TTL without needing to fetch the full body.

## Consequences

### Pros
- **Significant Quota Savings**: Repositories that update infrequently (the majority of cases) consume minimal API "budget."
- **Improved Latency**: 304 responses are faster to transmit and require zero JSON parsing.
- **Native Revalidation**: Provides a standard-compliant way to keep local data in sync with GitHub's state.

### Cons
- **Database Schema Complexity**: Requires additional columns in the database to store ETag strings for both repositories and releases.
- **State Management**: The caller must correctly manage the mapping between a resource and its ETag to avoid "mismatch" errors.