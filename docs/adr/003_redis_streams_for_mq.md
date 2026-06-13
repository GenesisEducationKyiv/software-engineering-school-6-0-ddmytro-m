# ADR 003: Use Redis Streams for Notification Message Queue

## Status
Accepted

Superseded by [ADR 005](005_rabbitmq_event_broker.md): Redis Streams is no longer used as the message queue — both events and commands now flow through RabbitMQ. Redis remains only for the GitHub response cache and consumer deduplication.

## Context

The system requires a mechanism to send notifications (e.g., for new software releases, repository moves) to users asynchronously and reliably. The core requirements for this mechanism are:

1.  **Asynchronicity**: The main application threads (API handlers, repository scanner) should not be blocked while waiting for emails to be sent.
2.  **Reliability**: Notifications must not be lost if a worker process crashes or the email service is temporarily unavailable.
3.  **Durability**: Messages should persist so they can be processed after a system restart.
4.  **Scalability**: The system should be able to handle a growing number of notifications by adding more consumer workers.

The existing architecture already includes Redis for caching GitHub API responses. We needed to choose a message queue (MQ) technology to build the notification engine. The candidates considered were:

-   A dedicated message broker (e.g., RabbitMQ, Apache Kafka).
-   Using the existing Redis instance.
-   An in-memory queue (rejected due to lack of durability).

## Decision

We will use **Redis Streams** as the backbone for our message queue (MQ) to handle all notification-related events.

The `mailer` worker will be implemented as a consumer group that processes messages from a stream.

## Consequences

### Positive

-   **No New Dependencies**: This is the most significant advantage. By leveraging the existing Redis instance, we avoid adding and managing a new piece of infrastructure (like a RabbitMQ or Kafka cluster). This simplifies our deployment, configuration, and operational overhead.
-   **Sufficient Feature Set**: Redis Streams provides all the necessary features for our use case out-of-the-box:
    -   **Persistence**: Messages in streams are persisted by Redis.
    -   **Consumer Groups**: Allows multiple `mailer` workers to consume messages from the same stream in parallel, enabling horizontal scaling.
    -   **Crash Recovery**: The `XAUTOCLAIM` command allows a new or restarted worker to claim messages that were being processed by a worker that crashed, preventing message loss. This is a key part of our fault-tolerance strategy (NFR3).
    -   **Dead-Letter Queue (DLQ)**: A DLQ can be easily implemented by having workers move unprocessable messages to a separate stream, as documented in the system design (NFR3).
-   **Simplicity**: The Redis Streams API is simpler to work with compared to the client libraries and concepts of more complex brokers like Kafka. This leads to faster development and easier maintenance.
-   **Performance**: Redis is renowned for its high performance, which is more than sufficient for the volume of notifications this system is expected to generate.

### Negative

-   **Not a Full-Fledged Broker**: Redis is not a dedicated message broker. It lacks advanced features found in systems like RabbitMQ (e.g., complex routing topologies, exchanges) or Kafka (e.g., a true distributed commit log for event sourcing at a massive scale). For the defined scope of this project, these features are not required.
-   **Observability**: While Redis has good monitoring capabilities, the tools for observing and managing message flows within streams may be less mature than those available for dedicated brokers. This is a manageable trade-off.

This decision aligns with our microservices architecture, allowing us to build a reliable, decoupled notification system where the mailer is an independently deployable service without significantly increasing the system's complexity.
