# Architecture diagrams

Companion to [`system_design.md`](system_design.md) (which has the prose FR/NFR
spec) and the [ADRs](adr/timeline.md) (which have the decision history). This
file only draws pictures; it does not re-argue any decision.

## 1. Components

Three independently deployable services share nothing but PostgreSQL (server
only), Redis (cache + dedup), and RabbitMQ (the only inter-service channel).

```mermaid
flowchart LR
    client(["Client"])

    subgraph Server["cmd/server"]
        api["API Server"]
        scanner["Scanner"]
        relay["Outbox Relay"]
        orch["Saga Orchestrator + Reaper"]
    end

    subgraph Notifier["cmd/notifier"]
        notif["Notification policy"]
    end

    subgraph Mailer["cmd/mailer"]
        mailer["Mailer"]
    end

    pg[("PostgreSQL\nsubscriptions, repos,\noutbox_rows, sagas")]
    redis[("Redis\nGitHub cache + dedup keys")]
    gh(["GitHub API"])
    smtp(["SMTP"])

    evX{{"RabbitMQ\ngithub_scanner.events"}}
    cmdX{{"RabbitMQ\ngithub_scanner.commands"}}

    client -->|HTTP| api
    api --> pg
    scanner --> gh
    scanner --> redis
    scanner --> pg
    api --> pg

    relay -- polls --> pg
    relay -->|publish| evX

    evX -->|notifications queue| notif
    notif --> redis
    notif -->|publish email.send| cmdX

    cmdX -->|email.delivery queue| mailer
    mailer --> smtp
    mailer -.->|verification.delivered / .failed| evX

    evX -->|onboarding.saga queue| orch
    orch --> pg
```

Notes:
- The **Outbox Relay** and **Saga Orchestrator** run as goroutines inside
  `cmd/server`, not separate binaries — see
  [ADR 006](adr/006_orchestrated_saga.md).
- The `notifier → mailer` hop can alternatively run over gRPC instead of the
  commands exchange (`NOTIFIER_DELIVERY_TRANSPORT=grpc`) — see
  [ADR 007](adr/007_grpc_mailer_transport.md). The diagram shows the default
  RabbitMQ path.

## 2. Layered dependency direction

Enforced by `internal/archtest` (`make test:unit`), not just documented. A
package may import its own layer or anything strictly below; never above.
Worker services are siblings, not a hierarchy — none of them may import
another (they only talk through the broker, per
[ADR 004](adr/004_modular_microservices.md)/[005](adr/005_rabbitmq_event_broker.md)).

```mermaid
flowchart TB
    subgraph L5["cmd (composition root)"]
        direction LR
        cs["cmd/server"]
        cn["cmd/notifier"]
        cm["cmd/mailer"]
    end

    subgraph L4["transport"]
        direction LR
        th["transport/http\n+ handlers"]
        tg["transport/grpc/mailer"]
    end

    subgraph L3["worker (independent bounded contexts)"]
        direction LR
        wsc["worker/scanner"]
        wor["worker/orchestrator"]
        wno["worker/notifier"]
        wma["worker/mailer"]
    end

    subgraph L2["infra"]
        direction LR
        idb[("infra/db")]
        iob["infra/outbox"]
        irmq["infra/rabbitmq"]
        ired[("infra/redis")]
        ismtp["infra/smtp"]
        igh["api/github"]
    end

    subgraph L1["shared"]
        direction LR
        cfg["config"]
        evt["events"]
        log["logger"]
        met["metrics"]
        utl["utils"]
    end

    L5 --> L4 --> L3 --> L2 --> L1
    L5 -.-> L3
    L5 -.-> L2
    L4 -.-> L2
```

`internal/archtest/layers_test.go` parses every non-test `.go` file's imports
and fails the build if any package imports something ranked above it, or if
one `internal/worker/<x>` package imports another.

## 3. Release notification flow

```mermaid
sequenceDiagram
    participant Scanner
    participant GitHub
    participant DB as PostgreSQL
    participant Relay as Outbox Relay
    participant Events as events exchange
    participant Notifier
    participant Commands as commands exchange
    participant Mailer
    participant SMTP

    Scanner->>GitHub: GET repo (If-None-Match: repo ETag)
    GitHub-->>Scanner: 200 / 304 / 404 / 403,429
    Note right of Scanner: 403/429 freezes quota, skips repo this cycle

    alt GitHub repo ID != stored GitHubID (moved/renamed)
        Scanner->>DB: tx: unsubscribe active subscribers + insert repository.moved outbox event(s)
    else identity confirmed (ID matches, or 304)
        Scanner->>GitHub: GET latest release (If-None-Match: release ETag)
        GitHub-->>Scanner: 200 / 304 / 404

        alt release tag changed
            Scanner->>DB: tx: update Repository.LastRelease + insert release.detected outbox event(s)
        else unchanged
            Scanner->>DB: tx: update scan status only (no outbox events)
        end
    end

    Relay->>DB: poll outbox_rows
    Relay->>Events: publish release.detected / repository.moved
    Events->>Notifier: notifications queue
    Notifier->>Notifier: dedup by envelope id (Redis SET NX)
    Notifier->>Commands: publish email.send
    Commands->>Mailer: email.delivery queue
    Mailer->>SMTP: send email
    Mailer-->>Commands: ack (or retry/DLQ on failure)
```

## 4. Subscription onboarding saga

```mermaid
sequenceDiagram
    participant Client
    participant API as API Server
    participant DB as PostgreSQL
    participant Relay as Outbox Relay
    participant Events as events exchange
    participant Notifier
    participant Commands as commands exchange
    participant Mailer
    participant SMTP
    participant Orch as Saga Orchestrator

    Client->>API: POST /subscribe
    API->>DB: tx: Subscription{pending} + OnboardingSaga{awaiting_delivery} + outbox event
    API-->>Client: 200 OK
    Relay->>DB: poll outbox_rows
    Relay->>Events: publish subscription.created
    Events->>Notifier: notifications queue
    Notifier->>Commands: publish email.send (verification)
    Commands->>Mailer: email.delivery queue
    Mailer->>SMTP: send verification email
    alt delivered
        Mailer->>Events: publish verification.delivered
        Events->>Orch: onboarding.saga queue
        Orch->>DB: mark saga completed
    else SMTP retries exhausted
        Mailer->>Events: publish verification.failed
        Events->>Orch: onboarding.saga queue
        Orch->>DB: compensate — soft-delete pending subscription, mark saga compensated
    end

    Note over Orch,DB: Reaper sweeps every SAGA_REAPER_POLL_INTERVAL_SECONDS,<br/>compensates any saga still awaiting_delivery past SAGA_STALE_AFTER_SECONDS<br/>(covers a lost start or result event)
```
