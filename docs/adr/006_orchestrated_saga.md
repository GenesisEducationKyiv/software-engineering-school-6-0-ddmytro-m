# ADR 006: Orchestrated Saga for Cross-Service Subscription Onboarding

## Status
Accepted

Builds on [ADR 005](005_rabbitmq_event_broker.md): the saga rides the existing
RabbitMQ exchanges and event envelope rather than introducing new infrastructure.

> **Update:** the transactional outbox this ADR originally deferred (see "Out of
> scope" below) now exists (`internal/infra/outbox`). Saga-start has been
> rewritten to use it: see the amended "T2" step and the "Out of scope" section.

## Context

After ADR 005 the system is three services that share nothing but the broker and
its message contracts:

- **server** (`cmd/server`) — HTTP API + scanner. Owns the **PostgreSQL**
  database (`Repository`, `Subscription`) and publishes domain events to the
  `github_scanner.events` topic exchange.
- **notifier** (`cmd/notifier`) — consumes events, decides what notification
  each warrants, and publishes `email.send` commands to
  `github_scanner.commands`.
- **mailer** (`cmd/mailer`) — consumes commands and performs the side effect
  that lives outside our database: sending email over **SMTP**.

The **subscribe flow** is a distributed transaction: it must change Postgres
**and** guarantee an email outcome that is produced by another service against an
external system. Today (`SubscriptionHandler.Subscribe`) it does two
state-changing things that are not coordinated:

1. **server** writes a `Subscription` row with `status = pending` to Postgres.
2. **server** publishes `subscription.created`; the notifier turns it into an
   `email.send` command; the **mailer** must deliver the verification email so
   the user can confirm.

This is fire-and-forget — the handler publishes the event and immediately returns
`200 OK` with "Confirmation email sent." The failure modes:

- If delivery **permanently** fails (bad address, SMTP rejects, retries
  exhausted), the user is left with a `pending` row they can never confirm: a
  half-finished subscription with no path forward and no cleanup.
- The user is told the email was sent even when it was not.

A local `BEGIN … COMMIT` cannot help — it only protects the server's own rows,
not a side effect that happens two services away. **2PC is not viable either**:
SMTP is not transactional, AMQP is not an XA resource, and 2PC would re-couple
the services with synchronous locking, defeating ADR 004/005. The broker already
gives us at-least-once delivery, retries, and a DLQ, but those guarantee *the
message is eventually handled or parked* — they say nothing about reconciling the
**server's database** with the final delivery outcome. That reconciliation is the
gap a saga fills.

Options considered:

- **Two-phase commit (2PC)** — rejected: no transactional resources to enlist,
  reintroduces tight coupling.
- **Choreographed saga** — each service reacts to events with no central brain.
  Viable, but the workflow logic and state become emergent and hard to audit.
- **Orchestrated saga** — a single coordinator owns the workflow, persists its
  state, drives the forward action, and runs compensation on failure.

## Decision

We will model subscription onboarding as an **orchestrated saga**, with the
**server** as the orchestrator. The orchestrator persists saga state in Postgres
so it survives restarts, and it correlates everything by the **confirm token** —
a random, unique, non-PII value that already flows end to end (handler →
`subscription.created` event → `email.send` command → email), so no new
correlation id has to be threaded through the chain.

```
Handler (server)
│
├─ T1  one local tx, Postgres: create/save Subscription{status: pending},
│      start saga: persist OnboardingSaga{token, awaiting}, and queue the
│      subscription.created outbox event — commits or rolls back together
│
├─ T2  outbox relay asynchronously publishes subscription.created (events exchange)
│        → notifier emits email.send command                     (commands exchange)
│        → mailer sends the verification email via SMTP
│
└─ mailer reports the outcome as a domain event (events exchange), consumed by
   the orchestrator (server):
     • verification.delivered{token} → mark saga completed
     • verification.failed{token}    → C1: cancel the still-pending subscription,
                                        mark saga compensated
```

Concretely, what changes versus today:

- **Two result events** join the contract: `verification.delivered` and
  `verification.failed`, carried on the existing `github_scanner.events` exchange
  with the confirm token as payload (no PII).
- **The mailer reports the outcome of a verification command.** On success it
  publishes `verification.delivered`; when its in-process SMTP retries are
  exhausted it publishes `verification.failed`. For verification commands this
  failure is **terminal at the mailer** (it acks rather than handing back to the
  broker's tiered retry/DLQ) so the failure signal reaches the orchestrator
  instead of disappearing into a parked DLQ message. Non-verification commands
  (release, repo-moved) keep the ADR 005 broker-retry behaviour unchanged.
- **Starting onboarding replaces the handler's fire-and-forget publish.** The
  subscription write, the `OnboardingSaga` row, and the `subscription.created`
  outbox event all commit in the same local transaction (`handlers.
  SubscriptionRepository`), so there is no publish-failure case left to
  compensate for at saga-start — the outbox already guarantees the event
  survives once that transaction commits. The orchestrator itself owns none of
  this; it only settles results.
- **The orchestrator consumes the result events** on its own queue bound to the
  events exchange (`verification.delivered`, `verification.failed`), and settles
  the saga: complete on delivered, compensate on failed.

### Compensation (C1)

Compensation cancels the orphaned subscription by **soft-deleting the pending
row** for that confirm token. It is guarded so it only acts while the
subscription is still `pending` — if the user confirmed in the meantime (a race
against a late `verification.failed`), the active subscription is left untouched.
A user whose verification failed simply re-subscribes, which the handler already
supports for any non-active record.

### Compensation timeout (the reaper)

A saga has no automatic path forward if the thing that would settle it is
lost: the `subscription.created` event (before the outbox existed) or, still
today, a `verification.delivered`/`verification.failed` result (see "Out of
scope" below). Left alone, such a saga sits in `awaiting_delivery` forever,
and so does its `pending` subscription.

`orchestrator.Reaper` (`internal/worker/orchestrator/reaper.go`) closes that
gap with a periodic sweep, run alongside the orchestrator's consumer in
`cmd/server`: every `SAGA_REAPER_POLL_INTERVAL_SECONDS` (default 300s) it
finds sagas still `awaiting_delivery` after `SAGA_STALE_AFTER_SECONDS`
(default 1800s) and runs the same `Compensate` used for an explicit
`verification.failed` — cancel the still-pending subscription, mark the saga
compensated. `StaleAfter` is deliberately generous: it must comfortably
outlast the RabbitMQ retry/DLQ tiers (ADR 005) and the mailer's in-process
SMTP retries, so the reaper only ever acts on sagas that are genuinely stuck,
not ones merely running slow.

This introduces one accepted race: if a `verification.delivered` result is
merely delayed rather than lost, and arrives after the reaper has already
compensated the saga, `claim()` sees a terminal state and acks without
re-acting — the delayed "delivered" signal is dropped, and a subscription
that did in fact get its verification email ends up cancelled anyway. The
`StaleAfter` default is sized to make this rare; a user who hits it can
simply re-subscribe, the same recovery path as an ordinary compensation.

### Properties the implementation must guarantee

- **Idempotency.** Broker delivery is at-least-once, so result events may be
  redelivered. Settlement keys on the persisted saga state (terminal sagas are
  acked without re-acting) and compensation is naturally idempotent (soft-delete
  only a still-pending row) - which also makes it safe for the reaper and a
  late result event to race on the same token.
- **Semantic compensation.** C1 is not a physical rollback; it is a new local
  transaction that returns the data to a consistent state.
- **Commit point.** Once T1/T2 commit we are *in* the saga: it ends in
  `completed` or `compensated`, never a half-written state.

### Out of scope (deliberate)

- **Outbox coverage for the result events.** The transactional outbox now
  covers saga-*start* (see above), but `verification.delivered`/
  `verification.failed` are still published directly by the mailer, not
  through an outbox of its own. If a result event is lost the saga stays
  `awaiting_delivery` until the reaper (see above) compensates it on a
  timeout, rather than being reconciled immediately. Giving the mailer its
  own outbox remains future work.
- **Saga for release notifications.** The scanner has the same shape (advance
  `Repository.LastRelease` only once delivery is acknowledged, otherwise revert
  so the next scan retries). The subscribe flow is implemented first because it
  is smaller, user-visible, and has an unambiguous compensation.

## Consequences

### Positive

- **Consistency without a shared transaction.** A well-defined, eventually
  consistent outcome across Postgres and SMTP — no 2PC, no shared database,
  preserving the service autonomy of ADR 004/005.
- **No orphaned state.** Compensation guarantees the system lands in `completed`
  or `compensated`; no permanently-stuck `pending` subscriptions - including
  when the thing that would settle a saga is itself lost, thanks to the
  reaper's timeout-based compensation.
- **Loose coupling preserved.** Services still communicate only through broker
  contracts; the saga adds two result events on the existing events exchange, not
  a synchronous call, so every service keeps its independent lifecycle.
- **Reuses ADR 005 primitives.** The event envelope (with its dedup `id`), topic
  exchanges, publisher confirms, and consumer/retry machinery are all reused; the
  net additions are two event types, an `OnboardingSaga` table, and one
  orchestrator consumer.
- **Centralized, auditable workflow.** Saga state, ordering, and compensation
  live in one place and one table — easier to log and debug than an emergent
  choreographed flow, and it scales as more steps are added.

### Negative

- **Eventual, not immediate, consistency** — a window exists where the
  subscription row exists but the delivery outcome is unknown.
- **More moving parts** — a saga table, two result events, an orchestrator
  consumer, and a reaper sweep to maintain and monitor.
- **Timeout-based compensation can race a delayed (not lost) result.** See
  "Compensation timeout" above - a `StaleAfter` that's too short trades false
  positives (compensating a saga that would have completed on its own) for
  faster recovery from genuinely lost events.
- **Verification loses broker-tier retries.** To keep the failure signal
  observable, a verification send that exhausts the mailer's in-process retries
  fails terminally instead of using the broker's tiered retry/DLQ. This is an
  intentional trade of one retry mechanism for saga observability; re-subscribe
  is the recovery path.
- **Per-step compensation logic** must be designed and tested, including the
  race between a late `verification.failed` and a user confirmation.

This decision extends the messaging architecture of ADR 005 with a reliability
guarantee for the one operation that genuinely spans services, without
sacrificing their independent deployment lifecycle.
