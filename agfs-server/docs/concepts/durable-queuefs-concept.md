---
summary: "DurableQueueFS: a queue-only durable filesystem contract with claim, ack, release, and manual recovery"
read_when:
  - You want to understand what DurableQueueFS is and what it is not
  - You are implementing QueueFS durable mode backends
  - You are integrating a separate TaskManagement layer on top of QueueFS
title: "DurableQueueFS"
---

# DurableQueueFS

`DurableQueueFS` is the durable-mode queue concept for AGFS `queuefs`.

It turns QueueFS from a simple "dequeue means delete" FIFO filesystem into a
durable queue substrate with explicit claim, acknowledgement, release, and
manual recovery semantics.

`DurableQueueFS` is intentionally a **queue-only** abstraction. It is not a
task registry, not a task query API, and not a full async job platform.

## Quick start

Enable durable mode in QueueFS config:

```json5
{
  plugins: {
    entries: {
      queuefs: {
        enabled: true,
        backend: "sqlite", // or pgsql / tidb
        mode: "durable",
      },
    },
  },
}
```

In durable mode, the queue directory is expected to expose these control files:

```text
/queue/<name>/
  enqueue
  dequeue
  ack
  recover
  size
  stats
  clear
```

Core semantics:

- `enqueue` writes a work item into the queue.
- `dequeue` claims a pending item instead of deleting it.
- `ack` confirms successful completion of a claimed item.
- `recover` manually requeues expired claimed items.
- `size` reports pending-only queue depth.
- `stats` reports queue-level state counts.

## What it is

`DurableQueueFS` is a durable queue substrate with these responsibilities:

- persist queue items across process restarts
- let workers claim one item at a time
- require explicit acknowledgement for successful completion
- support release of an uncompleted claim back to `pending`
- recover expired claims manually
- expose queue-level status via `size` and `stats`

The queue-level state model is deliberately small:

- `pending`
- `processing`

The first phase also tracks a queue-level recovery counter:

- `recoveries`

## What it is not

`DurableQueueFS` does **not** own task-management semantics.

It does not provide:

- `task_id` query APIs
- task listing or filtering
- task deduplication such as "no running task for this resource"
- task result storage
- task error history
- `/tasks` HTTP endpoints
- observer, wait-complete, or event-stream APIs

Those belong to a separate `TaskManagement` layer.

## How it works

In durable mode, QueueFS participates in four queue lifecycle points:

1. **Enqueue** — a producer writes a new queue item.
2. **Claim** — a worker reads `dequeue` and receives one claimed item plus a
   standardized `receipt`.
3. **Finish** — the worker either:
   - writes `ack` to confirm success, or
   - performs `release` semantics to return the item to `pending`.
4. **Recover** — an operator or runtime explicitly triggers `recover` to move
   expired `processing` items back to `pending`.

### Claim lifecycle

The durable claim lifecycle is:

```text
pending
  -> claim via dequeue
  -> processing
  -> ack      -> removed from active queue state
  -> release  -> pending
  -> expire   -> recover -> pending
```

The important difference from legacy FIFO mode is that `dequeue` no longer
means deletion. It means exclusive claim with a lease.

### Receipt

Every successful claim returns a standardized `receipt`.

`receipt` exists so that:

- late acknowledgements cannot confirm an old claim instance
- release applies only to the current claim holder
- backends share one portable contract across SQLite, PostgreSQL, and TiDB

`message_id` alone is not sufficient for safe durable acknowledgement.

## Durable vs FIFO mode

QueueFS has two explicit modes:

- `fifo` — the legacy queue behavior
- `durable` — the durable queue behavior described here

In `fifo` mode:

- `dequeue` consumes and removes the item immediately
- no `ack` or `recover` contract is required

In `durable` mode:

- `dequeue` is a claim operation
- `ack` is required for successful completion
- `recover` restores expired claims
- `size` means pending-only

Durable mode must not silently rewrite FIFO behavior. The two contracts are
separate and must be tested independently.

## The DurableQueueBackend interface

The backend concept behind durable mode is a queue-only extension interface:

```go
type DurableQueueBackend interface {
    QueueBackend

    Claim(queueName string, req ClaimRequest) (ClaimedMessage, bool, error)
    Ack(queueName string, messageID string, receipt string) error
    Release(queueName string, messageID string, req ReleaseRequest) error
    RecoverExpired(queueName string, now time.Time, limit int) (int, error)
    Stats(queueName string) (QueueStats, error)
}
```

Companion structures:

```go
type ClaimRequest struct {
    WorkerID      string
    LeaseDuration time.Duration
}

type ClaimedMessage struct {
    MessageID   string
    QueueName   string
    Data        []byte
    Receipt     string
    ClaimedAt   time.Time
    LeaseUntil  time.Time
    Attempt     int
}

type ReleaseRequest struct {
    Receipt string
    RetryAt *time.Time
    Reason  string
}

type QueueStats struct {
    Pending    int
    Processing int
    Recoveries int
}
```

### First-phase decisions

The current concept fixes these first-phase decisions:

- `Release` always returns the item to `pending`
- `receipt` is required and standardized
- `stats` contains `pending`, `processing`, and `recoveries`
- `recover` is manual-only in phase one

## Relationship to TaskManagement

`TaskManagement` is a separate concept layered above `DurableQueueFS`.

`TaskManagement` may use `DurableQueueFS` for scheduling and delivery, but it
owns higher-level concerns such as:

- task identity
- task query APIs
- deduplication by task type and resource
- result and error persistence
- HTTP `/tasks` endpoints

This separation keeps QueueFS small and backend-focused while allowing a later
task system to evolve independently.

## Configuration reference

Durable mode is selected through QueueFS config:

```json5
{
  plugins: {
    entries: {
      queuefs: {
        backend: "sqlite", // or pgsql / tidb / memory for non-durable testing
        mode: "durable",
      },
    },
  },
}
```

If a backend does not implement the durable contract, QueueFS should fail
initialization in `durable` mode instead of silently falling back to `fifo`.

## Tips

- Treat `dequeue` as `claim`, not delete, when reading durable-mode code.
- Keep `size` semantics narrow: pending-only.
- Use `stats` for queue-level visibility, not item-level inspection.
- Do not put task-query behavior into QueueFS just because task systems may sit
  on top of it later.
- Validate the same durable contract on SQLite, PostgreSQL, and TiDB.
