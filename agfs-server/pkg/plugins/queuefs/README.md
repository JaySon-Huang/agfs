# QueueFS Plugin - Durable Task Queue

QueueFS exposes queues as directories inside AGFS.

In `task` mode, `dequeue` leases work instead of deleting it permanently. Workers
must `ack` on durable success or `nack` on failure. Expired leases can be
recovered through `recover`.

## Files

Base files on every queue:

```bash
/enqueue  # Write-only: enqueue a task
/dequeue  # Read-only: lease the next task
/peek     # Read-only: inspect the next task without leasing it
/size     # Read-only: count immediately consumable tasks
/clear    # Write-only: remove all tasks
/README   # Plugin documentation
```

Additional files in `task` mode:

```bash
/stats    # Read-only: JSON counts by task state
/ack      # Write-only: {"id":"task-id"}
/nack     # Write-only: {"id":"task-id","error":"...","retry":true,"retry_after_seconds":30}
/recover  # Write-only: requeue expired processing tasks
```

## Modes

```toml
mode = "task"    # durable lease/ack/nack/recover semantics
mode = "message" # legacy dequeue-implies-complete behavior
```

## Configuration

```toml
[plugins.queuefs]
enabled = true
path = "/queuefs"

  [plugins.queuefs.config]
  backend = "tidb"          # memory | sqlite | tidb | mysql
  mode = "task"
  lease_seconds = 300
  max_attempts = 16
  enable_dead_letter = true
  consumer_id = ""
```

SQLite example:

```toml
[plugins.queuefs]
enabled = true
path = "/queuefs"

  [plugins.queuefs.config]
  backend = "sqlite"
  db_path = "queuefs.db"
  mode = "task"
```

## Usage

Create a queue:

```bash
mkdir /queuefs/jobs
```

Enqueue a raw string payload:

```bash
echo "hello" > /queuefs/jobs/enqueue
```

Enqueue a structured task envelope:

```bash
echo '{"data":"hello","task_type":"summary","resource_id":"doc-1","resource_version":"v1"}' > /queuefs/jobs/enqueue
```

Lease and ack:

```bash
cat /queuefs/jobs/dequeue
echo '{"id":"<task-id>"}' > /queuefs/jobs/ack
```

Retry with delay:

```bash
echo '{"id":"<task-id>","error":"temporary failure","retry":true,"retry_after_seconds":30}' > /queuefs/jobs/nack
```

Recover expired leases:

```bash
echo "" > /queuefs/jobs/recover
```
