# QueueFS Plugin - Message Queue Service

This plugin provides a message queue service through a file system interface.

## Files
```bash
/enqueue  # Write-only file to enqueue messages
/dequeue  # Read-only file to dequeue messages
/peek     # Read-only file to peek at next message
/size     # Read-only file showing queue size
/clear    # Write-only file to clear all messages
/README   # This file
```

## Dynamic Mounting With AGFS Shell

Interactive shell:
```bash  
agfs:/> mount queuefs /queue
agfs:/> mount queuefs /tasks
agfs:/> mount queuefs /messages
```
Direct command:
```bash
uv run agfs mount queuefs /queue
uv run agfs mount queuefs /jobs
```

## Configuration Parameters

None required - QueueFS works with default settings

Optional settings:

- `mode = "fifo"` keeps the original `enqueue/dequeue/peek/size/clear` contract.
- `mode = "durable"` switches `dequeue` to claim/lease semantics and exposes `ack`, `recover`, and `stats` instead of `peek`.

Durable mode is supported by the in-memory, SQLite, PostgreSQL, and TiDB backends.

## Usage
Enqueue a message:
```bash
echo "your message" > /enqueue
```

Dequeue a message:
```bash    
cat /dequeue
```

Peek at next message (without removing):
```bash    
cat /peek
```

Get queue size:
```bash    
cat /size
```

Clear the queue:
```bash
echo "" > /clear
```

Durable claim/ack workflow:

```bash
# Enable durable mode in plugin config first.
cat /dequeue
{"message_id":"...","queue_name":"jobs","data":"...","receipt":"...","claimed_at":"...","lease_until":"...","attempt":1}

echo '{"message_id":"...","receipt":"..."}' > /ack
cat /stats
{"pending":0,"processing":0,"recoveries":0}

# Recover expired claims manually when needed.
echo '{"limit":100}' > /recover
```

## Example
```bash
# Enqueue a message  
agfs:/> echo "task-123" > /queuefs/enqueue

# Check queue size
agfs:/> cat /queuefs/size
1
# Dequeue a message
agfs:/> cat /queuefs/dequeue
{"id":"...","data":"task-123","timestamp":"..."}
```

## Running Tests

From `agfs/agfs-server`, run the full `queuefs` test suite with:

```bash
go test ./pkg/plugins/queuefs/...
```

This covers the default in-memory tests plus the local SQLite-backed regression tests.
TiDB and PostgreSQL integration tests are gated separately and only run when explicitly enabled.

The supported integration-test entrypoints are:

```bash
make integration-test-pg
make integration-test-tidb
```

## Running SQLite Tests

`queuefs` includes regression tests that exercise the real SQLite-backed implementation.

To run only the SQLite-specific regression tests and bypass the Go test cache:

```bash
go test ./pkg/plugins/queuefs -run 'TestQueueFSSQLite' -count=1 -v
```

These tests create temporary `.db` files with `t.TempDir()` and initialize the plugin with:

```go
map[string]interface{}{
    "backend": "sqlite",
    "db_path": dbPath,
}
```

The current SQLite regression coverage lives in `agfs-server/pkg/plugins/queuefs/sqlite_backend_test.go`.

## Running TiDB Tests

`queuefs` also includes gated integration tests for the TiDB backend.

For a local playground started with:

```bash
tiup playground v8.5.5 --tiflash 0 --without-monitor
```

From `agfs/agfs-server`, run:

```bash
TIDB_TEST_DSN='root@tcp(127.0.0.1:4000)/queuedb?charset=utf8mb4&parseTime=True' \
make integration-test-tidb
```

Notes:

- `TIDB_TEST_DSN` is required; the tests replace the database name in that DSN with a fresh per-test database.
- The tests create a fresh database name for each run, so they do not reuse prior queue tables.
- The current TiDB regression coverage lives in `agfs-server/pkg/plugins/queuefs/tidb_backend_test.go`.

## Running PostgreSQL Tests

`queuefs` also includes gated integration tests for the PostgreSQL backend.

The default PostgreSQL service is available at `127.0.0.1:5432`.

From `agfs/agfs-server`, run:

```bash
PG_TEST_DSN="postgresql://${USER}@127.0.0.1:5432/postgres?sslmode=disable" \
make integration-test-pg
```

Notes:

- `PG_TEST_DSN` is required; the tests replace the database name in that DSN with a fresh per-test database.
- The tests create a fresh database name for each run, so they do not reuse prior queue tables.
- The current PostgreSQL regression coverage lives in `agfs-server/pkg/plugins/queuefs/pgsql_backend_test.go`.

## License

Apache License 2.0
