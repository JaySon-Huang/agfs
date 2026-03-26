package queuefs

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestMemoryBackendTaskLifecycle(t *testing.T) {
	backend := NewMemoryBackend()
	if err := backend.Initialize(map[string]interface{}{
		"mode":          "task",
		"lease_seconds": 30,
		"max_attempts":  3,
	}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if err := backend.CreateQueue("jobs"); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	task := QueueTask{
		ID:          "task-1",
		Data:        newRawStringPayload([]byte("hello")),
		Timestamp:   time.Now().UTC(),
		MaxAttempts: 3,
	}
	if err := backend.Enqueue("jobs", task); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	size, err := backend.Size("jobs")
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}
	if size != 1 {
		t.Fatalf("Size = %d, want 1", size)
	}

	peeked, found, err := backend.Peek("jobs")
	if err != nil {
		t.Fatalf("Peek failed: %v", err)
	}
	if !found || peeked.ID != task.ID {
		t.Fatalf("Peek = %#v, found=%v", peeked, found)
	}

	dequeued, found, err := backend.Dequeue("jobs", "worker-a", 30*time.Second)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if !found {
		t.Fatal("Dequeue returned no task")
	}
	if dequeued.ID != task.ID {
		t.Fatalf("Dequeued ID = %s, want %s", dequeued.ID, task.ID)
	}
	if dequeued.Attempt != 1 {
		t.Fatalf("Attempt = %d, want 1", dequeued.Attempt)
	}
	if dequeued.LeaseUntil == nil || !dequeued.LeaseUntil.After(time.Now().UTC()) {
		t.Fatalf("LeaseUntil = %v, want future time", dequeued.LeaseUntil)
	}

	stats, err := backend.Stats("jobs")
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.Processing != 1 || stats.Queued != 0 {
		t.Fatalf("Stats after dequeue = %+v", stats)
	}

	if err := backend.Ack("jobs", task.ID); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}

	stats, err = backend.Stats("jobs")
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.Succeeded != 1 || stats.Processing != 0 {
		t.Fatalf("Stats after ack = %+v", stats)
	}
}

func TestMemoryBackendRetryDelayRecoveryAndDeadLetter(t *testing.T) {
	backend := NewMemoryBackend()
	if err := backend.Initialize(map[string]interface{}{
		"mode":               "task",
		"lease_seconds":      5,
		"max_attempts":       2,
		"enable_dead_letter": true,
	}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if err := backend.CreateQueue("jobs"); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	delayedRetryTask := QueueTask{
		ID:          "delayed-retry-task",
		Data:        newRawStringPayload([]byte("retry")),
		Timestamp:   time.Now().UTC(),
		MaxAttempts: 2,
	}
	if err := backend.Enqueue("jobs", delayedRetryTask); err != nil {
		t.Fatalf("Enqueue retry task failed: %v", err)
	}

	leased, found, err := backend.Dequeue("jobs", "worker-a", 5*time.Second)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if !found {
		t.Fatal("Dequeue returned no task")
	}

	if err := backend.Nack("jobs", leased.ID, "retry later", true, time.Hour); err != nil {
		t.Fatalf("Nack retry failed: %v", err)
	}

	size, err := backend.Size("jobs")
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}
	if size != 0 {
		t.Fatalf("Size after delayed nack = %d, want 0", size)
	}

	if _, found, err := backend.Peek("jobs"); err != nil {
		t.Fatalf("Peek failed: %v", err)
	} else if found {
		t.Fatal("Peek unexpectedly returned delayed retry task")
	}

	deadLetterTask := QueueTask{
		ID:          "dead-letter-task",
		Data:        newRawStringPayload([]byte("dead-letter")),
		Timestamp:   time.Now().UTC(),
		MaxAttempts: 2,
	}
	if err := backend.Enqueue("jobs", deadLetterTask); err != nil {
		t.Fatalf("Enqueue dead-letter task failed: %v", err)
	}

	staleTask := QueueTask{
		ID:          "stale-task",
		Data:        newRawStringPayload([]byte("stale")),
		Timestamp:   time.Now().UTC(),
		MaxAttempts: 3,
	}
	leased, found, err = backend.Dequeue("jobs", "worker-b", 5*time.Second)
	if err != nil {
		t.Fatalf("Dequeue stale task failed: %v", err)
	}
	if !found || leased.ID != deadLetterTask.ID {
		t.Fatalf("Dequeued dead-letter candidate = %#v, found=%v", leased, found)
	}
	if err := backend.Nack("jobs", leased.ID, "retry now", true, 0); err != nil {
		t.Fatalf("Immediate retry nack failed: %v", err)
	}

	if err := backend.Enqueue("jobs", staleTask); err != nil {
		t.Fatalf("Enqueue stale task failed: %v", err)
	}

	leased, found, err = backend.Dequeue("jobs", "worker-b", 5*time.Second)
	if err != nil {
		t.Fatalf("Dequeue stale task failed: %v", err)
	}
	if !found || leased.ID != deadLetterTask.ID {
		t.Fatalf("Dequeued retry task = %#v, found=%v", leased, found)
	}
	if leased.Attempt != 2 {
		t.Fatalf("Retry attempt = %d, want 2", leased.Attempt)
	}
	if err := backend.Nack("jobs", leased.ID, "exhausted", true, 0); err != nil {
		t.Fatalf("Dead-letter nack failed: %v", err)
	}

	leased, found, err = backend.Dequeue("jobs", "worker-b", 5*time.Second)
	if err != nil {
		t.Fatalf("Dequeue stale task failed: %v", err)
	}
	if !found || leased.ID != staleTask.ID {
		t.Fatalf("Dequeued stale candidate = %#v, found=%v", leased, found)
	}

	recovered, err := backend.RecoverStale("jobs", leased.LeaseUntil.Add(time.Second))
	if err != nil {
		t.Fatalf("RecoverStale failed: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("RecoverStale = %d, want 1", recovered)
	}

	requeued, found, err := backend.Dequeue("jobs", "worker-b", 5*time.Second)
	if err != nil {
		t.Fatalf("Dequeue recovered task failed: %v", err)
	}
	if !found || requeued.ID != staleTask.ID || requeued.Attempt != 2 {
		t.Fatalf("Recovered dequeue = %#v, found=%v", requeued, found)
	}
	if err := backend.Ack("jobs", requeued.ID); err != nil {
		t.Fatalf("Ack recovered task failed: %v", err)
	}

	stats, err := backend.Stats("jobs")
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.DeadLettered != 1 || stats.Succeeded != 1 {
		t.Fatalf("Stats = %+v, want 1 dead-lettered and 1 succeeded", stats)
	}
}

func TestSQLBackendSQLiteLifecycle(t *testing.T) {
	backend := NewSQLBackend()
	dbPath := filepath.Join(t.TempDir(), "queuefs.db")
	if err := backend.Initialize(map[string]interface{}{
		"backend":       "sqlite",
		"db_path":       dbPath,
		"mode":          "task",
		"lease_seconds": 30,
		"max_attempts":  3,
	}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	defer backend.Close()

	if err := backend.CreateQueue("jobs"); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	task := QueueTask{
		ID:          "sql-task",
		Data:        newRawStringPayload([]byte("payload")),
		Timestamp:   time.Now().UTC(),
		MaxAttempts: 3,
	}
	if err := backend.Enqueue("jobs", task); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	dequeued, found, err := backend.Dequeue("jobs", "worker-sql", 30*time.Second)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if !found || dequeued.ID != task.ID || dequeued.Attempt != 1 {
		t.Fatalf("Dequeued = %#v, found=%v", dequeued, found)
	}

	if err := backend.Nack("jobs", dequeued.ID, "retry now", true, 0); err != nil {
		t.Fatalf("Nack failed: %v", err)
	}

	dequeued, found, err = backend.Dequeue("jobs", "worker-sql", 30*time.Second)
	if err != nil {
		t.Fatalf("Second dequeue failed: %v", err)
	}
	if !found || dequeued.Attempt != 2 {
		t.Fatalf("Second dequeue = %#v, found=%v", dequeued, found)
	}

	if err := backend.Ack("jobs", dequeued.ID); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}

	stats, err := backend.Stats("jobs")
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.Succeeded != 1 {
		t.Fatalf("Stats = %+v, want succeeded=1", stats)
	}
}

func TestSQLBackendMigratesLegacySchema(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}

	statements := []string{
		`CREATE TABLE queuefs_registry (
			queue_name TEXT PRIMARY KEY,
			table_name TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE queuefs_queue_jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			message_id TEXT NOT NULL,
			data TEXT NOT NULL,
			timestamp INTEGER NOT NULL,
			created_at INTEGER DEFAULT (strftime('%s', 'now')),
			deleted INTEGER DEFAULT 0,
			deleted_at DATETIME NULL
		)`,
		`INSERT INTO queuefs_registry (queue_name, table_name) VALUES ('jobs', 'queuefs_queue_jobs')`,
	}
	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("bootstrap exec failed: %v", err)
		}
	}

	legacyQueued, _ := json.Marshal(struct {
		ID        string    `json:"id"`
		Data      string    `json:"data"`
		Timestamp time.Time `json:"timestamp"`
	}{
		ID:        "legacy-queued",
		Data:      "queued payload",
		Timestamp: time.Unix(1710000000, 0).UTC(),
	})
	legacySucceeded, _ := json.Marshal(struct {
		ID        string    `json:"id"`
		Data      string    `json:"data"`
		Timestamp time.Time `json:"timestamp"`
	}{
		ID:        "legacy-succeeded",
		Data:      "done payload",
		Timestamp: time.Unix(1710000010, 0).UTC(),
	})

	if _, err := db.Exec(
		`INSERT INTO queuefs_queue_jobs (message_id, data, timestamp, deleted) VALUES (?, ?, ?, 0), (?, ?, ?, 1)`,
		"legacy-queued", legacyQueued, int64(1710000000),
		"legacy-succeeded", legacySucceeded, int64(1710000010),
	); err != nil {
		t.Fatalf("legacy insert failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close failed: %v", err)
	}

	backend := NewSQLBackend()
	if err := backend.Initialize(map[string]interface{}{
		"backend":       "sqlite",
		"db_path":       dbPath,
		"mode":          "task",
		"lease_seconds": 30,
		"max_attempts":  4,
	}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	defer backend.Close()

	stats, err := backend.Stats("jobs")
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.Queued != 1 || stats.Succeeded != 1 {
		t.Fatalf("Stats after migration = %+v", stats)
	}

	task, found, err := backend.Dequeue("jobs", "worker-migrate", 30*time.Second)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if !found || task.ID != "legacy-queued" {
		t.Fatalf("Dequeued = %#v, found=%v", task, found)
	}
}
