package queuefs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func pgTestConfig(t *testing.T, database string) map[string]interface{} {
	t.Helper()

	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("set PG_TEST_DSN to run PostgreSQL integration tests")
	}

	parsedURL, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse PG_TEST_DSN: %v", err)
	}
	parsedURL.Path = "/" + database
	query := parsedURL.Query()
	if query.Get("sslmode") == "" {
		query.Set("sslmode", "require")
	}
	parsedURL.RawQuery = query.Encode()

	adminURL := *parsedURL
	adminURL.Path = "/postgres"

	adminDSN := adminURL.String()
	password, _ := parsedURL.User.Password()
	user := parsedURL.User.Username()
	port := parsedURL.Port()
	host := parsedURL.Hostname()
	if port == "" {
		port = "5432"
	}

	return map[string]interface{}{
		"backend":   "pgsql",
		"dsn":       parsedURL.String(),
		"host":      host,
		"port":      port,
		"user":      user,
		"password":  password,
		"database":  database,
		"admin_dsn": adminDSN,
	}
}

func newPGTestQueueFS(t *testing.T, database string) *queueFS {
	t.Helper()

	plugin := NewQueueFSPlugin()
	if err := plugin.Initialize(pgTestConfig(t, database)); err != nil {
		t.Fatalf("initialize pgsql queuefs: %v", err)
	}
	t.Cleanup(func() {
		if plugin.backend != nil {
			_ = plugin.backend.Close()
		}
	})

	fs, ok := plugin.GetFileSystem().(*queueFS)
	if !ok {
		t.Fatalf("unexpected filesystem type %T", plugin.GetFileSystem())
	}
	return fs
}

func newPGTestDatabaseName() string {
	return fmt.Sprintf("queuefs_pg_test_%d", time.Now().UnixNano())
}

func newPGNamedDatabaseName(suffix string) string {
	return fmt.Sprintf("queuefs_pg_test_%d_%s", time.Now().UnixNano(), suffix)
}

func TestQueueFSPGSQLFileRegression(t *testing.T) {
	database := newPGTestDatabaseName()
	fs := newPGTestQueueFS(t, database)

	if err := fs.Mkdir("/jobs", 0o755); err != nil {
		t.Fatalf("mkdir /jobs: %v", err)
	}
	if err := fs.Mkdir("/logs/errors", 0o755); err != nil {
		t.Fatalf("mkdir /logs/errors: %v", err)
	}

	entries, err := fs.ReadDir("/")
	if err != nil {
		t.Fatalf("readdir root: %v", err)
	}
	rootEntries := queueDirEntryNames(entries)
	for _, name := range []string{"README", "jobs", "logs"} {
		if _, ok := rootEntries[name]; !ok {
			t.Fatalf("root missing %q in %+v", name, entries)
		}
	}

	if _, err := fs.Write("/jobs/enqueue", []byte("first"), -1, 0); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	if _, err := fs.Write("/jobs/enqueue", []byte("second"), -1, 0); err != nil {
		t.Fatalf("enqueue second: %v", err)
	}

	if got := string(mustReadAll(t, fs, "/jobs/size")); got != "2" {
		t.Fatalf("queue size = %q, want 2", got)
	}

	peeked := mustReadMessage(t, fs, "/jobs/peek")
	if peeked.Data != "first" {
		t.Fatalf("peeked message = %q, want first", peeked.Data)
	}

	first := mustReadMessage(t, fs, "/jobs/dequeue")
	second := mustReadMessage(t, fs, "/jobs/dequeue")
	if first.Data != "first" || second.Data != "second" {
		t.Fatalf("dequeue order = [%q, %q], want [first, second]", first.Data, second.Data)
	}

	if got := string(mustReadAll(t, fs, "/jobs/dequeue")); got != "{}" {
		t.Fatalf("empty dequeue = %q, want {}", got)
	}

	if _, err := fs.Write("/jobs/enqueue", []byte("to-clear"), -1, 0); err != nil {
		t.Fatalf("enqueue before clear: %v", err)
	}
	if _, err := fs.Write("/jobs/clear", nil, -1, 0); err != nil {
		t.Fatalf("clear queue: %v", err)
	}
	if got := string(mustReadAll(t, fs, "/jobs/size")); got != "0" {
		t.Fatalf("queue size after clear = %q, want 0", got)
	}

	if err := fs.RemoveAll("/logs"); err != nil {
		t.Fatalf("removeall /logs: %v", err)
	}
	if _, err := fs.Stat("/logs/errors"); err == nil {
		t.Fatal("expected removed nested pgsql queue to disappear")
	}
}

func TestQueueFSPGSQLPersistenceRegression(t *testing.T) {
	database := newPGTestDatabaseName()

	func() {
		fs := newPGTestQueueFS(t, database)
		if err := fs.Mkdir("/jobs", 0o755); err != nil {
			t.Fatalf("mkdir /jobs: %v", err)
		}
		if _, err := fs.Write("/jobs/enqueue", []byte("persisted"), -1, 0); err != nil {
			t.Fatalf("enqueue persisted message: %v", err)
		}
		if got := string(mustReadAll(t, fs, "/jobs/size")); got != "1" {
			t.Fatalf("initial queue size = %q, want 1", got)
		}
	}()

	fs := newPGTestQueueFS(t, database)

	entries, err := fs.ReadDir("/")
	if err != nil {
		t.Fatalf("readdir root after reopen: %v", err)
	}
	if _, ok := queueDirEntryNames(entries)["jobs"]; !ok {
		t.Fatalf("root missing reopened queue in %+v", entries)
	}

	if got := string(mustReadAll(t, fs, "/jobs/size")); got != "1" {
		t.Fatalf("reopened queue size = %q, want 1", got)
	}
	peeked := mustReadMessage(t, fs, "/jobs/peek")
	if peeked.Data != "persisted" {
		t.Fatalf("peek after reopen = %q, want persisted", peeked.Data)
	}

	dequeued := mustReadMessage(t, fs, "/jobs/dequeue")
	if dequeued.Data != "persisted" {
		t.Fatalf("dequeue after reopen = %q, want persisted", dequeued.Data)
	}
	if got := string(mustReadAll(t, fs, "/jobs/size")); got != "0" {
		t.Fatalf("queue size after reopened dequeue = %q, want 0", got)
	}

	if _, err := fs.Stat("/jobs"); err != nil {
		t.Fatalf("stat empty queue after reopen: %v", err)
	}
}

func TestQueueFSPGSQLConfigUsesDSN(t *testing.T) {
	config := pgTestConfig(t, newPGTestDatabaseName())
	if _, ok := config["dsn"].(string); !ok || config["dsn"] == "" {
		t.Fatalf("expected non-empty dsn in config: %+v", config)
	}
}

func TestQueueFSPGSQLConfigDatabaseOverridesDSN(t *testing.T) {
	baseDSN := os.Getenv("PG_TEST_DSN")
	if baseDSN == "" {
		t.Skip("set PG_TEST_DSN to run PostgreSQL integration tests")
	}

	baseURL, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatalf("parse PG_TEST_DSN: %v", err)
	}
	query := baseURL.Query()
	if query.Get("sslmode") == "" {
		query.Set("sslmode", "require")
	}
	baseURL.RawQuery = query.Encode()

	dsnDatabase := newPGNamedDatabaseName("dsn")
	targetDatabase := newPGNamedDatabaseName("target")

	adminURL := *baseURL
	adminURL.Path = "/postgres"
	adminDSN := adminURL.String()
	ensurePGDatabase(t, adminDSN, dsnDatabase)
	ensurePGDatabase(t, adminDSN, targetDatabase)

	dsnURL := *baseURL
	dsnURL.Path = "/" + dsnDatabase
	targetURL := *baseURL
	targetURL.Path = "/" + targetDatabase

	password, _ := baseURL.User.Password()
	port := baseURL.Port()
	if port == "" {
		port = "5432"
	}

	plugin := NewQueueFSPlugin()
	if err := plugin.Initialize(map[string]interface{}{
		"backend":   "pgsql",
		"dsn":       dsnURL.String(),
		"host":      baseURL.Hostname(),
		"port":      port,
		"user":      baseURL.User.Username(),
		"password":  password,
		"database":  targetDatabase,
		"admin_dsn": adminDSN,
	}); err != nil {
		t.Fatalf("initialize pgsql queuefs with overridden database: %v", err)
	}
	t.Cleanup(func() {
		if plugin.backend != nil {
			_ = plugin.backend.Close()
		}
	})

	fs, ok := plugin.GetFileSystem().(*queueFS)
	if !ok {
		t.Fatalf("unexpected filesystem type %T", plugin.GetFileSystem())
	}
	if err := fs.Mkdir("/jobs", 0o755); err != nil {
		t.Fatalf("mkdir /jobs: %v", err)
	}

	if got := pgQueueRegistryCount(t, targetURL.String()); got != 1 {
		t.Fatalf("target database queue registry count = %d, want 1", got)
	}
	if got := pgQueueRegistryCount(t, dsnURL.String()); got != 0 {
		t.Fatalf("dsn database queue registry count = %d, want 0", got)
	}
}

func ensurePGDatabase(t *testing.T, adminDSN string, database string) {
	t.Helper()

	db, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatalf("open admin database: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE %q", database)); err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("create database %q: %v", database, err)
	}
}

func pgQueueRegistryCount(t *testing.T, dsn string) int {
	t.Helper()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open database %q: %v", dsn, err)
	}
	defer db.Close()

	var exists bool
	if err := db.QueryRow(`SELECT EXISTS (
		SELECT 1
		FROM information_schema.tables
		WHERE table_schema = current_schema()
		  AND table_name = 'queuefs_registry'
	)`).Scan(&exists); err != nil {
		t.Fatalf("check queuefs_registry table in %q: %v", dsn, err)
	}
	if !exists {
		return 0
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM queuefs_registry`).Scan(&count); err != nil {
		t.Fatalf("count queuefs_registry rows in %q: %v", dsn, err)
	}
	return count
}

func TestQueueFSPGSQLConcurrentDequeueRegression(t *testing.T) {
	database := newPGTestDatabaseName()
	writerFS := newPGTestQueueFS(t, database)
	readerOne := newPGTestQueueFS(t, database)
	readerTwo := newPGTestQueueFS(t, database)

	if err := writerFS.Mkdir("/jobs", 0o755); err != nil {
		t.Fatalf("mkdir /jobs: %v", err)
	}
	if _, err := writerFS.Write("/jobs/enqueue", []byte("once"), -1, 0); err != nil {
		t.Fatalf("enqueue once: %v", err)
	}

	type dequeueResult struct {
		payload []byte
		err     error
	}

	start := make(chan struct{})
	results := make(chan dequeueResult, 2)
	var wg sync.WaitGroup
	for _, fs := range []*queueFS{readerOne, readerTwo} {
		wg.Add(1)
		go func(fs *queueFS) {
			defer wg.Done()
			<-start
			payload, err := fs.Read("/jobs/dequeue", 0, -1)
			results <- dequeueResult{payload: payload, err: err}
		}(fs)
	}
	close(start)
	wg.Wait()
	close(results)

	nonEmpty := 0
	for result := range results {
		if result.err != nil && !errors.Is(result.err, io.EOF) {
			t.Fatalf("concurrent dequeue: %v", result.err)
		}
		if string(result.payload) == "{}" {
			continue
		}

		var msg QueueMessage
		if err := json.Unmarshal(result.payload, &msg); err != nil {
			t.Fatalf("unmarshal concurrent dequeue payload: %v (payload=%q)", err, string(result.payload))
		}
		if msg.Data != "once" {
			t.Fatalf("concurrent dequeue returned %q, want once", msg.Data)
		}
		nonEmpty++
	}

	if nonEmpty != 1 {
		t.Fatalf("concurrent dequeue claimed %d messages, want 1", nonEmpty)
	}
	if got := string(mustReadAll(t, writerFS, "/jobs/size")); got != "0" {
		t.Fatalf("queue size after concurrent dequeue = %q, want 0", got)
	}
}
