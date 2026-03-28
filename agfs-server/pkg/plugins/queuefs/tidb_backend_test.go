package queuefs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func tidbTestConfig(t *testing.T, database string) map[string]interface{} {
	t.Helper()

	dsn := os.Getenv("TIDB_TEST_DSN")
	if dsn == "" {
		t.Skip("set TIDB_TEST_DSN to run TiDB integration tests")
	}

	parsedDSN, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse TIDB_TEST_DSN: %v", err)
	}
	parsedDSN.DBName = database
	if parsedDSN.Params == nil {
		parsedDSN.Params = map[string]string{}
	}

	return map[string]interface{}{
		"backend":  "tidb",
		"dsn":      parsedDSN.FormatDSN(),
		"database": database,
	}
}

func newTiDBTestQueueFS(t *testing.T, database string) *queueFS {
	t.Helper()
	plugin := newTiDBTestPluginWithConfig(t, database, nil)

	fs, ok := plugin.GetFileSystem().(*queueFS)
	if !ok {
		t.Fatalf("unexpected filesystem type %T", plugin.GetFileSystem())
	}
	return fs
}

func newTiDBTestPluginWithConfig(t *testing.T, database string, extra map[string]interface{}) *QueueFSPlugin {
	t.Helper()

	cfg := tidbTestConfig(t, database)
	for key, value := range extra {
		cfg[key] = value
	}

	plugin := NewQueueFSPlugin()
	if err := plugin.Initialize(cfg); err != nil {
		t.Fatalf("initialize tidb queuefs: %v", err)
	}
	t.Cleanup(func() {
		if plugin.backend != nil {
			_ = plugin.backend.Close()
		}
	})
	return plugin
}

func newTiDBTestDatabaseName(t *testing.T) string {
	t.Helper()

	return fmt.Sprintf("queuefs_test_%d", time.Now().UnixNano())
}

func newTiDBNamedDatabaseName(t *testing.T, suffix string) string {
	t.Helper()

	return fmt.Sprintf("queuefs_test_%d_%s", time.Now().UnixNano(), suffix)
}

func TestQueueFSTiDBFileRegression(t *testing.T) {
	database := newTiDBTestDatabaseName(t)
	fs := newTiDBTestQueueFS(t, database)

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
		t.Fatal("expected removed nested tidb queue to disappear")
	}
}

func TestQueueFSTiDBPersistenceRegression(t *testing.T) {
	database := newTiDBTestDatabaseName(t)

	func() {
		fs := newTiDBTestQueueFS(t, database)
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

	fs := newTiDBTestQueueFS(t, database)

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

func TestQueueFSTiDBConfigUsesDSN(t *testing.T) {
	config := tidbTestConfig(t, newTiDBTestDatabaseName(t))
	if _, ok := config["dsn"].(string); !ok || config["dsn"] == "" {
		t.Fatalf("expected non-empty dsn in config: %+v", config)
	}
}

func TestQueueFSTiDBConfigDatabaseOverridesDSN(t *testing.T) {
	baseDSN := os.Getenv("TIDB_TEST_DSN")
	if baseDSN == "" {
		t.Skip("set TIDB_TEST_DSN to run TiDB integration tests")
	}

	parsedDSN, err := mysql.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse TIDB_TEST_DSN: %v", err)
	}
	if parsedDSN.Params == nil {
		parsedDSN.Params = map[string]string{}
	}

	dsnDatabase := newTiDBNamedDatabaseName(t, "dsn")
	targetDatabase := newTiDBNamedDatabaseName(t, "target")
	ensureTiDBDatabase(t, parsedDSN, dsnDatabase)
	ensureTiDBDatabase(t, parsedDSN, targetDatabase)

	dsnConfig := *parsedDSN
	dsnConfig.DBName = dsnDatabase
	dsnURL := dsnConfig.FormatDSN()

	plugin := NewQueueFSPlugin()
	if err := plugin.Initialize(map[string]interface{}{
		"backend":  "tidb",
		"dsn":      dsnURL,
		"database": targetDatabase,
	}); err != nil {
		t.Fatalf("initialize tidb queuefs with overridden database: %v", err)
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

	if got := tidbQueueRegistryCount(t, parsedDSN, targetDatabase); got != 1 {
		t.Fatalf("target database queue registry count = %d, want 1", got)
	}
	if got := tidbQueueRegistryCount(t, parsedDSN, dsnDatabase); got != 0 {
		t.Fatalf("dsn database queue registry count = %d, want 0", got)
	}
}

func ensureTiDBDatabase(t *testing.T, baseDSN *mysql.Config, database string) {
	t.Helper()

	adminDSN := *baseDSN
	adminDSN.DBName = ""
	db, err := sql.Open("mysql", adminDSN.FormatDSN())
	if err != nil {
		t.Fatalf("open TiDB admin connection: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database)); err != nil {
		t.Fatalf("create TiDB database %q: %v", database, err)
	}
}

func tidbQueueRegistryCount(t *testing.T, baseDSN *mysql.Config, database string) int {
	t.Helper()

	dsn := *baseDSN
	dsn.DBName = database
	db, err := sql.Open("mysql", dsn.FormatDSN())
	if err != nil {
		t.Fatalf("open TiDB database %q: %v", database, err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM queuefs_registry`).Scan(&count); err != nil {
		if strings.Contains(err.Error(), "doesn't exist") {
			return 0
		}
		t.Fatalf("count queuefs_registry rows in %q: %v", database, err)
	}
	return count
}

func TestQueueFSTiDBConcurrentDequeueRegression(t *testing.T) {
	database := newTiDBTestDatabaseName(t)
	writerFS := newTiDBTestQueueFS(t, database)
	readerOne := newTiDBTestQueueFS(t, database)
	readerTwo := newTiDBTestQueueFS(t, database)

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

func TestQueueFSTiDBDurableLifecycle(t *testing.T) {
	database := newTiDBTestDatabaseName(t)
	plugin := newTiDBTestPluginWithConfig(t, database, map[string]interface{}{"mode": queueModeDurable})
	fs, ok := plugin.GetFileSystem().(*queueFS)
	if !ok {
		t.Fatalf("unexpected filesystem type %T", plugin.GetFileSystem())
	}

	if err := fs.Mkdir("/jobs", 0o755); err != nil {
		t.Fatalf("mkdir /jobs: %v", err)
	}
	if _, err := fs.Write("/jobs/enqueue", []byte("tidb-durable"), -1, 0); err != nil {
		t.Fatalf("enqueue durable tidb message: %v", err)
	}

	claimedBytes := mustReadAll(t, fs, "/jobs/dequeue")
	var claimed ClaimedMessage
	if err := json.Unmarshal(claimedBytes, &claimed); err != nil {
		t.Fatalf("unmarshal durable tidb claim: %v (payload=%q)", err, string(claimedBytes))
	}
	if got := claimed.Data; got != "tidb-durable" {
		t.Fatalf("claimed durable tidb data = %q, want tidb-durable", got)
	}
	ackPayload := []byte(`{"message_id":"` + claimed.MessageID + `","receipt":"` + claimed.Receipt + `"}`)
	if _, err := fs.Write("/jobs/ack", ackPayload, -1, 0); err != nil {
		t.Fatalf("ack durable tidb message: %v", err)
	}
	if got := string(mustReadAll(t, fs, "/jobs/size")); got != "0" {
		t.Fatalf("durable tidb size after ack = %q, want 0", got)
	}

	durableBackend, ok := plugin.backend.(DurableQueueBackend)
	if !ok {
		t.Fatal("tidb backend should implement DurableQueueBackend")
	}
	if _, err := fs.Write("/jobs/enqueue", []byte("recover-tidb"), -1, 0); err != nil {
		t.Fatalf("enqueue recover tidb message: %v", err)
	}
	claim, found, err := durableBackend.Claim("jobs", ClaimRequest{LeaseDuration: time.Second})
	if err != nil {
		t.Fatalf("claim recover tidb message: %v", err)
	}
	if !found {
		t.Fatal("expected recover tidb message to be claimed")
	}
	if recovered, err := durableBackend.RecoverExpired("jobs", time.Now().UTC().Add(2*time.Second), 0); err != nil {
		t.Fatalf("recover durable tidb message: %v", err)
	} else if recovered != 1 {
		t.Fatalf("recovered durable tidb count = %d, want 1", recovered)
	}
	reclaimed, found, err := durableBackend.Claim("jobs", ClaimRequest{})
	if err != nil {
		t.Fatalf("reclaim durable tidb message: %v", err)
	}
	if !found || reclaimed.MessageID != claim.MessageID || reclaimed.Attempt != claim.Attempt+1 {
		t.Fatalf("unexpected reclaimed durable tidb message: found=%v claimed=%+v", found, reclaimed)
	}
	ackPayload = []byte(`{"message_id":"` + reclaimed.MessageID + `","receipt":"` + reclaimed.Receipt + `"}`)
	if _, err := fs.Write("/jobs/ack", ackPayload, -1, 0); err != nil {
		t.Fatalf("ack recovered durable tidb message: %v", err)
	}
	statsBytes := mustReadAll(t, fs, "/jobs/stats")
	var stats QueueStats
	if err := json.Unmarshal(statsBytes, &stats); err != nil {
		t.Fatalf("unmarshal durable tidb stats after recovered ack: %v (payload=%q)", err, string(statsBytes))
	}
	if stats.Pending != 0 || stats.Processing != 0 || stats.Recoveries != 1 {
		t.Fatalf("durable tidb stats after recovered ack = %+v, want pending=0 processing=0 recoveries=1", stats)
	}
}
