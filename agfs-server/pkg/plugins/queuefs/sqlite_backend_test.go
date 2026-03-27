package queuefs

import (
	"path/filepath"
	"testing"

	"github.com/pingcap/failpoint"
)

func newSQLiteTestPlugin(t *testing.T, dbPath string) *QueueFSPlugin {
	t.Helper()

	plugin := NewQueueFSPlugin()
	if err := plugin.Initialize(map[string]interface{}{
		"backend": "sqlite",
		"db_path": dbPath,
	}); err != nil {
		t.Fatalf("initialize sqlite queuefs: %v", err)
	}
	t.Cleanup(func() {
		if plugin.backend != nil {
			_ = plugin.backend.Close()
		}
	})
	return plugin
}

func newSQLiteTestQueueFS(t *testing.T, dbPath string) *queueFS {
	t.Helper()

	plugin := newSQLiteTestPlugin(t, dbPath)

	fs, ok := plugin.GetFileSystem().(*queueFS)
	if !ok {
		t.Fatalf("unexpected filesystem type %T", plugin.GetFileSystem())
	}
	return fs
}

func TestQueueFSSQLiteFileRegression(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuefs.db")
	fs := newSQLiteTestQueueFS(t, dbPath)

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
		t.Fatal("expected removed nested sqlite queue to disappear")
	}
}

func TestQueueFSSQLitePersistenceRegression(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuefs-persist.db")

	func() {
		fs := newSQLiteTestQueueFS(t, dbPath)
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

	fs := newSQLiteTestQueueFS(t, dbPath)

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

func TestQueueFSSQLiteRemoveQueuePartialFailureRegression(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuefs-remove-partial.db")
	plugin := newSQLiteTestPlugin(t, dbPath)
	fs, ok := plugin.GetFileSystem().(*queueFS)
	if !ok {
		t.Fatalf("unexpected filesystem type %T", plugin.GetFileSystem())
	}
	backend, ok := plugin.backend.(*SQLQueueBackend)
	if !ok {
		t.Fatalf("unexpected backend type %T", plugin.backend)
	}

	for _, path := range []string{"/jobs", "/logs", "/alerts"} {
		if err := fs.Mkdir(path, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
	}

	// Inject a DROP TABLE failure on the second removal attempt so the test can
	// verify partial-success handling deterministically without relying on backend-
	// specific locking or permission behavior.
	failpointPath := "github.com/c4pt0r/agfs/agfs-server/pkg/plugins/queuefs/queuefsRemoveQueueDropError"
	if err := failpoint.Enable(failpointPath, "return(2)"); err != nil {
		if err == failpoint.ErrNotExist {
			t.Skip("run this test with failpoint-ctl enable on pkg/plugins/queuefs")
		}
		t.Fatalf("enable failpoint: %v", err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable(failpointPath)
	})

	err := backend.RemoveQueue("")
	if err == nil {
		t.Skip("failpoint was not instrumented; run this test after failpoint-ctl enable")
	}

	// Successful drops should be removed from both registry and cache, while the
	// failed drop remains visible so queuefs_registry stays aligned with surviving
	// physical tables.
	remaining := 0
	for _, name := range []string{"jobs", "logs", "alerts"} {
		exists, existsErr := backend.QueueExists(name)
		if existsErr != nil {
			t.Fatalf("QueueExists(%s): %v", name, existsErr)
		}
		if exists {
			remaining++
			if _, ok := backend.tableCache[name]; !ok {
				t.Fatalf("expected cache to retain failed queue %q", name)
			}
			continue
		}
		if _, ok := backend.tableCache[name]; ok {
			t.Fatalf("expected cache entry for removed queue %q to be cleared", name)
		}
	}
	if remaining != 1 {
		t.Fatalf("remaining queues = %d, want 1", remaining)
	}

	queues, listErr := backend.ListQueues("")
	if listErr != nil {
		t.Fatalf("ListQueues: %v", listErr)
	}
	if len(queues) != 1 {
		t.Fatalf("visible queues after partial failure = %v, want 1 remaining queue", queues)
	}
}
