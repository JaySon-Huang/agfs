//go:build failpoint

package sqlqueue

import (
	"path/filepath"
	"testing"

	"github.com/pingcap/failpoint"
)

func newSQLiteTestBackend(t *testing.T, dbPath string) *Backend {
	t.Helper()

	backend := NewSQLiteBackend()
	if err := backend.Initialize(map[string]interface{}{
		"backend": "sqlite",
		"db_path": dbPath,
	}); err != nil {
		t.Fatalf("initialize sqlite sqlqueue backend: %v", err)
	}
	t.Cleanup(func() {
		_ = backend.Close()
	})
	return backend
}

func TestSQLiteRemoveQueuePartialFailureRegression(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuefs-remove-partial.db")
	backend := newSQLiteTestBackend(t, dbPath)

	for _, queueName := range []string{"jobs", "logs", "alerts"} {
		if err := backend.CreateQueue(queueName); err != nil {
			t.Fatalf("CreateQueue(%s): %v", queueName, err)
		}
	}

	failpointPath := "github.com/c4pt0r/agfs/agfs-server/pkg/plugins/queuefs/internal/sqlqueue/queuefsRemoveQueueDropError"
	if err := failpoint.Enable(failpointPath, "return(2)"); err != nil {
		t.Fatalf("enable failpoint: %v", err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable(failpointPath)
	})

	err := backend.RemoveQueue("")
	if err == nil {
		t.Fatalf("RemoveQueue unexpectedly succeeded after enabling %s; expected injected DROP TABLE failure", failpointPath)
	}

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
	if len(backend.tableCache) != 1 {
		t.Fatalf("cache entries after partial failure = %d, want 1", len(backend.tableCache))
	}
}
