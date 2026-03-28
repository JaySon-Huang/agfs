package memory

import (
	"fmt"
	"sync"
	"testing"
	"time"

	model "github.com/c4pt0r/agfs/agfs-server/pkg/plugins/queuefs/internal"
)

func TestBackendConcurrentQueueMapAccess(t *testing.T) {
	backend := NewBackend()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			for j := 0; j < 200; j++ {
				queueName := fmt.Sprintf("jobs/%d", j%8)
				msg := model.QueueMessage{
					ID:        fmt.Sprintf("%d-%d", worker, j),
					Data:      "payload",
					Timestamp: time.Now(),
				}

				if err := backend.Enqueue(queueName, msg); err != nil {
					t.Errorf("Enqueue(%q): %v", queueName, err)
					return
				}
				if _, err := backend.ListQueues("jobs"); err != nil {
					t.Errorf("ListQueues: %v", err)
					return
				}
				if _, err := backend.QueueExists(queueName); err != nil {
					t.Errorf("QueueExists(%q): %v", queueName, err)
					return
				}
				if j%25 == 0 {
					if err := backend.RemoveQueue(queueName); err != nil {
						t.Errorf("RemoveQueue(%q): %v", queueName, err)
						return
					}
				}
			}
		}(i)
	}

	wg.Wait()
	if err := backend.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
