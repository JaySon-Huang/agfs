package memory

import (
	"sync"
	"time"

	model "github.com/c4pt0r/agfs/agfs-server/pkg/plugins/queuefs/internal"
)

// Queue represents a single message queue for the memory backend.
type Queue struct {
	messages        []model.QueueMessage
	mu              sync.Mutex
	lastEnqueueTime time.Time // Tracks the timestamp of the most recently enqueued message
}

// Backend implements QueueBackend using in-memory storage.
type Backend struct {
	mu     sync.RWMutex
	queues map[string]*Queue
}

func NewBackend() *Backend {
	return &Backend{
		queues: make(map[string]*Queue),
	}
}

func (b *Backend) Initialize(config map[string]interface{}) error {
	// No initialization needed for memory backend.
	return nil
}

func (b *Backend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.queues = nil
	return nil
}

func (b *Backend) GetType() string {
	return "memory"
}

func (b *Backend) getOrCreateQueue(queueName string) *Queue {
	b.mu.Lock()
	defer b.mu.Unlock()

	if queue, exists := b.queues[queueName]; exists {
		return queue
	}
	queue := &Queue{
		messages:        []model.QueueMessage{},
		lastEnqueueTime: time.Time{},
	}
	b.queues[queueName] = queue
	return queue
}

func (b *Backend) Enqueue(queueName string, msg model.QueueMessage) error {
	queue := b.getOrCreateQueue(queueName)
	queue.mu.Lock()
	defer queue.mu.Unlock()

	queue.messages = append(queue.messages, msg)

	// Update lastEnqueueTime.
	if msg.Timestamp.After(queue.lastEnqueueTime) {
		queue.lastEnqueueTime = msg.Timestamp
	} else {
		queue.lastEnqueueTime = queue.lastEnqueueTime.Add(1 * time.Nanosecond)
	}

	return nil
}

func (b *Backend) Dequeue(queueName string) (model.QueueMessage, bool, error) {
	b.mu.RLock()
	queue, exists := b.queues[queueName]
	b.mu.RUnlock()
	if !exists {
		return model.QueueMessage{}, false, nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	if len(queue.messages) == 0 {
		return model.QueueMessage{}, false, nil
	}

	msg := queue.messages[0]
	queue.messages = queue.messages[1:]
	return msg, true, nil
}

func (b *Backend) Peek(queueName string) (model.QueueMessage, bool, error) {
	b.mu.RLock()
	queue, exists := b.queues[queueName]
	b.mu.RUnlock()
	if !exists {
		return model.QueueMessage{}, false, nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	if len(queue.messages) == 0 {
		return model.QueueMessage{}, false, nil
	}

	return queue.messages[0], true, nil
}

func (b *Backend) Size(queueName string) (int, error) {
	b.mu.RLock()
	queue, exists := b.queues[queueName]
	b.mu.RUnlock()
	if !exists {
		return 0, nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	return len(queue.messages), nil
}

func (b *Backend) Clear(queueName string) error {
	b.mu.RLock()
	queue, exists := b.queues[queueName]
	b.mu.RUnlock()
	if !exists {
		return nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	queue.messages = []model.QueueMessage{}
	queue.lastEnqueueTime = time.Time{}
	return nil
}

func (b *Backend) ListQueues(prefix string) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var queues []string
	for qName := range b.queues {
		if prefix == "" || qName == prefix || len(qName) > len(prefix) && qName[:len(prefix)+1] == prefix+"/" {
			queues = append(queues, qName)
		}
	}
	return queues, nil
}

func (b *Backend) GetLastEnqueueTime(queueName string) (time.Time, error) {
	b.mu.RLock()
	queue, exists := b.queues[queueName]
	b.mu.RUnlock()
	if !exists {
		return time.Time{}, nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	return queue.lastEnqueueTime, nil
}

func (b *Backend) RemoveQueue(queueName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Remove the queue and all nested queues.
	if queueName == "" {
		b.queues = make(map[string]*Queue)
		return nil
	}

	delete(b.queues, queueName)

	// Remove nested queues.
	prefix := queueName + "/"
	for qName := range b.queues {
		if len(qName) > len(prefix) && qName[:len(prefix)] == prefix {
			delete(b.queues, qName)
		}
	}

	return nil
}

func (b *Backend) CreateQueue(queueName string) error {
	b.getOrCreateQueue(queueName)
	return nil
}

func (b *Backend) QueueExists(queueName string) (bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	_, exists := b.queues[queueName]
	return exists, nil
}
