package queuefs

import "time"

// MemoryBackend implements QueueBackend using in-memory storage.
type MemoryBackend struct {
	queues map[string]*Queue
}

func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		queues: make(map[string]*Queue),
	}
}

func (b *MemoryBackend) Initialize(config map[string]interface{}) error {
	// No initialization needed for memory backend.
	return nil
}

func (b *MemoryBackend) Close() error {
	b.queues = nil
	return nil
}

func (b *MemoryBackend) GetType() string {
	return "memory"
}

func (b *MemoryBackend) getOrCreateQueue(queueName string) *Queue {
	if queue, exists := b.queues[queueName]; exists {
		return queue
	}
	queue := &Queue{
		messages:        []QueueMessage{},
		lastEnqueueTime: time.Time{},
	}
	b.queues[queueName] = queue
	return queue
}

func (b *MemoryBackend) Enqueue(queueName string, msg QueueMessage) error {
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

func (b *MemoryBackend) Dequeue(queueName string) (QueueMessage, bool, error) {
	queue, exists := b.queues[queueName]
	if !exists {
		return QueueMessage{}, false, nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	if len(queue.messages) == 0 {
		return QueueMessage{}, false, nil
	}

	msg := queue.messages[0]
	queue.messages = queue.messages[1:]
	return msg, true, nil
}

func (b *MemoryBackend) Peek(queueName string) (QueueMessage, bool, error) {
	queue, exists := b.queues[queueName]
	if !exists {
		return QueueMessage{}, false, nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	if len(queue.messages) == 0 {
		return QueueMessage{}, false, nil
	}

	return queue.messages[0], true, nil
}

func (b *MemoryBackend) Size(queueName string) (int, error) {
	queue, exists := b.queues[queueName]
	if !exists {
		return 0, nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	return len(queue.messages), nil
}

func (b *MemoryBackend) Clear(queueName string) error {
	queue, exists := b.queues[queueName]
	if !exists {
		return nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	queue.messages = []QueueMessage{}
	queue.lastEnqueueTime = time.Time{}
	return nil
}

func (b *MemoryBackend) ListQueues(prefix string) ([]string, error) {
	var queues []string
	for qName := range b.queues {
		if prefix == "" || qName == prefix || len(qName) > len(prefix) && qName[:len(prefix)+1] == prefix+"/" {
			queues = append(queues, qName)
		}
	}
	return queues, nil
}

func (b *MemoryBackend) GetLastEnqueueTime(queueName string) (time.Time, error) {
	queue, exists := b.queues[queueName]
	if !exists {
		return time.Time{}, nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	return queue.lastEnqueueTime, nil
}

func (b *MemoryBackend) RemoveQueue(queueName string) error {
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

func (b *MemoryBackend) CreateQueue(queueName string) error {
	b.getOrCreateQueue(queueName)
	return nil
}

func (b *MemoryBackend) QueueExists(queueName string) (bool, error) {
	_, exists := b.queues[queueName]
	return exists, nil
}
