package memory

import (
	"fmt"
	"sync"
	"time"

	model "github.com/c4pt0r/agfs/agfs-server/pkg/plugins/queuefs/internal"
	"github.com/google/uuid"
)

const defaultLeaseDuration = 30 * time.Second

type queueItem struct {
	message       model.QueueMessage
	attempt       int
	recoveryCount int
}

type claimedItem struct {
	item       queueItem
	receipt    string
	claimedAt  time.Time
	leaseUntil time.Time
}

// Queue represents a single message queue for the memory backend.
type Queue struct {
	pending    []queueItem
	processing map[string]claimedItem
	// processingOrder preserves claim order for processing items because Go map
	// iteration is intentionally nondeterministic.
	processingOrder []string
	recoveries      int
	mu              sync.Mutex
	lastEnqueueTime time.Time // Tracks the timestamp of the most recently enqueued message
}

// Backend implements QueueBackend using in-memory storage.
type Backend struct {
	mu     sync.RWMutex
	queues map[string]*Queue
}

// NewBackend returns an in-memory queue backend.
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
		pending:         []queueItem{},
		processing:      make(map[string]claimedItem),
		processingOrder: []string{},
		lastEnqueueTime: time.Time{},
	}
	b.queues[queueName] = queue
	return queue
}

func (b *Backend) Enqueue(queueName string, msg model.QueueMessage) error {
	queue := b.getOrCreateQueue(queueName)
	queue.mu.Lock()
	defer queue.mu.Unlock()

	queue.pending = append(queue.pending, queueItem{message: msg})

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

	if len(queue.pending) == 0 {
		return model.QueueMessage{}, false, nil
	}

	item := queue.pending[0]
	queue.pending = queue.pending[1:]
	return item.message, true, nil
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

	if len(queue.pending) == 0 {
		return model.QueueMessage{}, false, nil
	}

	return queue.pending[0].message, true, nil
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

	return len(queue.pending), nil
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

	queue.pending = []queueItem{}
	queue.processing = make(map[string]claimedItem)
	queue.processingOrder = []string{}
	queue.recoveries = 0
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

func (b *Backend) Claim(queueName string, req model.ClaimRequest) (model.ClaimedMessage, bool, error) {
	b.mu.RLock()
	queue, exists := b.queues[queueName]
	b.mu.RUnlock()
	if !exists {
		return model.ClaimedMessage{}, false, nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	if len(queue.pending) == 0 {
		return model.ClaimedMessage{}, false, nil
	}

	item := queue.pending[0]
	queue.pending = queue.pending[1:]
	item.attempt++

	leaseDuration := req.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = defaultLeaseDuration
	}
	claimedAt := time.Now().UTC()
	receipt := uuid.NewString()
	claimed := claimedItem{
		item:       item,
		receipt:    receipt,
		claimedAt:  claimedAt,
		leaseUntil: claimedAt.Add(leaseDuration),
	}
	queue.processing[item.message.ID] = claimed
	// Keep a parallel ordered list so recovery can restore expired claims to the
	// pending queue deterministically.
	queue.processingOrder = append(queue.processingOrder, item.message.ID)

	return model.ClaimedMessage{
		MessageID:  item.message.ID,
		QueueName:  queueName,
		Data:       item.message.Data,
		Receipt:    receipt,
		ClaimedAt:  claimedAt,
		LeaseUntil: claimed.leaseUntil,
		Attempt:    item.attempt,
	}, true, nil
}

func (b *Backend) Ack(queueName string, messageID string, receipt string) error {
	_, queue, err := b.requireClaim(queueName, messageID, receipt)
	if err != nil {
		return err
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()
	if current, ok := queue.processing[messageID]; !ok || current.receipt != receipt {
		return fmt.Errorf("claim is no longer active for message %q", messageID)
	}
	delete(queue.processing, messageID)
	queue.removeProcessingOrderLocked(messageID)
	return nil
}

func (b *Backend) Release(queueName string, messageID string, req model.ReleaseRequest) error {
	claimed, queue, err := b.requireClaim(queueName, messageID, req.Receipt)
	if err != nil {
		return err
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()
	if current, ok := queue.processing[messageID]; !ok || current.receipt != req.Receipt {
		return fmt.Errorf("claim is no longer active for message %q", messageID)
	}
	delete(queue.processing, messageID)
	queue.removeProcessingOrderLocked(messageID)
	// Requeue at the front so an explicit release preserves FIFO semantics for the
	// message that was previously at the head of the queue.
	queue.pending = append([]queueItem{claimed.item}, queue.pending...)
	return nil
}

func (b *Backend) RecoverExpired(queueName string, now time.Time, limit int) (int, error) {
	b.mu.RLock()
	queue, exists := b.queues[queueName]
	b.mu.RUnlock()
	if !exists {
		return 0, nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	if now.IsZero() {
		now = time.Now().UTC()
	}
	recovered := 0
	toPrepend := make([]queueItem, 0)
	remainingProcessingOrder := make([]string, 0, len(queue.processingOrder))
	// Walk the explicit claim order rather than queue.processing so recovered
	// items re-enter pending in a stable, FIFO-compatible order.
	for _, messageID := range queue.processingOrder {
		claimed, ok := queue.processing[messageID]
		if !ok {
			continue
		}
		if limit > 0 && recovered >= limit {
			remainingProcessingOrder = append(remainingProcessingOrder, messageID)
			continue
		}
		if claimed.leaseUntil.After(now) {
			remainingProcessingOrder = append(remainingProcessingOrder, messageID)
			continue
		}
		delete(queue.processing, messageID)
		item := claimed.item
		item.recoveryCount++
		toPrepend = append(toPrepend, item)
		recovered++
	}
	queue.processingOrder = remainingProcessingOrder
	// Reverse the prepend order so multiple recovered claims become pending again
	// in the same claim order they originally occupied in processing.
	for i := len(toPrepend) - 1; i >= 0; i-- {
		queue.pending = append([]queueItem{toPrepend[i]}, queue.pending...)
	}
	queue.recoveries += recovered
	return recovered, nil
}

func (q *Queue) removeProcessingOrderLocked(messageID string) {
	// processingOrder is small and only mutated while holding queue.mu, so a
	// linear removal keeps the bookkeeping obvious and predictable.
	for i, current := range q.processingOrder {
		if current != messageID {
			continue
		}
		q.processingOrder = append(q.processingOrder[:i], q.processingOrder[i+1:]...)
		return
	}
}

func (b *Backend) Stats(queueName string) (model.QueueStats, error) {
	b.mu.RLock()
	queue, exists := b.queues[queueName]
	b.mu.RUnlock()
	if !exists {
		return model.QueueStats{}, nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	return model.QueueStats{
		Pending:    len(queue.pending),
		Processing: len(queue.processing),
		Recoveries: queue.recoveries,
	}, nil
}

func (b *Backend) requireClaim(queueName string, messageID string, receipt string) (claimedItem, *Queue, error) {
	b.mu.RLock()
	queue, exists := b.queues[queueName]
	b.mu.RUnlock()
	if !exists {
		return claimedItem{}, nil, fmt.Errorf("queue does not exist: %s", queueName)
	}

	queue.mu.Lock()
	claimed, ok := queue.processing[messageID]
	queue.mu.Unlock()
	if !ok {
		return claimedItem{}, nil, fmt.Errorf("message %q is not currently claimed", messageID)
	}
	if claimed.receipt != receipt {
		return claimedItem{}, nil, fmt.Errorf("invalid receipt for message %q", messageID)
	}
	// The caller re-locks the queue before mutating state and revalidates the
	// claim, so this helper only performs the shared existence/receipt checks.
	return claimed, queue, nil
}
