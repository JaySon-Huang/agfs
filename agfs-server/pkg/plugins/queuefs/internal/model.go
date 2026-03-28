package model

import "time"

// QueueBackend defines the interface for fifo queue storage backends.
type QueueBackend interface {
	// Initialize initializes the backend with configuration.
	Initialize(config map[string]interface{}) error

	// Close closes the backend connection.
	Close() error

	// GetType returns the backend type name.
	GetType() string

	// Enqueue adds a message to a queue.
	Enqueue(queueName string, msg QueueMessage) error

	// Dequeue removes and returns the first message from a queue.
	Dequeue(queueName string) (QueueMessage, bool, error)

	// Peek returns the first message without removing it.
	Peek(queueName string) (QueueMessage, bool, error)

	// Size returns the number of messages in a queue.
	Size(queueName string) (int, error)

	// Clear removes all messages from a queue.
	Clear(queueName string) error

	// ListQueues returns all queue names (for directory listing).
	ListQueues(prefix string) ([]string, error)

	// GetLastEnqueueTime returns the timestamp of the last enqueued message.
	GetLastEnqueueTime(queueName string) (time.Time, error)

	// RemoveQueue removes all messages for a queue and its nested queues.
	RemoveQueue(queueName string) error

	// CreateQueue creates an empty queue (for mkdir support).
	CreateQueue(queueName string) error

	// QueueExists checks if a queue exists (even if empty).
	QueueExists(queueName string) (bool, error)
}

// DurableQueueBackend extends QueueBackend with claim/ack/recover semantics.
type DurableQueueBackend interface {
	QueueBackend

	// Claim atomically reserves the next pending message for a worker lease.
	Claim(queueName string, req ClaimRequest) (ClaimedMessage, bool, error)

	// Ack permanently completes a previously claimed message.
	Ack(queueName string, messageID string, receipt string) error

	// Release returns a claimed message back to the queue lifecycle.
	Release(queueName string, messageID string, req ReleaseRequest) error

	// RecoverExpired returns expired leases to the pending queue.
	RecoverExpired(queueName string, now time.Time, limit int) (int, error)

	// Stats returns queue-level durable state aggregates.
	Stats(queueName string) (QueueStats, error)
}

// QueueMessage is the logical message payload exchanged with queue backends.
type QueueMessage struct {
	ID        string    `json:"id"`
	Data      string    `json:"data"`
	Timestamp time.Time `json:"timestamp"`
}

// ClaimRequest describes how a worker wants to claim the next message.
type ClaimRequest struct {
	WorkerID      string        `json:"worker_id,omitempty"`
	LeaseDuration time.Duration `json:"lease_duration,omitempty"`
}

// ClaimedMessage is the durable dequeue payload returned to workers.
// Data intentionally follows the same string semantics as QueueMessage.Data so
// fifo and durable dequeue APIs expose the message body consistently.
type ClaimedMessage struct {
	MessageID  string    `json:"message_id"`
	QueueName  string    `json:"queue_name"`
	Data       string    `json:"data"`
	Receipt    string    `json:"receipt"`
	ClaimedAt  time.Time `json:"claimed_at"`
	LeaseUntil time.Time `json:"lease_until"`
	Attempt    int       `json:"attempt"`
}

// ReleaseRequest describes how a claimed message should be released.
type ReleaseRequest struct {
	Receipt string     `json:"receipt"`
	RetryAt *time.Time `json:"retry_at,omitempty"`
	Reason  string     `json:"reason,omitempty"`
}

// QueueStats captures durable queue-level state counts.
// Recoveries is a historical queue-level counter rather than a count of only
// the messages that are still pending or processing.
type QueueStats struct {
	Pending    int `json:"pending"`
	Processing int `json:"processing"`
	Recoveries int `json:"recoveries"`
}
