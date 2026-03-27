package queuefs

import "time"

// QueueBackend defines the interface for queue storage backends.
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
