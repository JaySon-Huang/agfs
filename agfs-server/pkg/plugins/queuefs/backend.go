package queuefs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// QueueBackend defines the durable queue operations implemented by queuefs
// storage backends.
type QueueBackend interface {
	// Initialize initializes the backend with configuration.
	Initialize(config map[string]interface{}) error

	// Close closes the backend connection.
	Close() error

	// GetType returns the backend type name.
	GetType() string

	// Enqueue appends a task to the queue.
	Enqueue(queueName string, task QueueTask) error

	// Dequeue leases and returns the next executable task.
	Dequeue(queueName string, consumerID string, leaseDuration time.Duration) (QueueTask, bool, error)

	// Peek returns the next executable task without leasing it.
	Peek(queueName string) (QueueTask, bool, error)

	// Ack marks a leased task as successfully completed.
	Ack(queueName string, taskID string) error

	// Nack marks a leased task as failed or re-queues it for retry.
	Nack(queueName string, taskID string, lastError string, retry bool, retryAfter time.Duration) error

	// RecoverStale re-queues tasks whose lease has expired.
	RecoverStale(queueName string, now time.Time) (int, error)

	// Size returns the number of tasks that are immediately consumable.
	Size(queueName string) (int, error)

	// Stats returns counts for each durable task state.
	Stats(queueName string) (QueueStats, error)

	// Clear removes all tasks from a queue.
	Clear(queueName string) error

	// ListQueues returns all queue names (for directory listing).
	ListQueues(prefix string) ([]string, error)

	// GetLastEnqueueTime returns the timestamp of the last enqueued task.
	GetLastEnqueueTime(queueName string) (time.Time, error)

	// RemoveQueue removes all tasks for a queue and its nested queues.
	RemoveQueue(queueName string) error

	// CreateQueue creates an empty queue (for mkdir support).
	CreateQueue(queueName string) error

	// QueueExists checks if a queue exists even if it is empty.
	QueueExists(queueName string) (bool, error)
}

type memoryTaskRecord struct {
	task           QueueTask
	status         TaskStatus
	attemptCount   int
	maxAttempts    int
	leasedBy       string
	leasedAt       time.Time
	leaseUntil     time.Time
	ackedAt        time.Time
	failedAt       time.Time
	deadLetteredAt time.Time
	lastError      string
	nextRetryAt    time.Time
	hasNextRetryAt bool
}

type memoryQueue struct {
	tasks           []*memoryTaskRecord
	lastEnqueueTime time.Time
	mu              sync.Mutex
}

// MemoryBackend implements QueueBackend using in-memory storage.
type MemoryBackend struct {
	cfg    QueueConfig
	queues map[string]*memoryQueue
	mu     sync.RWMutex
}

func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		queues: make(map[string]*memoryQueue),
	}
}

func (b *MemoryBackend) Initialize(config map[string]interface{}) error {
	cfg, err := parseQueueConfig(config)
	if err != nil {
		return err
	}
	b.cfg = cfg
	return nil
}

func (b *MemoryBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queues = nil
	return nil
}

func (b *MemoryBackend) GetType() string {
	return "memory"
}

func (b *MemoryBackend) getOrCreateQueue(queueName string) *memoryQueue {
	b.mu.Lock()
	defer b.mu.Unlock()

	if queue, exists := b.queues[queueName]; exists {
		return queue
	}

	queue := &memoryQueue{}
	b.queues[queueName] = queue
	return queue
}

func (b *MemoryBackend) getQueue(queueName string) (*memoryQueue, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	queue, exists := b.queues[queueName]
	return queue, exists
}

func (b *MemoryBackend) Enqueue(queueName string, task QueueTask) error {
	queue := b.getOrCreateQueue(queueName)
	queue.mu.Lock()
	defer queue.mu.Unlock()

	record := &memoryTaskRecord{
		task:        task.clone(),
		status:      TaskStatusQueued,
		maxAttempts: task.MaxAttempts,
	}
	if record.maxAttempts <= 0 {
		record.maxAttempts = b.cfg.DefaultMaxRetry
		record.task.MaxAttempts = record.maxAttempts
	}

	queue.tasks = append(queue.tasks, record)
	if task.Timestamp.After(queue.lastEnqueueTime) {
		queue.lastEnqueueTime = task.Timestamp
	} else {
		queue.lastEnqueueTime = queue.lastEnqueueTime.Add(time.Nanosecond)
	}

	return nil
}

func (b *MemoryBackend) Dequeue(queueName string, consumerID string, leaseDuration time.Duration) (QueueTask, bool, error) {
	queue, exists := b.getQueue(queueName)
	if !exists {
		return QueueTask{}, false, nil
	}

	now := time.Now().UTC()
	leaseUntil := now.Add(leaseDuration)

	queue.mu.Lock()
	defer queue.mu.Unlock()

	for _, record := range queue.tasks {
		if record.status != TaskStatusQueued || !record.isReady(now) {
			continue
		}

		record.status = TaskStatusProcessing
		record.attemptCount++
		record.leasedBy = consumerID
		record.leasedAt = now
		record.leaseUntil = leaseUntil
		record.hasNextRetryAt = false

		task := record.materialize()
		task.Attempt = record.attemptCount
		task.MaxAttempts = record.maxAttempts
		task.LeaseUntil = &leaseUntil
		return task, true, nil
	}

	return QueueTask{}, false, nil
}

func (b *MemoryBackend) Peek(queueName string) (QueueTask, bool, error) {
	queue, exists := b.getQueue(queueName)
	if !exists {
		return QueueTask{}, false, nil
	}

	now := time.Now().UTC()

	queue.mu.Lock()
	defer queue.mu.Unlock()

	for _, record := range queue.tasks {
		if record.status != TaskStatusQueued || !record.isReady(now) {
			continue
		}

		task := record.materialize()
		task.Attempt = record.attemptCount
		task.MaxAttempts = record.maxAttempts
		return task, true, nil
	}

	return QueueTask{}, false, nil
}

func (b *MemoryBackend) Ack(queueName string, taskID string) error {
	queue, exists := b.getQueue(queueName)
	if !exists {
		return fmt.Errorf("queue does not exist: %s", queueName)
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	record := queue.findTask(taskID)
	if record == nil {
		return fmt.Errorf("task not found: %s", taskID)
	}
	if record.status != TaskStatusProcessing {
		return fmt.Errorf("task %s is not in processing state", taskID)
	}

	now := time.Now().UTC()
	record.status = TaskStatusSucceeded
	record.ackedAt = now
	record.clearLease()
	return nil
}

func (b *MemoryBackend) Nack(queueName string, taskID string, lastError string, retry bool, retryAfter time.Duration) error {
	queue, exists := b.getQueue(queueName)
	if !exists {
		return fmt.Errorf("queue does not exist: %s", queueName)
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	record := queue.findTask(taskID)
	if record == nil {
		return fmt.Errorf("task not found: %s", taskID)
	}
	if record.status != TaskStatusProcessing {
		return fmt.Errorf("task %s is not in processing state", taskID)
	}

	now := time.Now().UTC()
	record.lastError = lastError

	if retry && record.attemptCount < record.maxAttempts {
		record.status = TaskStatusQueued
		record.clearLease()
		if retryAfter > 0 {
			record.nextRetryAt = now.Add(retryAfter)
			record.hasNextRetryAt = true
		} else {
			record.hasNextRetryAt = false
		}
		return nil
	}

	record.clearLease()
	if retry && b.cfg.EnableDeadLetter && record.attemptCount >= record.maxAttempts {
		record.status = TaskStatusDeadLettered
		record.deadLetteredAt = now
		return nil
	}

	record.status = TaskStatusFailed
	record.failedAt = now
	return nil
}

func (b *MemoryBackend) RecoverStale(queueName string, now time.Time) (int, error) {
	queue, exists := b.getQueue(queueName)
	if !exists {
		return 0, nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	recovered := 0
	for _, record := range queue.tasks {
		if record.status != TaskStatusProcessing {
			continue
		}
		if record.leaseUntil.IsZero() || record.leaseUntil.After(now) {
			continue
		}

		record.status = TaskStatusQueued
		record.clearLease()
		recovered++
	}

	return recovered, nil
}

func (b *MemoryBackend) Size(queueName string) (int, error) {
	queue, exists := b.getQueue(queueName)
	if !exists {
		return 0, nil
	}

	now := time.Now().UTC()

	queue.mu.Lock()
	defer queue.mu.Unlock()

	count := 0
	for _, record := range queue.tasks {
		if record.status == TaskStatusQueued && record.isReady(now) {
			count++
		}
	}
	return count, nil
}

func (b *MemoryBackend) Stats(queueName string) (QueueStats, error) {
	queue, exists := b.getQueue(queueName)
	if !exists {
		return QueueStats{}, nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	var stats QueueStats
	for _, record := range queue.tasks {
		switch record.status {
		case TaskStatusQueued:
			stats.Queued++
		case TaskStatusProcessing:
			stats.Processing++
		case TaskStatusSucceeded:
			stats.Succeeded++
		case TaskStatusFailed:
			stats.Failed++
		case TaskStatusDeadLettered:
			stats.DeadLettered++
		}
	}
	return stats, nil
}

func (b *MemoryBackend) Clear(queueName string) error {
	queue, exists := b.getQueue(queueName)
	if !exists {
		return nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	queue.tasks = nil
	queue.lastEnqueueTime = time.Time{}
	return nil
}

func (b *MemoryBackend) ListQueues(prefix string) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var queues []string
	for queueName := range b.queues {
		if prefix == "" || queueName == prefix || strings.HasPrefix(queueName, prefix+"/") {
			queues = append(queues, queueName)
		}
	}
	return queues, nil
}

func (b *MemoryBackend) GetLastEnqueueTime(queueName string) (time.Time, error) {
	queue, exists := b.getQueue(queueName)
	if !exists {
		return time.Time{}, nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	return queue.lastEnqueueTime, nil
}

func (b *MemoryBackend) RemoveQueue(queueName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if queueName == "" {
		b.queues = make(map[string]*memoryQueue)
		return nil
	}

	delete(b.queues, queueName)
	prefix := queueName + "/"
	for name := range b.queues {
		if strings.HasPrefix(name, prefix) {
			delete(b.queues, name)
		}
	}
	return nil
}

func (b *MemoryBackend) CreateQueue(queueName string) error {
	b.getOrCreateQueue(queueName)
	return nil
}

func (b *MemoryBackend) QueueExists(queueName string) (bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	_, exists := b.queues[queueName]
	return exists, nil
}

func (q *memoryQueue) findTask(taskID string) *memoryTaskRecord {
	for _, record := range q.tasks {
		if record.task.ID == taskID {
			return record
		}
	}
	return nil
}

func (r *memoryTaskRecord) materialize() QueueTask {
	task := r.task.clone()
	task.Attempt = r.attemptCount
	task.MaxAttempts = r.maxAttempts
	if !r.leaseUntil.IsZero() {
		leaseUntil := r.leaseUntil
		task.LeaseUntil = &leaseUntil
	}
	return task
}

func (r *memoryTaskRecord) isReady(now time.Time) bool {
	return !r.hasNextRetryAt || !r.nextRetryAt.After(now)
}

func (r *memoryTaskRecord) clearLease() {
	r.leasedBy = ""
	r.leasedAt = time.Time{}
	r.leaseUntil = time.Time{}
}

// SQLBackend implements QueueBackend using a SQL database backend. It supports
// TiDB/MySQL for production and SQLite for local testing.
type SQLBackend struct {
	db          *sql.DB
	backend     DBBackend
	backendType string
	cfg         QueueConfig
	tableCache  map[string]string
	cacheMu     sync.RWMutex
}

func NewSQLBackend() *SQLBackend {
	return &SQLBackend{
		tableCache: make(map[string]string),
	}
}

// NewTiDBBackend is kept for compatibility with the existing plugin wiring.
func NewTiDBBackend() *SQLBackend {
	return NewSQLBackend()
}

func (b *SQLBackend) Initialize(config map[string]interface{}) error {
	queueCfg, err := parseQueueConfig(config)
	if err != nil {
		return err
	}
	b.cfg = queueCfg

	backendType := "memory"
	if value, ok := config["backend"].(string); ok && value != "" {
		backendType = value
	}
	b.backendType = backendType

	backend, err := CreateBackend(config)
	if err != nil {
		return fmt.Errorf("failed to create backend: %w", err)
	}
	b.backend = backend

	db, err := backend.Open(config)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	b.db = db

	for _, stmt := range backend.GetInitSQL() {
		if _, err := b.db.Exec(stmt); err != nil {
			_ = b.db.Close()
			return fmt.Errorf("failed to initialize schema: %w", err)
		}
	}

	if err := b.migrateRegisteredQueues(); err != nil {
		_ = b.db.Close()
		return err
	}

	return nil
}

func (b *SQLBackend) Close() error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

func (b *SQLBackend) GetType() string {
	return b.backendType
}

func (b *SQLBackend) Enqueue(queueName string, task QueueTask) error {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return fmt.Errorf("queue does not exist: %s (create it with mkdir first)", queueName)
	}
	if err != nil {
		return fmt.Errorf("failed to get queue table name: %w", err)
	}

	if task.MaxAttempts <= 0 {
		task.MaxAttempts = b.cfg.DefaultMaxRetry
	}

	payload, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task payload: %w", err)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (message_id, data, timestamp, status, attempt_count, max_attempts, dedupe_key, priority) VALUES (?, ?, ?, ?, 0, ?, ?, ?)",
		quoteIdentifier(tableName),
	)

	_, err = b.db.Exec(
		query,
		task.ID,
		payload,
		encodeTimestamp(task.Timestamp),
		string(TaskStatusQueued),
		task.MaxAttempts,
		nullIfEmpty(task.DedupeKey),
		task.Priority,
	)
	if err != nil {
		return fmt.Errorf("failed to enqueue task: %w", err)
	}

	return nil
}

func (b *SQLBackend) Dequeue(queueName string, consumerID string, leaseDuration time.Duration) (QueueTask, bool, error) {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return QueueTask{}, false, nil
	}
	if err != nil {
		return QueueTask{}, false, fmt.Errorf("failed to get queue table name: %w", err)
	}

	if b.backend.GetDriverName() == "mysql" {
		return b.dequeueSkipLocked(tableName, consumerID, leaseDuration)
	}
	return b.dequeueCompareAndSwap(tableName, consumerID, leaseDuration)
}

func (b *SQLBackend) Peek(queueName string) (QueueTask, bool, error) {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return QueueTask{}, false, nil
	}
	if err != nil {
		return QueueTask{}, false, fmt.Errorf("failed to get queue table name: %w", err)
	}

	query := fmt.Sprintf(
		"SELECT data, attempt_count, max_attempts FROM %s WHERE status = ? AND (next_retry_at IS NULL OR next_retry_at <= ?) ORDER BY id LIMIT 1",
		quoteIdentifier(tableName),
	)

	var payload []byte
	var attemptCount int
	var maxAttempts int
	err = b.db.QueryRow(query, string(TaskStatusQueued), time.Now().UTC()).Scan(&payload, &attemptCount, &maxAttempts)
	if err == sql.ErrNoRows {
		return QueueTask{}, false, nil
	}
	if err != nil {
		return QueueTask{}, false, fmt.Errorf("failed to peek task: %w", err)
	}

	task, err := decodeTask(payload)
	if err != nil {
		return QueueTask{}, false, err
	}
	task.Attempt = attemptCount
	task.MaxAttempts = maxAttempts
	return task, true, nil
}

func (b *SQLBackend) Ack(queueName string, taskID string) error {
	return b.updateTaskTerminalState(queueName, taskID, taskUpdate{
		status:   TaskStatusSucceeded,
		setNowAt: "acked_at",
	})
}

func (b *SQLBackend) Nack(queueName string, taskID string, lastError string, retry bool, retryAfter time.Duration) error {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return fmt.Errorf("queue does not exist: %s", queueName)
	}
	if err != nil {
		return fmt.Errorf("failed to get queue table name: %w", err)
	}

	taskMeta, err := b.getTaskMeta(tableName, taskID)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	var query string
	var args []interface{}

	switch {
	case retry && taskMeta.AttemptCount < taskMeta.MaxAttempts:
		query = fmt.Sprintf(
			"UPDATE %s SET status = ?, last_error = ?, next_retry_at = ?, lease_until = NULL, leased_at = NULL, leased_by = NULL WHERE message_id = ? AND status = ?",
			quoteIdentifier(tableName),
		)
		var nextRetryAt interface{}
		if retryAfter > 0 {
			nextRetryAt = now.Add(retryAfter)
		} else {
			nextRetryAt = nil
		}
		args = []interface{}{string(TaskStatusQueued), nullIfEmpty(lastError), nextRetryAt, taskID, string(TaskStatusProcessing)}
	case retry && b.cfg.EnableDeadLetter:
		query = fmt.Sprintf(
			"UPDATE %s SET status = ?, last_error = ?, dead_lettered_at = ?, lease_until = NULL, leased_at = NULL, leased_by = NULL WHERE message_id = ? AND status = ?",
			quoteIdentifier(tableName),
		)
		args = []interface{}{string(TaskStatusDeadLettered), nullIfEmpty(lastError), now, taskID, string(TaskStatusProcessing)}
	default:
		query = fmt.Sprintf(
			"UPDATE %s SET status = ?, last_error = ?, failed_at = ?, lease_until = NULL, leased_at = NULL, leased_by = NULL WHERE message_id = ? AND status = ?",
			quoteIdentifier(tableName),
		)
		args = []interface{}{string(TaskStatusFailed), nullIfEmpty(lastError), now, taskID, string(TaskStatusProcessing)}
	}

	result, err := b.db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed to update task %s: %w", taskID, err)
	}
	return ensureRowsAffected(result, taskID)
}

func (b *SQLBackend) RecoverStale(queueName string, now time.Time) (int, error) {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get queue table name: %w", err)
	}

	query := fmt.Sprintf(
		"UPDATE %s SET status = ?, lease_until = NULL, leased_at = NULL, leased_by = NULL WHERE status = ? AND lease_until IS NOT NULL AND lease_until < ?",
		quoteIdentifier(tableName),
	)

	result, err := b.db.Exec(query, string(TaskStatusQueued), string(TaskStatusProcessing), now.UTC())
	if err != nil {
		return 0, fmt.Errorf("failed to recover stale tasks: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to read recover result: %w", err)
	}
	return int(count), nil
}

func (b *SQLBackend) Size(queueName string) (int, error) {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get queue table name: %w", err)
	}

	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s WHERE status = ? AND (next_retry_at IS NULL OR next_retry_at <= ?)",
		quoteIdentifier(tableName),
	)

	var count int
	err = b.db.QueryRow(query, string(TaskStatusQueued), time.Now().UTC()).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get queue size: %w", err)
	}
	return count, nil
}

func (b *SQLBackend) Stats(queueName string) (QueueStats, error) {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return QueueStats{}, nil
	}
	if err != nil {
		return QueueStats{}, fmt.Errorf("failed to get queue table name: %w", err)
	}

	query := fmt.Sprintf("SELECT status, COUNT(*) FROM %s GROUP BY status", quoteIdentifier(tableName))
	rows, err := b.db.Query(query)
	if err != nil {
		return QueueStats{}, fmt.Errorf("failed to query queue stats: %w", err)
	}
	defer rows.Close()

	var stats QueueStats
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return QueueStats{}, fmt.Errorf("failed to scan queue stats: %w", err)
		}

		switch TaskStatus(status) {
		case TaskStatusQueued:
			stats.Queued = count
		case TaskStatusProcessing:
			stats.Processing = count
		case TaskStatusSucceeded:
			stats.Succeeded = count
		case TaskStatusFailed:
			stats.Failed = count
		case TaskStatusDeadLettered:
			stats.DeadLettered = count
		}
	}

	return stats, nil
}

func (b *SQLBackend) Clear(queueName string) error {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get queue table name: %w", err)
	}

	_, err = b.db.Exec(fmt.Sprintf("DELETE FROM %s", quoteIdentifier(tableName)))
	if err != nil {
		return fmt.Errorf("failed to clear queue: %w", err)
	}
	return nil
}

func (b *SQLBackend) ListQueues(prefix string) ([]string, error) {
	var (
		query string
		args  []interface{}
	)

	if prefix == "" {
		query = "SELECT queue_name FROM queuefs_registry"
	} else {
		query = "SELECT queue_name FROM queuefs_registry WHERE queue_name = ? OR queue_name LIKE ?"
		args = []interface{}{prefix, prefix + "/%"}
	}

	rows, err := b.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list queues: %w", err)
	}
	defer rows.Close()

	var queues []string
	for rows.Next() {
		var queueName string
		if err := rows.Scan(&queueName); err != nil {
			return nil, fmt.Errorf("failed to scan queue name: %w", err)
		}
		queues = append(queues, queueName)
	}
	return queues, nil
}

func (b *SQLBackend) GetLastEnqueueTime(queueName string) (time.Time, error) {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get queue table name: %w", err)
	}

	query := fmt.Sprintf("SELECT MAX(timestamp) FROM %s", quoteIdentifier(tableName))
	var timestamp sql.NullInt64
	err = b.db.QueryRow(query).Scan(&timestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get last enqueue time: %w", err)
	}
	if !timestamp.Valid {
		return time.Time{}, nil
	}
	return decodeTimestamp(timestamp.Int64), nil
}

func (b *SQLBackend) RemoveQueue(queueName string) error {
	if queueName == "" {
		rows, err := b.db.Query("SELECT queue_name, table_name FROM queuefs_registry")
		if err != nil {
			return fmt.Errorf("failed to list queues: %w", err)
		}
		defer rows.Close()

		var entries []struct {
			queueName string
			tableName string
		}
		for rows.Next() {
			var entry struct {
				queueName string
				tableName string
			}
			if err := rows.Scan(&entry.queueName, &entry.tableName); err != nil {
				return fmt.Errorf("failed to scan queue registry: %w", err)
			}
			entries = append(entries, entry)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("failed to close queue registry rows: %w", err)
		}

		for _, entry := range entries {
			if _, err := b.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdentifier(entry.tableName))); err != nil {
				log.Warnf("[queuefs] Failed to drop table %s: %v", entry.tableName, err)
			}
			b.invalidateCache(entry.queueName)
		}

		if _, err := b.db.Exec("DELETE FROM queuefs_registry"); err != nil {
			return err
		}
		return nil
	}

	rows, err := b.db.Query(
		"SELECT queue_name, table_name FROM queuefs_registry WHERE queue_name = ? OR queue_name LIKE ?",
		queueName, queueName+"/%",
	)
	if err != nil {
		return fmt.Errorf("failed to query queues: %w", err)
	}
	defer rows.Close()

	var entries []struct {
		queueName string
		tableName string
	}
	for rows.Next() {
		var entry struct {
			queueName string
			tableName string
		}
		if err := rows.Scan(&entry.queueName, &entry.tableName); err != nil {
			return fmt.Errorf("failed to scan queue registry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("failed to close queue registry rows: %w", err)
	}

	for _, entry := range entries {
		if _, err := b.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdentifier(entry.tableName))); err != nil {
			log.Warnf("[queuefs] Failed to drop table %s: %v", entry.tableName, err)
		}
		b.invalidateCache(entry.queueName)
	}

	_, err = b.db.Exec(
		"DELETE FROM queuefs_registry WHERE queue_name = ? OR queue_name LIKE ?",
		queueName, queueName+"/%",
	)
	return err
}

func (b *SQLBackend) CreateQueue(queueName string) error {
	tableName := sanitizeTableName(queueName)

	if _, err := b.db.Exec(b.backend.GetCreateQueueTableSQL(tableName)); err != nil {
		return fmt.Errorf("failed to create queue table: %w", err)
	}
	if err := b.ensureQueueTableSchema(tableName); err != nil {
		return err
	}

	_, err := b.db.Exec(
		"INSERT OR IGNORE INTO queuefs_registry (queue_name, table_name) VALUES (?, ?)",
		queueName,
		tableName,
	)
	if err != nil && b.backend.GetDriverName() == "mysql" {
		_, err = b.db.Exec(
			"INSERT IGNORE INTO queuefs_registry (queue_name, table_name) VALUES (?, ?)",
			queueName,
			tableName,
		)
	}
	if err != nil {
		return fmt.Errorf("failed to register queue: %w", err)
	}

	b.cacheMu.Lock()
	b.tableCache[queueName] = tableName
	b.cacheMu.Unlock()

	return nil
}

func (b *SQLBackend) QueueExists(queueName string) (bool, error) {
	b.cacheMu.RLock()
	_, exists := b.tableCache[queueName]
	b.cacheMu.RUnlock()
	if exists {
		return true, nil
	}

	var count int
	err := b.db.QueryRow("SELECT COUNT(*) FROM queuefs_registry WHERE queue_name = ?", queueName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check queue existence: %w", err)
	}
	return count > 0, nil
}

func (b *SQLBackend) migrateRegisteredQueues() error {
	rows, err := b.db.Query("SELECT queue_name, table_name FROM queuefs_registry")
	if err != nil {
		if isMissingTableError(err) {
			return nil
		}
		return fmt.Errorf("failed to read queue registry: %w", err)
	}

	var entries []struct {
		queueName string
		tableName string
	}
	for rows.Next() {
		var entry struct {
			queueName string
			tableName string
		}
		if err := rows.Scan(&entry.queueName, &entry.tableName); err != nil {
			return fmt.Errorf("failed to scan queue registry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("failed to close queue registry rows: %w", err)
	}

	for _, entry := range entries {
		if err := b.ensureQueueTableSchema(entry.tableName); err != nil {
			return err
		}
		b.cacheMu.Lock()
		b.tableCache[entry.queueName] = entry.tableName
		b.cacheMu.Unlock()
	}

	return nil
}

func (b *SQLBackend) getTableName(queueName string, forceRefresh bool) (string, error) {
	if !forceRefresh {
		b.cacheMu.RLock()
		if tableName, exists := b.tableCache[queueName]; exists {
			b.cacheMu.RUnlock()
			return tableName, nil
		}
		b.cacheMu.RUnlock()
	}

	var tableName string
	err := b.db.QueryRow(
		"SELECT table_name FROM queuefs_registry WHERE queue_name = ?",
		queueName,
	).Scan(&tableName)
	if err != nil {
		return "", err
	}

	b.cacheMu.Lock()
	b.tableCache[queueName] = tableName
	b.cacheMu.Unlock()

	return tableName, nil
}

func (b *SQLBackend) invalidateCache(queueName string) {
	b.cacheMu.Lock()
	delete(b.tableCache, queueName)
	b.cacheMu.Unlock()
}

func (b *SQLBackend) ensureQueueTableSchema(tableName string) error {
	columnNames, err := b.readColumnNames(tableName)
	if err != nil {
		return fmt.Errorf("failed to inspect queue table %s: %w", tableName, err)
	}

	hadStatus := columnNames["status"]
	for column, definition := range b.backend.GetQueueColumnDefinitions() {
		if columnNames[column] {
			continue
		}
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", quoteIdentifier(tableName), quoteIdentifier(column), definition)
		if _, err := b.db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to migrate queue table %s: %w", tableName, err)
		}
	}

	if !hadStatus && columnNames["deleted"] {
		update := fmt.Sprintf(
			"UPDATE %s SET status = CASE WHEN deleted = 1 THEN ? ELSE ? END",
			quoteIdentifier(tableName),
		)
		if _, err := b.db.Exec(update, string(TaskStatusSucceeded), string(TaskStatusQueued)); err != nil {
			return fmt.Errorf("failed to backfill queue status for %s: %w", tableName, err)
		}
	}

	for indexName, columns := range b.backend.GetQueueIndexes(tableName) {
		stmt := fmt.Sprintf("CREATE INDEX %s ON %s (%s)", quoteIdentifier(indexName), quoteIdentifier(tableName), columns)
		if _, err := b.db.Exec(stmt); err != nil && !isDuplicateIndexError(err) {
			return fmt.Errorf("failed to create index %s on %s: %w", indexName, tableName, err)
		}
	}

	return nil
}

func (b *SQLBackend) readColumnNames(tableName string) (map[string]bool, error) {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", quoteIdentifier(tableName))
	rows, err := b.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	columnNames := make(map[string]bool, len(columns))
	for _, column := range columns {
		columnNames[column] = true
	}
	return columnNames, nil
}

func (b *SQLBackend) dequeueSkipLocked(tableName string, consumerID string, leaseDuration time.Duration) (QueueTask, bool, error) {
	tx, err := b.db.Begin()
	if err != nil {
		return QueueTask{}, false, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	leaseUntil := now.Add(leaseDuration)

	query := fmt.Sprintf(
		"SELECT id, data, attempt_count, max_attempts FROM %s WHERE status = ? AND (next_retry_at IS NULL OR next_retry_at <= ?) ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",
		quoteIdentifier(tableName),
	)

	var (
		rowID        int64
		payload      []byte
		attemptCount int
		maxAttempts  int
	)
	err = tx.QueryRow(query, string(TaskStatusQueued), now).Scan(&rowID, &payload, &attemptCount, &maxAttempts)
	if err == sql.ErrNoRows {
		return QueueTask{}, false, nil
	}
	if err != nil {
		return QueueTask{}, false, fmt.Errorf("failed to select task: %w", err)
	}

	update := fmt.Sprintf(
		"UPDATE %s SET status = ?, attempt_count = attempt_count + 1, leased_at = ?, lease_until = ?, leased_by = ?, next_retry_at = NULL WHERE id = ? AND status = ?",
		quoteIdentifier(tableName),
	)
	result, err := tx.Exec(update, string(TaskStatusProcessing), now, leaseUntil, nullIfEmpty(consumerID), rowID, string(TaskStatusQueued))
	if err != nil {
		return QueueTask{}, false, fmt.Errorf("failed to lease task: %w", err)
	}
	if err := ensureRowsAffected(result, ""); err != nil {
		return QueueTask{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return QueueTask{}, false, fmt.Errorf("failed to commit dequeue transaction: %w", err)
	}

	task, err := decodeTask(payload)
	if err != nil {
		return QueueTask{}, false, err
	}
	task.Attempt = attemptCount + 1
	task.MaxAttempts = maxAttempts
	task.LeaseUntil = &leaseUntil
	return task, true, nil
}

func (b *SQLBackend) dequeueCompareAndSwap(tableName string, consumerID string, leaseDuration time.Duration) (QueueTask, bool, error) {
	now := time.Now().UTC()
	leaseUntil := now.Add(leaseDuration)

	for range 4 {
		tx, err := b.db.Begin()
		if err != nil {
			return QueueTask{}, false, fmt.Errorf("failed to start transaction: %w", err)
		}

		query := fmt.Sprintf(
			"SELECT id, data, attempt_count, max_attempts FROM %s WHERE status = ? AND (next_retry_at IS NULL OR next_retry_at <= ?) ORDER BY id LIMIT 1",
			quoteIdentifier(tableName),
		)

		var (
			rowID        int64
			payload      []byte
			attemptCount int
			maxAttempts  int
		)
		err = tx.QueryRow(query, string(TaskStatusQueued), now).Scan(&rowID, &payload, &attemptCount, &maxAttempts)
		if err == sql.ErrNoRows {
			_ = tx.Rollback()
			return QueueTask{}, false, nil
		}
		if err != nil {
			_ = tx.Rollback()
			return QueueTask{}, false, fmt.Errorf("failed to select task: %w", err)
		}

		update := fmt.Sprintf(
			"UPDATE %s SET status = ?, attempt_count = ?, leased_at = ?, lease_until = ?, leased_by = ?, next_retry_at = NULL WHERE id = ? AND status = ?",
			quoteIdentifier(tableName),
		)
		result, err := tx.Exec(update, string(TaskStatusProcessing), attemptCount+1, now, leaseUntil, nullIfEmpty(consumerID), rowID, string(TaskStatusQueued))
		if err != nil {
			_ = tx.Rollback()
			return QueueTask{}, false, fmt.Errorf("failed to lease task: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			_ = tx.Rollback()
			return QueueTask{}, false, fmt.Errorf("failed to read dequeue result: %w", err)
		}
		if rowsAffected == 0 {
			_ = tx.Rollback()
			continue
		}

		if err := tx.Commit(); err != nil {
			return QueueTask{}, false, fmt.Errorf("failed to commit dequeue transaction: %w", err)
		}

		task, err := decodeTask(payload)
		if err != nil {
			return QueueTask{}, false, err
		}
		task.Attempt = attemptCount + 1
		task.MaxAttempts = maxAttempts
		task.LeaseUntil = &leaseUntil
		return task, true, nil
	}

	return QueueTask{}, false, nil
}

type taskUpdate struct {
	status   TaskStatus
	setNowAt string
}

func (b *SQLBackend) updateTaskTerminalState(queueName string, taskID string, update taskUpdate) error {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return fmt.Errorf("queue does not exist: %s", queueName)
	}
	if err != nil {
		return fmt.Errorf("failed to get queue table name: %w", err)
	}

	query := fmt.Sprintf(
		"UPDATE %s SET status = ?, %s = ?, lease_until = NULL, leased_at = NULL, leased_by = NULL WHERE message_id = ? AND status = ?",
		quoteIdentifier(tableName),
		quoteIdentifier(update.setNowAt),
	)

	result, err := b.db.Exec(query, string(update.status), time.Now().UTC(), taskID, string(TaskStatusProcessing))
	if err != nil {
		return fmt.Errorf("failed to update task %s: %w", taskID, err)
	}
	return ensureRowsAffected(result, taskID)
}

type taskMeta struct {
	AttemptCount int
	MaxAttempts  int
}

func (b *SQLBackend) getTaskMeta(tableName string, taskID string) (taskMeta, error) {
	query := fmt.Sprintf(
		"SELECT attempt_count, max_attempts FROM %s WHERE message_id = ? AND status = ?",
		quoteIdentifier(tableName),
	)

	var meta taskMeta
	err := b.db.QueryRow(query, taskID, string(TaskStatusProcessing)).Scan(&meta.AttemptCount, &meta.MaxAttempts)
	if err == sql.ErrNoRows {
		return taskMeta{}, fmt.Errorf("task %s is not in processing state", taskID)
	}
	if err != nil {
		return taskMeta{}, fmt.Errorf("failed to load task %s: %w", taskID, err)
	}
	return meta, nil
}

func encodeTimestamp(ts time.Time) int64 {
	return ts.UTC().UnixNano()
}

func decodeTimestamp(value int64) time.Time {
	if value < 1_000_000_000_000 {
		return time.Unix(value, 0).UTC()
	}
	return time.Unix(0, value).UTC()
}

func decodeTask(payload []byte) (QueueTask, error) {
	var task QueueTask
	if err := json.Unmarshal(payload, &task); err != nil {
		return QueueTask{}, fmt.Errorf("failed to unmarshal task payload: %w", err)
	}
	if task.Timestamp.IsZero() {
		task.Timestamp = time.Now().UTC()
	}
	return task, nil
}

func nullIfEmpty(value string) interface{} {
	if value == "" {
		return nil
	}
	return value
}

func ensureRowsAffected(result sql.Result, taskID string) error {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read rows affected: %w", err)
	}
	if rowsAffected == 0 {
		if taskID == "" {
			return fmt.Errorf("queue state changed during dequeue")
		}
		return fmt.Errorf("task %s is not in processing state", taskID)
	}
	return nil
}

func quoteIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

func isDuplicateIndexError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "already exists") || strings.Contains(message, "duplicate key name")
}

func isMissingTableError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "no such table") || strings.Contains(message, "doesn't exist")
}
