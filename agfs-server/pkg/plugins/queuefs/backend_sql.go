package queuefs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	log "github.com/sirupsen/logrus"
)

// SQLQueueBackend implements QueueBackend using a SQL database.
// queuefs_registry is the logical source of truth for queue existence and
// queue-to-table mapping; physical queue tables are addressed only through
// registry/cache lookups rather than by scanning database objects directly.
// TODO: tableCache is still treated as a local fast path in some code paths.
// That can make cross-instance idempotent delete/recreate flows observe stale
// queue metadata until the cache is refreshed from queuefs_registry.
type SQLQueueBackend struct {
	db          *sql.DB
	backend     DBBackend
	backendType string
	tableCache  map[string]string // queueName -> tableName cache
	cacheMu     sync.RWMutex      // protects tableCache
}

type queueDeleteTarget struct {
	queueName string
	tableName string
}

func newSQLQueueBackend(backendType string, backend DBBackend) *SQLQueueBackend {
	return &SQLQueueBackend{
		backendType: backendType,
		backend:     backend,
		tableCache:  make(map[string]string),
	}
}

func NewSQLQueueBackend() *SQLQueueBackend {
	return newSQLQueueBackend("", nil)
}

func NewSQLiteQueueBackend() *SQLQueueBackend {
	return newSQLQueueBackend("sqlite", NewSQLiteDBBackend())
}

func NewTiDBQueueBackend() *SQLQueueBackend {
	return newSQLQueueBackend("tidb", NewTiDBDBBackend())
}

func NewPostgresQueueBackend() *SQLQueueBackend {
	return newSQLQueueBackend("pgsql", NewPostgreSQLDBBackend())
}

func (b *SQLQueueBackend) Initialize(config map[string]interface{}) error {
	backendType := b.backendType
	if backendType == "" {
		if val, ok := config["backend"]; ok {
			if strVal, ok := val.(string); ok {
				backendType = strVal
			}
		}
	}
	if backendType == "" || backendType == "memory" {
		return fmt.Errorf("SQLQueueBackend requires a SQL backend (sqlite, tidb/mysql, pgsql/postgres); use MemoryBackend for in-memory mode")
	}
	b.backendType = backendType

	// Create database backend.
	backend := b.backend
	if backend == nil {
		var err error
		backend, err = CreateBackend(config)
		if err != nil {
			return fmt.Errorf("failed to create backend: %w", err)
		}
	}
	b.backend = backend

	// Open database connection.
	db, err := backend.Open(config)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	b.db = db

	// Initialize schema.
	for _, sqlStmt := range backend.GetInitSQL() {
		if _, err := db.Exec(sqlStmt); err != nil {
			db.Close()
			return fmt.Errorf("failed to initialize schema: %w", err)
		}
	}

	return nil
}

func (b *SQLQueueBackend) Close() error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

func (b *SQLQueueBackend) GetType() string {
	return b.backendType
}

func (b *SQLQueueBackend) rebind(query string) string {
	if b.backend == nil {
		return query
	}
	return b.backend.Rebind(query)
}

func (b *SQLQueueBackend) boolLiteral(value bool) string {
	if b.backend == nil {
		if value {
			return "1"
		}
		return "0"
	}
	return b.backend.BoolLiteral(value)
}

// getTableName retrieves the table name for a queue, using cache when possible.
// If forceRefresh is true, it will bypass the cache and query from database.
func (b *SQLQueueBackend) getTableName(queueName string, forceRefresh bool) (string, error) {
	// Try to get from cache first (unless force refresh).
	if !forceRefresh {
		b.cacheMu.RLock()
		if tableName, exists := b.tableCache[queueName]; exists {
			b.cacheMu.RUnlock()
			return tableName, nil
		}
		b.cacheMu.RUnlock()
	}

	// Query from database.
	var tableName string
	err := b.db.QueryRow(
		b.rebind("SELECT table_name FROM queuefs_registry WHERE queue_name = ?"),
		queueName,
	).Scan(&tableName)

	if err != nil {
		return "", err
	}

	// Update cache.
	b.cacheMu.Lock()
	b.tableCache[queueName] = tableName
	b.cacheMu.Unlock()

	return tableName, nil
}

// invalidateCache removes a queue from the cache.
func (b *SQLQueueBackend) invalidateCache(queueName string) {
	b.cacheMu.Lock()
	delete(b.tableCache, queueName)
	b.cacheMu.Unlock()
}

func (b *SQLQueueBackend) dropQueueTables(targets []queueDeleteTarget) ([]queueDeleteTarget, error) {
	var dropped []queueDeleteTarget
	var firstErr error
	failed := 0
	dropAttempt := 0

	for _, q := range targets {
		dropAttempt++
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", q.tableName)
		var dropErr error
		failpoint.Inject("queuefsRemoveQueueDropError", func(val failpoint.Value) {
			switch injected := val.(type) {
			case int:
				if dropAttempt == injected {
					dropErr = fmt.Errorf("injected drop failure on attempt %d for %s", dropAttempt, q.tableName)
				}
			case string:
				if injected == q.tableName || injected == q.queueName {
					dropErr = fmt.Errorf("injected drop failure for %s", q.tableName)
				}
			}
		})
		if dropErr == nil {
			_, dropErr = b.db.Exec(dropSQL)
		}
		if dropErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to drop queue table %q: %w", q.tableName, dropErr)
			}
			failed++
			continue
		}
		log.Infof("[queuefs] Dropped queue table '%s' for queue '%s'", q.tableName, q.queueName)
		dropped = append(dropped, q)
	}

	if firstErr != nil {
		return dropped, fmt.Errorf("failed to drop %d of %d queue tables: %w", failed, len(targets), firstErr)
	}
	return dropped, nil
}

func (b *SQLQueueBackend) deleteRegistryEntries(targets []queueDeleteTarget) error {
	for _, q := range targets {
		if _, err := b.db.Exec(
			b.rebind("DELETE FROM queuefs_registry WHERE queue_name = ?"),
			q.queueName,
		); err != nil {
			return fmt.Errorf("failed to remove queue %q from registry: %w", q.queueName, err)
		}
	}
	return nil
}

func (b *SQLQueueBackend) invalidateQueueCaches(targets []queueDeleteTarget) {
	for _, q := range targets {
		b.invalidateCache(q.queueName)
	}
}

func (b *SQLQueueBackend) Enqueue(queueName string, msg QueueMessage) error {
	msgData, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Get table name from cache (lazy loading).
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return fmt.Errorf("queue does not exist: %s (create it with mkdir first)", queueName)
	} else if err != nil {
		return fmt.Errorf("failed to get queue table name: %w", err)
	}

	// Insert message into queue table.
	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (message_id, data, timestamp, deleted) VALUES (?, ?, ?, %s)",
		tableName, b.boolLiteral(false),
	)
	insertSQL = b.rebind(insertSQL)
	_, err = b.db.Exec(insertSQL, msg.ID, string(msgData), msg.Timestamp.Unix())
	if err != nil {
		return fmt.Errorf("failed to enqueue message: %w", err)
	}

	return nil
}

func (b *SQLQueueBackend) Dequeue(queueName string) (QueueMessage, bool, error) {
	// Get table name from cache (lazy loading).
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return QueueMessage{}, false, nil
	} else if err != nil {
		return QueueMessage{}, false, fmt.Errorf("failed to get queue table name: %w", err)
	}

	// Start transaction.
	tx, err := b.db.Begin()
	if err != nil {
		return QueueMessage{}, false, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	// Get and mark the first non-deleted message as deleted in a single atomic operation.
	// Using FOR UPDATE SKIP LOCKED to skip rows locked by other transactions for better concurrency.
	var id int64
	var data string

	querySQL := fmt.Sprintf(
		"SELECT id, data FROM %s WHERE deleted = %s ORDER BY id LIMIT 1",
		tableName, b.boolLiteral(false),
	)
	if b.backend.SupportsSkipLocked() {
		querySQL += " FOR UPDATE SKIP LOCKED"
	}
	querySQL = b.rebind(querySQL)
	err = tx.QueryRow(querySQL).Scan(&id, &data)

	if err == sql.ErrNoRows {
		return QueueMessage{}, false, nil
	} else if err != nil {
		return QueueMessage{}, false, fmt.Errorf("failed to query message: %w", err)
	}

	// Mark the message as deleted.
	updateSQL := fmt.Sprintf(
		"UPDATE %s SET deleted = %s, deleted_at = CURRENT_TIMESTAMP WHERE id = ? AND deleted = %s",
		tableName, b.boolLiteral(true), b.boolLiteral(false),
	)
	updateSQL = b.rebind(updateSQL)
	result, err := tx.Exec(updateSQL, id)
	if err != nil {
		return QueueMessage{}, false, fmt.Errorf("failed to mark message as deleted: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return QueueMessage{}, false, fmt.Errorf("failed to check dequeue result: %w", err)
	}
	if rowsAffected == 0 {
		return QueueMessage{}, false, nil
	}

	// Commit transaction.
	if err := tx.Commit(); err != nil {
		return QueueMessage{}, false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Unmarshal message.
	var msg QueueMessage
	if err := json.Unmarshal([]byte(data), &msg); err != nil {
		return QueueMessage{}, false, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return msg, true, nil
}

func (b *SQLQueueBackend) Peek(queueName string) (QueueMessage, bool, error) {
	// Get table name from cache (lazy loading).
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return QueueMessage{}, false, nil
	} else if err != nil {
		return QueueMessage{}, false, fmt.Errorf("failed to get queue table name: %w", err)
	}

	var data string
	querySQL := fmt.Sprintf(
		"SELECT data FROM %s WHERE deleted = %s ORDER BY id LIMIT 1",
		tableName, b.boolLiteral(false),
	)
	err = b.db.QueryRow(querySQL).Scan(&data)

	if err == sql.ErrNoRows {
		return QueueMessage{}, false, nil
	} else if err != nil {
		return QueueMessage{}, false, fmt.Errorf("failed to peek message: %w", err)
	}

	// Unmarshal message.
	var msg QueueMessage
	if err := json.Unmarshal([]byte(data), &msg); err != nil {
		return QueueMessage{}, false, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return msg, true, nil
}

func (b *SQLQueueBackend) Size(queueName string) (int, error) {
	// Get table name from cache (lazy loading).
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("failed to get queue table name: %w", err)
	}

	var count int
	querySQL := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s WHERE deleted = %s",
		tableName, b.boolLiteral(false),
	)
	err = b.db.QueryRow(querySQL).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get queue size: %w", err)
	}
	return count, nil
}

func (b *SQLQueueBackend) Clear(queueName string) error {
	// Get table name from cache (lazy loading).
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return nil // Queue doesn't exist, nothing to clear.
	} else if err != nil {
		return fmt.Errorf("failed to get queue table name: %w", err)
	}

	// Clear all messages (both deleted and non-deleted).
	deleteSQL := fmt.Sprintf("DELETE FROM %s", tableName)
	_, err = b.db.Exec(deleteSQL)
	if err != nil {
		return fmt.Errorf("failed to clear queue: %w", err)
	}
	return nil
}

func (b *SQLQueueBackend) ListQueues(prefix string) ([]string, error) {
	// Query from registry table to include all queues.
	var query string
	var args []interface{}

	if prefix == "" {
		query = "SELECT queue_name FROM queuefs_registry"
	} else {
		query = "SELECT queue_name FROM queuefs_registry WHERE queue_name = ? OR queue_name LIKE ?"
		args = []interface{}{prefix, prefix + "/%"}
	}
	query = b.rebind(query)

	rows, err := b.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list queues: %w", err)
	}
	defer rows.Close()

	var queues []string
	for rows.Next() {
		var qName string
		if err := rows.Scan(&qName); err != nil {
			return nil, fmt.Errorf("failed to scan queue name: %w", err)
		}
		queues = append(queues, qName)
	}

	return queues, nil
}

func (b *SQLQueueBackend) GetLastEnqueueTime(queueName string) (time.Time, error) {
	// Get table name from cache (lazy loading).
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return time.Time{}, nil
	} else if err != nil {
		return time.Time{}, fmt.Errorf("failed to get queue table name: %w", err)
	}

	var timestamp sql.NullInt64
	querySQL := fmt.Sprintf(
		"SELECT MAX(timestamp) FROM %s WHERE deleted = %s",
		tableName, b.boolLiteral(false),
	)
	err = b.db.QueryRow(querySQL).Scan(&timestamp)

	if err != nil {
		if err == sql.ErrNoRows {
			return time.Time{}, nil
		}
		return time.Time{}, fmt.Errorf("failed to get last enqueue time: %w", err)
	}
	if !timestamp.Valid || timestamp.Int64 == 0 {
		return time.Time{}, nil
	}

	return time.Unix(timestamp.Int64, 0), nil
}

func (b *SQLQueueBackend) RemoveQueue(queueName string) error {
	if queueName == "" {
		// Remove all queues: drop all queue tables and clear registry.
		rows, err := b.db.Query("SELECT queue_name, table_name FROM queuefs_registry")
		if err != nil {
			return fmt.Errorf("failed to list queues: %w", err)
		}
		defer rows.Close()

		var queuesToDelete []queueDeleteTarget

		for rows.Next() {
			var qName, tName string
			if err := rows.Scan(&qName, &tName); err != nil {
				return fmt.Errorf("failed to scan queue: %w", err)
			}
			queuesToDelete = append(queuesToDelete, queueDeleteTarget{qName, tName})
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("failed to iterate queues: %w", err)
		}

		dropped, dropErr := b.dropQueueTables(queuesToDelete)
		if err := b.deleteRegistryEntries(dropped); err != nil {
			return err
		}
		b.invalidateQueueCaches(dropped)
		return dropErr
	}

	// Remove queue and nested queues.
	rows, err := b.db.Query(
		b.rebind("SELECT queue_name, table_name FROM queuefs_registry WHERE queue_name = ? OR queue_name LIKE ?"),
		queueName, queueName+"/%",
	)
	if err != nil {
		return fmt.Errorf("failed to query queues: %w", err)
	}
	defer rows.Close()

	var queuesToDelete []queueDeleteTarget

	for rows.Next() {
		var qName, tName string
		if err := rows.Scan(&qName, &tName); err != nil {
			return fmt.Errorf("failed to scan queue: %w", err)
		}
		queuesToDelete = append(queuesToDelete, queueDeleteTarget{qName, tName})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate queues: %w", err)
	}

	dropped, dropErr := b.dropQueueTables(queuesToDelete)
	if err := b.deleteRegistryEntries(dropped); err != nil {
		return err
	}
	b.invalidateQueueCaches(dropped)
	return dropErr
}

func (b *SQLQueueBackend) CreateQueue(queueName string) error {
	// Generate table name.
	tableName := sanitizeTableName(queueName)

	// Table/index creation is intentionally idempotent via IF NOT EXISTS.
	// We do not try to clean up partially-created database objects here: concurrent
	// CreateQueue calls for the same queue name may share the same table/index, so
	// a failing caller cannot safely tell whether it owns the objects it is about
	// to drop. DDL auto-commit and locking behavior also differs across SQLite,
	// TiDB/MySQL, and PostgreSQL, which makes rollback-style cleanup unreliable.
	// Instead, if registry insertion fails after DDL succeeds, we allow an orphaned
	// table to remain and rely on a later CreateQueue retry to register it and keep
	// queue creation idempotent from the user's perspective.
	// Create the queue table.
	createTableSQL := b.backend.QueueTableDDL(tableName)
	if _, err := b.db.Exec(createTableSQL); err != nil {
		return fmt.Errorf("failed to create queue table: %w", err)
	}
	if err := b.backend.EnsureQueueIndexes(b.db, tableName); err != nil {
		return err
	}

	// Register in queuefs_registry.
	registerSQL := b.backend.RegistryInsertSQL()
	registerSQL = b.rebind(registerSQL)
	_, err := b.db.Exec(registerSQL, queueName, tableName)
	if err != nil {
		return fmt.Errorf("failed to register queue: %w", err)
	}

	// Update cache.
	b.cacheMu.Lock()
	b.tableCache[queueName] = tableName
	b.cacheMu.Unlock()

	log.Infof("[queuefs] Created queue table '%s' for queue '%s'", tableName, queueName)
	return nil
}

func (b *SQLQueueBackend) QueueExists(queueName string) (bool, error) {
	// Check cache first.
	b.cacheMu.RLock()
	_, exists := b.tableCache[queueName]
	b.cacheMu.RUnlock()

	if exists {
		return true, nil
	}

	// If not in cache, query database.
	var count int
	err := b.db.QueryRow(
		b.rebind("SELECT COUNT(*) FROM queuefs_registry WHERE queue_name = ?"),
		queueName,
	).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check queue existence: %w", err)
	}
	return count > 0, nil
}
