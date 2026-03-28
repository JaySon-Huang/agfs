package sqlqueue

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	model "github.com/c4pt0r/agfs/agfs-server/pkg/plugins/queuefs/internal"
	"github.com/google/uuid"
	"github.com/pingcap/failpoint"
	log "github.com/sirupsen/logrus"
)

type QueueMessage = model.QueueMessage

const defaultLeaseDuration = 30 * time.Second

// Backend implements QueueBackend using a SQL database.
// queuefs_registry is the logical source of truth for queue existence and
// queue-to-table mapping; physical queue tables are addressed only through
// registry/cache lookups rather than by scanning database objects directly.
// TODO: tableCache is still treated as a local fast path in some code paths.
// That can make cross-instance idempotent delete/recreate flows observe stale
// queue metadata until the cache is refreshed from queuefs_registry.
type Backend struct {
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

// newBackend wires a SQL queue backend to an optional concrete DBBackend.
func newBackend(backendType string, backend DBBackend) *Backend {
	return &Backend{
		backendType: backendType,
		backend:     backend,
		tableCache:  make(map[string]string),
	}
}

// NewBackend returns a SQL-backed queue backend that resolves the concrete
// database implementation from configuration at initialization time.
func NewBackend() *Backend {
	return newBackend("", nil)
}

// NewSQLiteBackend returns a queue backend pinned to the SQLite implementation.
func NewSQLiteBackend() *Backend {
	return newBackend("sqlite", NewSQLiteDBBackend())
}

// NewTiDBBackend returns a queue backend pinned to the TiDB implementation.
func NewTiDBBackend() *Backend {
	return newBackend("tidb", NewTiDBDBBackend())
}

// NewPostgresBackend returns a queue backend pinned to the PostgreSQL implementation.
func NewPostgresBackend() *Backend {
	return newBackend("pgsql", NewPostgreSQLDBBackend())
}

func (b *Backend) Initialize(config map[string]interface{}) error {
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

	backend := b.backend
	if backend == nil {
		var err error
		backend, err = CreateBackend(config)
		if err != nil {
			return fmt.Errorf("failed to create backend: %w", err)
		}
	}
	b.backend = backend

	db, err := backend.Open(config)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	b.db = db

	for _, sqlStmt := range backend.GetInitSQL() {
		if _, err := db.Exec(sqlStmt); err != nil {
			db.Close()
			return fmt.Errorf("failed to initialize schema: %w", err)
		}
	}

	return nil
}

func (b *Backend) Close() error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

func (b *Backend) GetType() string {
	return b.backendType
}

func (b *Backend) rebind(query string) string {
	if b.backend == nil {
		return query
	}
	return b.backend.Rebind(query)
}

func (b *Backend) boolLiteral(value bool) string {
	if b.backend == nil {
		if value {
			return "1"
		}
		return "0"
	}
	return b.backend.BoolLiteral(value)
}

func (b *Backend) pendingPredicate() string {
	// Durable state is encoded inline on the queue row: pending rows have not
	// been deleted and do not yet carry an active receipt.
	return fmt.Sprintf("deleted = %s AND receipt IS NULL", b.boolLiteral(false))
}

func (b *Backend) processingPredicate() string {
	// Claimed rows stay in the same table and flip into processing state by
	// attaching a receipt/lease instead of moving to a side table.
	return fmt.Sprintf("deleted = %s AND receipt IS NOT NULL", b.boolLiteral(false))
}

func normalizeLeaseDuration(req model.ClaimRequest) time.Duration {
	if req.LeaseDuration <= 0 {
		return defaultLeaseDuration
	}
	return req.LeaseDuration
}

func decodeStoredMessage(raw string) (QueueMessage, error) {
	var msg QueueMessage
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		return QueueMessage{}, fmt.Errorf("failed to unmarshal message: %w", err)
	}
	return msg, nil
}

func (b *Backend) getTableName(queueName string, forceRefresh bool) (string, error) {
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
		b.rebind("SELECT table_name FROM queuefs_registry WHERE queue_name = ?"),
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

func (b *Backend) invalidateCache(queueName string) {
	b.cacheMu.Lock()
	delete(b.tableCache, queueName)
	b.cacheMu.Unlock()
}

func (b *Backend) dropQueueTables(targets []queueDeleteTarget) ([]queueDeleteTarget, error) {
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

func (b *Backend) deleteRegistryEntries(targets []queueDeleteTarget) error {
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

func (b *Backend) invalidateQueueCaches(targets []queueDeleteTarget) {
	for _, q := range targets {
		b.invalidateCache(q.queueName)
	}
}

func (b *Backend) Enqueue(queueName string, msg QueueMessage) error {
	msgData, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return fmt.Errorf("queue does not exist: %s (create it with mkdir first)", queueName)
	} else if err != nil {
		return fmt.Errorf("failed to get queue table name: %w", err)
	}

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

func (b *Backend) Dequeue(queueName string) (QueueMessage, bool, error) {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return QueueMessage{}, false, nil
	} else if err != nil {
		return QueueMessage{}, false, fmt.Errorf("failed to get queue table name: %w", err)
	}

	tx, err := b.db.Begin()
	if err != nil {
		return QueueMessage{}, false, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	var id int64
	var data string

	querySQL := fmt.Sprintf(
		"SELECT id, data FROM %s WHERE %s ORDER BY id LIMIT 1",
		tableName, b.pendingPredicate(),
	)
	if b.backend != nil && b.backend.SupportsSkipLocked() {
		querySQL += " FOR UPDATE SKIP LOCKED"
	}
	querySQL = b.rebind(querySQL)
	err = tx.QueryRow(querySQL).Scan(&id, &data)

	if err == sql.ErrNoRows {
		return QueueMessage{}, false, nil
	} else if err != nil {
		return QueueMessage{}, false, fmt.Errorf("failed to query message: %w", err)
	}

	updateSQL := fmt.Sprintf(
		"UPDATE %s SET deleted = %s, deleted_at = CURRENT_TIMESTAMP WHERE id = ? AND %s",
		tableName, b.boolLiteral(true), b.pendingPredicate(),
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

	if err := tx.Commit(); err != nil {
		return QueueMessage{}, false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	msg, err := decodeStoredMessage(data)
	if err != nil {
		return QueueMessage{}, false, err
	}

	return msg, true, nil
}

func (b *Backend) Peek(queueName string) (QueueMessage, bool, error) {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return QueueMessage{}, false, nil
	} else if err != nil {
		return QueueMessage{}, false, fmt.Errorf("failed to get queue table name: %w", err)
	}

	var data string
	querySQL := fmt.Sprintf(
		"SELECT data FROM %s WHERE %s ORDER BY id LIMIT 1",
		tableName, b.pendingPredicate(),
	)
	err = b.db.QueryRow(querySQL).Scan(&data)

	if err == sql.ErrNoRows {
		return QueueMessage{}, false, nil
	} else if err != nil {
		return QueueMessage{}, false, fmt.Errorf("failed to peek message: %w", err)
	}

	msg, err := decodeStoredMessage(data)
	if err != nil {
		return QueueMessage{}, false, err
	}

	return msg, true, nil
}

func (b *Backend) Size(queueName string) (int, error) {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("failed to get queue table name: %w", err)
	}

	var count int
	querySQL := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s WHERE %s",
		tableName, b.pendingPredicate(),
	)
	err = b.db.QueryRow(querySQL).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get queue size: %w", err)
	}
	return count, nil
}

func (b *Backend) Clear(queueName string) error {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to get queue table name: %w", err)
	}

	deleteSQL := fmt.Sprintf("DELETE FROM %s", tableName)
	_, err = b.db.Exec(deleteSQL)
	if err != nil {
		return fmt.Errorf("failed to clear queue: %w", err)
	}
	return nil
}

func (b *Backend) ListQueues(prefix string) ([]string, error) {
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate queue rows: %w", err)
	}

	return queues, nil
}

func (b *Backend) GetLastEnqueueTime(queueName string) (time.Time, error) {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return time.Time{}, nil
	} else if err != nil {
		return time.Time{}, fmt.Errorf("failed to get queue table name: %w", err)
	}

	var timestamp sql.NullInt64
	querySQL := fmt.Sprintf(
		"SELECT MAX(timestamp) FROM %s WHERE %s",
		tableName, b.pendingPredicate(),
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

func (b *Backend) RemoveQueue(queueName string) error {
	if queueName == "" {
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

func (b *Backend) CreateQueue(queueName string) error {
	tableName := sanitizeTableName(queueName)

	createTableSQL := b.backend.QueueTableDDL(tableName)
	if _, err := b.db.Exec(createTableSQL); err != nil {
		return fmt.Errorf("failed to create queue table: %w", err)
	}
	if err := b.backend.EnsureQueueIndexes(b.db, tableName); err != nil {
		return err
	}

	registerSQL := b.backend.RegistryInsertSQL()
	registerSQL = b.rebind(registerSQL)
	_, err := b.db.Exec(registerSQL, queueName, tableName)
	if err != nil {
		return fmt.Errorf("failed to register queue: %w", err)
	}

	b.cacheMu.Lock()
	b.tableCache[queueName] = tableName
	b.cacheMu.Unlock()

	log.Infof("[queuefs] Created queue table '%s' for queue '%s'", tableName, queueName)
	return nil
}

func (b *Backend) QueueExists(queueName string) (bool, error) {
	b.cacheMu.RLock()
	_, exists := b.tableCache[queueName]
	b.cacheMu.RUnlock()

	if exists {
		return true, nil
	}

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

func (b *Backend) Claim(queueName string, req model.ClaimRequest) (model.ClaimedMessage, bool, error) {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return model.ClaimedMessage{}, false, nil
	} else if err != nil {
		return model.ClaimedMessage{}, false, fmt.Errorf("failed to get queue table name: %w", err)
	}

	tx, err := b.db.Begin()
	if err != nil {
		return model.ClaimedMessage{}, false, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	var id int64
	var messageID string
	var data string
	var attempt int
	querySQL := fmt.Sprintf(
		"SELECT id, message_id, data, attempt FROM %s WHERE %s ORDER BY id LIMIT 1",
		tableName, b.pendingPredicate(),
	)
	if b.backend != nil && b.backend.SupportsSkipLocked() {
		// Prefer SKIP LOCKED when available so concurrent claimers naturally spread
		// across different rows instead of blocking one another.
		querySQL += " FOR UPDATE SKIP LOCKED"
	}
	querySQL = b.rebind(querySQL)
	if err := tx.QueryRow(querySQL).Scan(&id, &messageID, &data, &attempt); err != nil {
		if err == sql.ErrNoRows {
			return model.ClaimedMessage{}, false, nil
		}
		return model.ClaimedMessage{}, false, fmt.Errorf("failed to query claimable message: %w", err)
	}

	message, err := decodeStoredMessage(data)
	if err != nil {
		return model.ClaimedMessage{}, false, err
	}

	claimedAt := time.Now().UTC()
	leaseUntil := claimedAt.Add(normalizeLeaseDuration(req))
	receipt := uuid.NewString()
	updateSQL := fmt.Sprintf(
		"UPDATE %s SET receipt = ?, claimed_at = ?, lease_until = ?, attempt = ? WHERE id = ? AND %s",
		tableName, b.pendingPredicate(),
	)
	updateSQL = b.rebind(updateSQL)
	// The claim is finalized by a conditional row update in the same transaction.
	// If another worker wins the race first, RowsAffected becomes zero and the
	// caller simply observes an empty claim result.
	result, err := tx.Exec(updateSQL, receipt, claimedAt.Unix(), leaseUntil.Unix(), attempt+1, id)
	if err != nil {
		return model.ClaimedMessage{}, false, fmt.Errorf("failed to claim message: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return model.ClaimedMessage{}, false, fmt.Errorf("failed to verify claim result: %w", err)
	}
	if rowsAffected == 0 {
		return model.ClaimedMessage{}, false, nil
	}
	if err := tx.Commit(); err != nil {
		return model.ClaimedMessage{}, false, fmt.Errorf("failed to commit claim: %w", err)
	}

	return model.ClaimedMessage{
		MessageID:  messageID,
		QueueName:  queueName,
		Data:       message.Data,
		Receipt:    receipt,
		ClaimedAt:  claimedAt,
		LeaseUntil: leaseUntil,
		Attempt:    attempt + 1,
	}, true, nil
}

func (b *Backend) Ack(queueName string, messageID string, receipt string) error {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return fmt.Errorf("queue does not exist: %s", queueName)
	} else if err != nil {
		return fmt.Errorf("failed to get queue table name: %w", err)
	}

	updateSQL := fmt.Sprintf(
		"UPDATE %s SET deleted = %s, deleted_at = CURRENT_TIMESTAMP, receipt = NULL, claimed_at = NULL, lease_until = NULL WHERE message_id = ? AND receipt = ? AND %s",
		tableName, b.boolLiteral(true), b.processingPredicate(),
	)
	updateSQL = b.rebind(updateSQL)
	result, err := b.db.Exec(updateSQL, messageID, receipt)
	if err != nil {
		return fmt.Errorf("failed to ack message: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to verify ack result: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("message %q is not currently claimed with the provided receipt", messageID)
	}
	return nil
}

func (b *Backend) Release(queueName string, messageID string, req model.ReleaseRequest) error {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return fmt.Errorf("queue does not exist: %s", queueName)
	} else if err != nil {
		return fmt.Errorf("failed to get queue table name: %w", err)
	}

	updateSQL := fmt.Sprintf(
		"UPDATE %s SET receipt = NULL, claimed_at = NULL, lease_until = NULL WHERE message_id = ? AND receipt = ? AND %s",
		tableName, b.processingPredicate(),
	)
	updateSQL = b.rebind(updateSQL)
	result, err := b.db.Exec(updateSQL, messageID, req.Receipt)
	if err != nil {
		return fmt.Errorf("failed to release message: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to verify release result: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("message %q is not currently claimed with the provided receipt", messageID)
	}
	return nil
}

func (b *Backend) RecoverExpired(queueName string, now time.Time, limit int) (int, error) {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("failed to get queue table name: %w", err)
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	tx, err := b.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	querySQL := fmt.Sprintf(
		"SELECT message_id FROM %s WHERE %s AND lease_until IS NOT NULL AND lease_until <= ? ORDER BY id",
		tableName, b.processingPredicate(),
	)
	args := []interface{}{now.Unix()}
	if limit > 0 {
		querySQL += " LIMIT ?"
		args = append(args, limit)
	}
	if b.backend != nil && b.backend.SupportsSkipLocked() {
		// Keep recovery non-blocking under concurrent claimers by skipping rows that
		// another session already locked for claim/recovery work.
		querySQL += " FOR UPDATE SKIP LOCKED"
	}
	querySQL = b.rebind(querySQL)
	rows, err := tx.Query(querySQL, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to query expired claims: %w", err)
	}
	defer rows.Close()

	var messageIDs []string
	for rows.Next() {
		var messageID string
		if err := rows.Scan(&messageID); err != nil {
			return 0, fmt.Errorf("failed to scan expired claim: %w", err)
		}
		messageIDs = append(messageIDs, messageID)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("failed to iterate expired claims: %w", err)
	}

	updateSQL := fmt.Sprintf(
		"UPDATE %s SET receipt = NULL, claimed_at = NULL, lease_until = NULL, recovery_count = recovery_count + 1 WHERE message_id = ? AND %s AND lease_until IS NOT NULL AND lease_until <= ?",
		tableName, b.processingPredicate(),
	)
	updateSQL = b.rebind(updateSQL)
	recovered := 0
	// Apply recovery row by row so the same code path works across SQLite,
	// PostgreSQL, and TiDB without relying on dialect-specific bulk UPDATE syntax.
	// TODO(queuefs): revisit this if recovery throughput becomes a bottleneck.
	// A batched UPDATE may be faster, but it needs dialect-specific handling and
	// careful verification across SQLite, PostgreSQL, TiDB, and future MySQL support.
	for _, messageID := range messageIDs {
		result, err := tx.Exec(updateSQL, messageID, now.Unix())
		if err != nil {
			return 0, fmt.Errorf("failed to recover message %q: %w", messageID, err)
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("failed to verify recovery result: %w", err)
		}
		recovered += int(rowsAffected)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit recovery: %w", err)
	}
	return recovered, nil
}

func (b *Backend) Stats(queueName string) (model.QueueStats, error) {
	tableName, err := b.getTableName(queueName, false)
	if err == sql.ErrNoRows {
		return model.QueueStats{}, nil
	} else if err != nil {
		return model.QueueStats{}, fmt.Errorf("failed to get queue table name: %w", err)
	}

	var stats model.QueueStats
	queries := []struct {
		query  string
		target interface{}
	}{
		{query: fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tableName, b.pendingPredicate()), target: &stats.Pending},
		{query: fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tableName, b.processingPredicate()), target: &stats.Processing},
	}
	for _, item := range queries {
		if err := b.db.QueryRow(item.query).Scan(item.target); err != nil {
			return model.QueueStats{}, fmt.Errorf("failed to get queue stats: %w", err)
		}
	}

	var recoveries sql.NullInt64
	// Recoveries is defined as a queue-level historical counter, so acked rows
	// continue contributing their recovery_count after they leave the live set.
	recoveriesQuery := fmt.Sprintf("SELECT COALESCE(SUM(recovery_count), 0) FROM %s", tableName)
	if err := b.db.QueryRow(recoveriesQuery).Scan(&recoveries); err != nil {
		return model.QueueStats{}, fmt.Errorf("failed to get recovery stats: %w", err)
	}
	if recoveries.Valid {
		stats.Recoveries = int(recoveries.Int64)
	}
	return stats, nil
}
