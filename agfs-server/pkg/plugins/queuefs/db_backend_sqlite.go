package queuefs

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/c4pt0r/agfs/agfs-server/pkg/plugin/config"
	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// SQLiteDBBackend implements DBBackend for SQLite.
type SQLiteDBBackend struct{}

func NewSQLiteDBBackend() *SQLiteDBBackend {
	return &SQLiteDBBackend{}
}

func (b *SQLiteDBBackend) Open(cfg map[string]interface{}) (*sql.DB, error) {
	dbPath := config.GetStringConfig(cfg, "db_path", "queue.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	// Enable WAL mode for better concurrency.
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
	}

	return db, nil
}

func (b *SQLiteDBBackend) GetInitSQL() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS queuefs_registry (
			queue_name TEXT PRIMARY KEY,
			table_name TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
	}
}

func (b *SQLiteDBBackend) SupportsSkipLocked() bool {
	return false
}

func (b *SQLiteDBBackend) QueueTableDDL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		message_id TEXT NOT NULL,
		data BLOB NOT NULL,
		timestamp INTEGER NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		deleted INTEGER DEFAULT 0,
		deleted_at DATETIME NULL
	)`, tableName)
}

func (b *SQLiteDBBackend) EnsureQueueIndexes(db *sql.DB, tableName string) error {
	indexSQL := fmt.Sprintf(
		"CREATE INDEX IF NOT EXISTS idx_%s_deleted_id ON %s(deleted, id)",
		strings.TrimPrefix(tableName, "queuefs_queue_"),
		tableName,
	)
	_, err := db.Exec(indexSQL)
	if err != nil {
		return fmt.Errorf("failed to create queue index: %w", err)
	}
	return nil
}

func (b *SQLiteDBBackend) RegistryInsertSQL() string {
	return "INSERT OR IGNORE INTO queuefs_registry (queue_name, table_name) VALUES (?, ?)"
}

func (b *SQLiteDBBackend) Rebind(query string) string {
	return query
}

func (b *SQLiteDBBackend) BoolLiteral(value bool) string {
	if value {
		return "1"
	}
	return "0"
}
