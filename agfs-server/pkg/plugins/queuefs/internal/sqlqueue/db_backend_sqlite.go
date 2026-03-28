package sqlqueue

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/c4pt0r/agfs/agfs-server/pkg/plugin/config"
	_ "github.com/mattn/go-sqlite3"
)

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
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
	}
	return db, nil
}

func (b *SQLiteDBBackend) GetInitSQL() []string {
	return []string{`CREATE TABLE IF NOT EXISTS queuefs_registry (
			queue_name TEXT PRIMARY KEY,
			table_name TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`}
}

func (b *SQLiteDBBackend) SupportsSkipLocked() bool { return false }

func (b *SQLiteDBBackend) QueueTableDDL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		message_id TEXT NOT NULL,
		data BLOB NOT NULL,
		timestamp INTEGER NOT NULL,
		attempt INTEGER NOT NULL DEFAULT 0,
		recovery_count INTEGER NOT NULL DEFAULT 0,
		receipt TEXT NULL,
		claimed_at INTEGER NULL,
		lease_until INTEGER NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		deleted INTEGER DEFAULT 0,
		deleted_at DATETIME NULL
	)`, tableName)
}

func (b *SQLiteDBBackend) EnsureQueueIndexes(db *sql.DB, tableName string) error {
	if err := ensureSQLiteDurableColumns(db, tableName); err != nil {
		return err
	}
	baseName := strings.TrimPrefix(tableName, "queuefs_queue_")
	indexSQLs := []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_deleted_receipt_id ON %s(deleted, receipt, id)", baseName, tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_deleted_lease_until ON %s(deleted, lease_until)", baseName, tableName),
	}
	for _, indexSQL := range indexSQLs {
		if _, err := db.Exec(indexSQL); err != nil {
			return fmt.Errorf("failed to create queue index: %w", err)
		}
	}
	return nil
}

func (b *SQLiteDBBackend) RegistryInsertSQL() string {
	return "INSERT OR IGNORE INTO queuefs_registry (queue_name, table_name) VALUES (?, ?)"
}
func (b *SQLiteDBBackend) Rebind(query string) string { return query }
func (b *SQLiteDBBackend) BoolLiteral(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func ensureSQLiteDurableColumns(db *sql.DB, tableName string) error {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return fmt.Errorf("failed to inspect queue table schema: %w", err)
	}
	defer rows.Close()

	columns := map[string]bool{}
	for rows.Next() {
		var cid int
		var name string
		var columnType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			return fmt.Errorf("failed to scan queue table schema: %w", err)
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate queue table schema: %w", err)
	}

	addColumns := []struct {
		name string
		ddl  string
	}{
		{name: "attempt", ddl: "ALTER TABLE %s ADD COLUMN attempt INTEGER NOT NULL DEFAULT 0"},
		{name: "recovery_count", ddl: "ALTER TABLE %s ADD COLUMN recovery_count INTEGER NOT NULL DEFAULT 0"},
		{name: "receipt", ddl: "ALTER TABLE %s ADD COLUMN receipt TEXT NULL"},
		{name: "claimed_at", ddl: "ALTER TABLE %s ADD COLUMN claimed_at INTEGER NULL"},
		{name: "lease_until", ddl: "ALTER TABLE %s ADD COLUMN lease_until INTEGER NULL"},
	}
	for _, column := range addColumns {
		if columns[column.name] {
			continue
		}
		if _, err := db.Exec(fmt.Sprintf(column.ddl, tableName)); err != nil {
			return fmt.Errorf("failed to add queue column %q: %w", column.name, err)
		}
	}
	return nil
}
