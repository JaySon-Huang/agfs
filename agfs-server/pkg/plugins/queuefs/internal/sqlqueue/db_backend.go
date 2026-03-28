package sqlqueue

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/c4pt0r/agfs/agfs-server/pkg/plugin/config"
)

// DBBackend defines the interface for database operations.
type DBBackend interface {
	Open(cfg map[string]interface{}) (*sql.DB, error)
	GetInitSQL() []string
	SupportsSkipLocked() bool
	QueueTableDDL(tableName string) string
	EnsureQueueIndexes(db *sql.DB, tableName string) error
	RegistryInsertSQL() string
	Rebind(query string) string
	BoolLiteral(value bool) string
}

// sanitizeTableName converts a queue name to a safe table name.
// Replaces / with _ and ensures the name is safe for SQL.
// FIXME: This normalization is not collision-free. Distinct queue names such as
// `a/b`, `a-b`, `a.b`, and `a_b` can collapse to the same physical table name,
// which breaks the intended one-queue-to-one-table mapping recorded in
// queuefs_registry.
func sanitizeTableName(queueName string) string {
	tableName := strings.ReplaceAll(queueName, "/", "_")
	tableName = strings.ReplaceAll(tableName, "-", "_")
	tableName = strings.ReplaceAll(tableName, ".", "_")
	return "queuefs_queue_" + tableName
}

func quotePostgresIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func CreateBackend(cfg map[string]interface{}) (DBBackend, error) {
	backendType := config.GetStringConfig(cfg, "backend", "memory")

	switch backendType {
	case "sqlite", "sqlite3":
		return NewSQLiteDBBackend(), nil
	case "tidb", "mysql":
		return NewTiDBDBBackend(), nil
	case "pgsql", "postgres", "postgresql":
		return NewPostgreSQLDBBackend(), nil
	default:
		return nil, fmt.Errorf("unsupported database backend: %s", backendType)
	}
}
