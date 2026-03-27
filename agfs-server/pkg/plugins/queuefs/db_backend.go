package queuefs

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/c4pt0r/agfs/agfs-server/pkg/plugin/config"
)

// DBBackend defines the interface for database operations.
type DBBackend interface {
	// Open opens a connection to the database.
	Open(cfg map[string]interface{}) (*sql.DB, error)

	// GetInitSQL returns the SQL statements to initialize the schema.
	GetInitSQL() []string

	// SupportsSkipLocked reports whether the backend supports FOR UPDATE SKIP LOCKED.
	SupportsSkipLocked() bool

	// QueueTableDDL returns the SQL to create a queue table.
	QueueTableDDL(tableName string) string

	// EnsureQueueIndexes creates any backend-specific queue indexes.
	EnsureQueueIndexes(db *sql.DB, tableName string) error

	// RegistryInsertSQL returns the SQL used to register a queue.
	RegistryInsertSQL() string

	// Rebind rewrites generic placeholders to the backend-specific format.
	Rebind(query string) string

	// BoolLiteral returns the backend-specific SQL literal for a boolean value.
	BoolLiteral(value bool) string
}

func extractDatabaseName(dsn string, configDB string) string {
	if dsn != "" {
		re := regexp.MustCompile(`\)/([^?]+)`)
		if matches := re.FindStringSubmatch(dsn); len(matches) > 1 {
			return matches[1]
		}
	}
	return configDB
}

func removeDatabaseFromDSN(dsn string) string {
	re := regexp.MustCompile(`\)/[^?]+(\?|$)`)
	return re.ReplaceAllString(dsn, ")/$1")
}

// sanitizeTableName converts a queue name to a safe table name.
// Replaces / with _ and ensures the name is safe for SQL.
// FIXME: This normalization is not collision-free. Distinct queue names such as
// `a/b`, `a-b`, `a.b`, and `a_b` can collapse to the same physical table name,
// which breaks the intended one-queue-to-one-table mapping recorded in
// queuefs_registry.
func sanitizeTableName(queueName string) string {
	// Replace forward slashes with underscores.
	tableName := strings.ReplaceAll(queueName, "/", "_")

	// Replace any other potentially problematic characters.
	tableName = strings.ReplaceAll(tableName, "-", "_")
	tableName = strings.ReplaceAll(tableName, ".", "_")

	// Prefix with queuefs_queue_ to avoid conflicts with system tables.
	return "queuefs_queue_" + tableName
}

func quotePostgresIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

// CreateBackend creates the appropriate database backend.
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
