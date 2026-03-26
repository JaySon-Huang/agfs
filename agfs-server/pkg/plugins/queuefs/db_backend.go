package queuefs

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/c4pt0r/agfs/agfs-server/pkg/plugin/config"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
)

// DBBackend defines the database-specific pieces of the durable queue schema.
type DBBackend interface {
	// Open opens a connection to the database.
	Open(cfg map[string]interface{}) (*sql.DB, error)

	// GetInitSQL returns the SQL statements used to initialize shared schema.
	GetInitSQL() []string

	// GetDriverName returns the driver name used by database/sql.
	GetDriverName() string

	// GetCreateQueueTableSQL returns the create-table SQL for one durable queue.
	GetCreateQueueTableSQL(tableName string) string

	// GetQueueColumnDefinitions returns column definitions used for legacy table
	// migration. Keys are column names, values are SQL type/default fragments.
	GetQueueColumnDefinitions() map[string]string

	// GetQueueIndexes returns index definitions keyed by index name.
	GetQueueIndexes(tableName string) map[string]string
}

// SQLiteDBBackend implements DBBackend for SQLite.
type SQLiteDBBackend struct{}

func NewSQLiteDBBackend() *SQLiteDBBackend {
	return &SQLiteDBBackend{}
}

func (b *SQLiteDBBackend) GetDriverName() string {
	return "sqlite3"
}

func (b *SQLiteDBBackend) Open(cfg map[string]interface{}) (*sql.DB, error) {
	dbPath := config.GetStringConfig(cfg, "db_path", "queuefs.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		_ = db.Close()
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

func (b *SQLiteDBBackend) GetCreateQueueTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		message_id TEXT NOT NULL,
		data BLOB NOT NULL,
		timestamp INTEGER NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		status TEXT NOT NULL DEFAULT 'queued',
		attempt_count INTEGER NOT NULL DEFAULT 0,
		max_attempts INTEGER NOT NULL DEFAULT 16,
		leased_by TEXT NULL,
		leased_at DATETIME NULL,
		lease_until DATETIME NULL,
		acked_at DATETIME NULL,
		failed_at DATETIME NULL,
		dead_lettered_at DATETIME NULL,
		last_error TEXT NULL,
		next_retry_at DATETIME NULL,
		dedupe_key TEXT NULL,
		priority INTEGER NOT NULL DEFAULT 0
	)`, quoteIdentifier(tableName))
}

func (b *SQLiteDBBackend) GetQueueColumnDefinitions() map[string]string {
	return map[string]string{
		"status":           "TEXT NOT NULL DEFAULT 'queued'",
		"attempt_count":    "INTEGER NOT NULL DEFAULT 0",
		"max_attempts":     "INTEGER NOT NULL DEFAULT 16",
		"leased_by":        "TEXT NULL",
		"leased_at":        "DATETIME NULL",
		"lease_until":      "DATETIME NULL",
		"acked_at":         "DATETIME NULL",
		"failed_at":        "DATETIME NULL",
		"dead_lettered_at": "DATETIME NULL",
		"last_error":       "TEXT NULL",
		"next_retry_at":    "DATETIME NULL",
		"dedupe_key":       "TEXT NULL",
		"priority":         "INTEGER NOT NULL DEFAULT 0",
	}
}

func (b *SQLiteDBBackend) GetQueueIndexes(tableName string) map[string]string {
	return map[string]string{
		"idx_status_id":       "status, id",
		"idx_status_retry_id": "status, next_retry_at, id",
		"idx_lease_until":     "status, lease_until",
		"idx_dedupe_key":      "dedupe_key",
	}
}

// TiDBDBBackend implements DBBackend for TiDB/MySQL.
type TiDBDBBackend struct{}

func NewTiDBDBBackend() *TiDBDBBackend {
	return &TiDBDBBackend{}
}

func (b *TiDBDBBackend) GetDriverName() string {
	return "mysql"
}

func (b *TiDBDBBackend) Open(cfg map[string]interface{}) (*sql.DB, error) {
	dsnString := config.GetStringConfig(cfg, "dsn", "")
	dsnHasTLS := strings.Contains(dsnString, "tls=")
	enableTLS := config.GetBoolConfig(cfg, "enable_tls", false) || dsnHasTLS
	tlsConfigName := "tidb-queuefs"

	if enableTLS {
		serverName := config.GetStringConfig(cfg, "tls_server_name", "")
		if serverName == "" {
			if dsnString != "" {
				re := regexp.MustCompile(`@tcp\(([^:]+):\d+\)`)
				if matches := re.FindStringSubmatch(dsnString); len(matches) > 1 {
					serverName = matches[1]
				}
			} else {
				serverName = config.GetStringConfig(cfg, "host", "")
			}
		}

		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if serverName != "" {
			tlsConfig.ServerName = serverName
		}
		if config.GetBoolConfig(cfg, "tls_skip_verify", false) {
			tlsConfig.InsecureSkipVerify = true
			log.Warn("[queuefs] TLS certificate verification is disabled (insecure)")
		}

		if err := mysql.RegisterTLSConfig(tlsConfigName, tlsConfig); err != nil {
			log.Warnf("[queuefs] Failed to register TLS config (may already exist): %v", err)
		}
	}

	var dsn string
	if dsnString != "" {
		dsn = dsnString
	} else {
		user := config.GetStringConfig(cfg, "user", "root")
		password := config.GetStringConfig(cfg, "password", "")
		host := config.GetStringConfig(cfg, "host", "127.0.0.1")
		port := config.GetPortConfig(cfg, "port", "4000")
		database := config.GetStringConfig(cfg, "database", "queuedb")

		if password != "" {
			dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True", user, password, host, port, database)
		} else {
			dsn = fmt.Sprintf("%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True", user, host, port, database)
		}

		if enableTLS {
			dsn += fmt.Sprintf("&tls=%s", tlsConfigName)
		}
	}

	log.Infof("[queuefs] Connecting to TiDB/MySQL (TLS: %v)", enableTLS)

	dbName := extractDatabaseName(dsn, config.GetStringConfig(cfg, "database", ""))
	if dbName != "" {
		bootstrapDSN := removeDatabaseFromDSN(dsn)
		if bootstrapDSN != dsn {
			tempDB, err := sql.Open("mysql", bootstrapDSN)
			if err == nil {
				defer tempDB.Close()
				if _, err := tempDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)); err != nil {
					log.Warnf("[queuefs] Failed to create database %s: %v", dbName, err)
				}
			}
		}
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open TiDB/MySQL database: %w", err)
	}

	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping TiDB/MySQL database: %w", err)
	}

	return db, nil
}

func (b *TiDBDBBackend) GetInitSQL() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS queuefs_registry (
			queue_name VARCHAR(255) PRIMARY KEY,
			table_name VARCHAR(255) NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	}
}

func (b *TiDBDBBackend) GetCreateQueueTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		message_id VARCHAR(64) NOT NULL,
		data LONGBLOB NOT NULL,
		timestamp BIGINT NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		status VARCHAR(32) NOT NULL DEFAULT 'queued',
		attempt_count INT NOT NULL DEFAULT 0,
		max_attempts INT NOT NULL DEFAULT 16,
		leased_by VARCHAR(255) NULL,
		leased_at TIMESTAMP NULL,
		lease_until TIMESTAMP NULL,
		acked_at TIMESTAMP NULL,
		failed_at TIMESTAMP NULL,
		dead_lettered_at TIMESTAMP NULL,
		last_error TEXT NULL,
		next_retry_at TIMESTAMP NULL,
		dedupe_key VARCHAR(255) NULL,
		priority INT NOT NULL DEFAULT 0,
		INDEX idx_status_id (status, id),
		INDEX idx_status_retry_id (status, next_retry_at, id),
		INDEX idx_lease_until (status, lease_until),
		INDEX idx_dedupe_key (dedupe_key)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, quoteIdentifier(tableName))
}

func (b *TiDBDBBackend) GetQueueColumnDefinitions() map[string]string {
	return map[string]string{
		"status":           "VARCHAR(32) NOT NULL DEFAULT 'queued'",
		"attempt_count":    "INT NOT NULL DEFAULT 0",
		"max_attempts":     "INT NOT NULL DEFAULT 16",
		"leased_by":        "VARCHAR(255) NULL",
		"leased_at":        "TIMESTAMP NULL",
		"lease_until":      "TIMESTAMP NULL",
		"acked_at":         "TIMESTAMP NULL",
		"failed_at":        "TIMESTAMP NULL",
		"dead_lettered_at": "TIMESTAMP NULL",
		"last_error":       "TEXT NULL",
		"next_retry_at":    "TIMESTAMP NULL",
		"dedupe_key":       "VARCHAR(255) NULL",
		"priority":         "INT NOT NULL DEFAULT 0",
	}
}

func (b *TiDBDBBackend) GetQueueIndexes(tableName string) map[string]string {
	return map[string]string{
		"idx_status_id":       "status, id",
		"idx_status_retry_id": "status, next_retry_at, id",
		"idx_lease_until":     "status, lease_until",
		"idx_dedupe_key":      "dedupe_key",
	}
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
func sanitizeTableName(queueName string) string {
	tableName := strings.ReplaceAll(queueName, "/", "_")
	tableName = strings.ReplaceAll(tableName, "-", "_")
	tableName = strings.ReplaceAll(tableName, ".", "_")
	return "queuefs_queue_" + tableName
}

// CreateBackend creates the appropriate database backend.
func CreateBackend(cfg map[string]interface{}) (DBBackend, error) {
	switch config.GetStringConfig(cfg, "backend", "memory") {
	case "sqlite", "sqlite3":
		return NewSQLiteDBBackend(), nil
	case "tidb", "mysql":
		return NewTiDBDBBackend(), nil
	default:
		return nil, fmt.Errorf("unsupported database backend: %s", config.GetStringConfig(cfg, "backend", "memory"))
	}
}
