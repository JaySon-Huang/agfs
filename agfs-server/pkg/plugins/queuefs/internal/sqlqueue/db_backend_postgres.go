package sqlqueue

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/c4pt0r/agfs/agfs-server/pkg/plugin/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	log "github.com/sirupsen/logrus"
)

type PostgreSQLDBBackend struct{}

func NewPostgreSQLDBBackend() *PostgreSQLDBBackend { return &PostgreSQLDBBackend{} }

func buildPostgresSSLMode(cfg map[string]interface{}) string {
	if !config.GetBoolConfig(cfg, "enable_tls", false) {
		return "disable"
	}
	if config.GetBoolConfig(cfg, "tls_skip_verify", false) {
		return "require"
	}
	return "verify-full"
}

func applyPostgresTLSOverrides(connConfig *pgx.ConnConfig, cfg map[string]interface{}) {
	if connConfig == nil || connConfig.TLSConfig == nil {
		return
	}
	if serverName := config.GetStringConfig(cfg, "tls_server_name", ""); serverName != "" {
		connConfig.TLSConfig.ServerName = serverName
	}
	if config.GetBoolConfig(cfg, "tls_skip_verify", false) {
		log.Warn("PostgreSQL backend: TLS certificate verification is disabled (tls_skip_verify=true); this is insecure and should not be used in production")
		connConfig.TLSConfig.InsecureSkipVerify = true
	}
}

func registerPostgresConnString(dsn string, cfg map[string]interface{}) (string, *pgx.ConnConfig, error) {
	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return "", nil, err
	}
	applyPostgresTLSOverrides(connConfig, cfg)
	// TODO: Replace RegisterConnConfig with a connector-based OpenDB path.
	// This helper is called from Open(), and the registered configs currently
	// stay in pgx's global registry for the lifetime of the process.
	return stdlib.RegisterConnConfig(connConfig), connConfig, nil
}

func (b *PostgreSQLDBBackend) Open(cfg map[string]interface{}) (*sql.DB, error) {
	dsn := config.GetStringConfig(cfg, "dsn", "")
	database := config.GetStringConfig(cfg, "database", "")
	if dsn == "" {
		if database == "" {
			database = "queuedb"
		}
		host := config.GetStringConfig(cfg, "host", "127.0.0.1")
		port := config.GetStringConfig(cfg, "port", "5432")
		user := config.GetStringConfig(cfg, "user", "postgres")
		password := config.GetStringConfig(cfg, "password", "")
		sslMode := buildPostgresSSLMode(cfg)
		dsn = fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=%s", host, port, user, database, sslMode)
		if password != "" {
			dsn += fmt.Sprintf(" password=%s", password)
		}
	}

	targetDSN, targetConnConfig, err := registerPostgresConnString(dsn, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PostgreSQL DSN: %w", err)
	}
	if database == "" {
		database = targetConnConfig.Database
	}
	if database == "" {
		database = "queuedb"
		targetConnConfig.Database = database
		targetDSN = stdlib.RegisterConnConfig(targetConnConfig)
	}

	adminDSN := config.GetStringConfig(cfg, "admin_dsn", "")
	if adminDSN == "" && database != "" {
		adminConnConfig := *targetConnConfig
		adminConnConfig.Database = "postgres"
		adminDSN = stdlib.RegisterConnConfig(&adminConnConfig)
	} else if adminDSN != "" {
		registeredAdminDSN, _, err := registerPostgresConnString(adminDSN, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to parse PostgreSQL admin DSN: %w", err)
		}
		adminDSN = registeredAdminDSN
	}

	if adminDSN != "" && database != "" {
		tempDB, err := sql.Open("pgx", adminDSN)
		if err == nil {
			defer tempDB.Close()
			_, err = tempDB.Exec(fmt.Sprintf("CREATE DATABASE %s", quotePostgresIdentifier(database)))
			if err != nil && !strings.Contains(err.Error(), "already exists") {
				log.Warnf("[queuefs] Failed to create PostgreSQL database %q: %v", database, err)
			} else {
				log.Infof("[queuefs] PostgreSQL database %q created or already exists", database)
			}
		}
	}

	db, err := sql.Open("pgx", targetDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open PostgreSQL database: %w", err)
	}
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping PostgreSQL database: %w", err)
	}
	return db, nil
}

func (b *PostgreSQLDBBackend) GetInitSQL() []string {
	return []string{`CREATE TABLE IF NOT EXISTS queuefs_registry (
			queue_name TEXT PRIMARY KEY,
			table_name TEXT NOT NULL,
			created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
		)`}
}

func (b *PostgreSQLDBBackend) SupportsSkipLocked() bool { return true }

func (b *PostgreSQLDBBackend) QueueTableDDL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGSERIAL PRIMARY KEY,
		message_id TEXT NOT NULL,
		data BYTEA NOT NULL,
		timestamp BIGINT NOT NULL,
		created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
		deleted BOOLEAN DEFAULT FALSE,
		deleted_at TIMESTAMPTZ NULL
	)`, tableName)
}

func (b *PostgreSQLDBBackend) EnsureQueueIndexes(db *sql.DB, tableName string) error {
	indexName := fmt.Sprintf("idx_%s_deleted_id", strings.TrimPrefix(tableName, "queuefs_queue_"))
	indexSQL := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s(deleted, id)", quotePostgresIdentifier(indexName), tableName)
	_, err := db.Exec(indexSQL)
	if err != nil {
		return fmt.Errorf("failed to create queue index: %w", err)
	}
	return nil
}

func (b *PostgreSQLDBBackend) RegistryInsertSQL() string {
	return `INSERT INTO queuefs_registry (queue_name, table_name) VALUES (?, ?) ON CONFLICT (queue_name) DO NOTHING`
}

func (b *PostgreSQLDBBackend) Rebind(query string) string {
	var builder strings.Builder
	builder.Grow(len(query) + 8)
	argIndex := 1
	for _, ch := range query {
		if ch == '?' {
			builder.WriteByte('$')
			builder.WriteString(strconv.Itoa(argIndex))
			argIndex++
			continue
		}
		builder.WriteRune(ch)
	}
	return builder.String()
}

func (b *PostgreSQLDBBackend) BoolLiteral(value bool) string {
	if value {
		return "TRUE"
	}
	return "FALSE"
}
