package sqlqueue

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/c4pt0r/agfs/agfs-server/pkg/plugin/config"
	"github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

// TiDBDBBackend adapts queuefs SQL operations to TiDB/MySQL-compatible syntax.
type TiDBDBBackend struct{}

// NewTiDBDBBackend returns a TiDB dialect adapter for queuefs.
func NewTiDBDBBackend() *TiDBDBBackend { return &TiDBDBBackend{} }

func (b *TiDBDBBackend) Open(cfg map[string]interface{}) (*sql.DB, error) {
	dsnStr := config.GetStringConfig(cfg, "dsn", "")
	database := config.GetStringConfig(cfg, "database", "")
	dsnHasTLS := strings.Contains(dsnStr, "tls=")
	enableTLS := config.GetBoolConfig(cfg, "enable_tls", false) || dsnHasTLS
	tlsConfigName := "tidb-queuefs"

	if enableTLS {
		serverName := config.GetStringConfig(cfg, "tls_server_name", "")
		if serverName == "" {
			if dsnStr != "" {
				re := regexp.MustCompile(`@tcp\(([^:]+):\d+\)`)
				if matches := re.FindStringSubmatch(dsnStr); len(matches) > 1 {
					serverName = matches[1]
				}
			} else {
				serverName = config.GetStringConfig(cfg, "host", "")
			}
		}

		skipVerify := config.GetBoolConfig(cfg, "tls_skip_verify", false)
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if serverName != "" {
			tlsConfig.ServerName = serverName
		}
		if skipVerify {
			tlsConfig.InsecureSkipVerify = true
			log.Warn("[queuefs] TLS certificate verification is disabled (insecure)")
		}
		if err := mysql.RegisterTLSConfig(tlsConfigName, tlsConfig); err != nil {
			log.Warnf("[queuefs] Failed to register TLS config (may already exist): %v", err)
		}
	}

	var dsn string
	var dsnWithoutDB string
	if dsnStr != "" {
		parsedDSN, err := mysql.ParseDSN(dsnStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse TiDB DSN: %w", err)
		}
		if database == "" {
			database = parsedDSN.DBName
		}
		if database == "" {
			database = "queuedb"
		}
		// Keep config precedence stable: when both dsn and database are provided,
		// the explicit database setting must win so create/open target the same DB.
		if parsedDSN.DBName != database {
			parsedDSN.DBName = database
		}
		dsn = parsedDSN.FormatDSN()
		adminDSN := *parsedDSN
		adminDSN.DBName = ""
		dsnWithoutDB = adminDSN.FormatDSN()
	} else {
		user := config.GetStringConfig(cfg, "user", "root")
		password := config.GetStringConfig(cfg, "password", "")
		host := config.GetStringConfig(cfg, "host", "127.0.0.1")
		port := config.GetStringConfig(cfg, "port", "4000")
		if database == "" {
			database = "queuedb"
		}
		if password != "" {
			dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True", user, password, host, port, database)
			dsnWithoutDB = fmt.Sprintf("%s:%s@tcp(%s:%s)/?charset=utf8mb4&parseTime=True", user, password, host, port)
		} else {
			dsn = fmt.Sprintf("%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True", user, host, port, database)
			dsnWithoutDB = fmt.Sprintf("%s@tcp(%s:%s)/?charset=utf8mb4&parseTime=True", user, host, port)
		}
		if enableTLS {
			dsn += fmt.Sprintf("&tls=%s", tlsConfigName)
			dsnWithoutDB += fmt.Sprintf("&tls=%s", tlsConfigName)
		}
	}

	log.Infof("[queuefs] Connecting to TiDB (TLS: %v)", enableTLS)
	if database != "" && dsnWithoutDB != "" && dsnWithoutDB != dsn {
		tempDB, err := sql.Open("mysql", dsnWithoutDB)
		if err == nil {
			defer tempDB.Close()
			_, err = tempDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database))
			if err != nil {
				log.Warnf("[queuefs] Failed to create database '%s': %v", database, err)
			} else {
				log.Infof("[queuefs] Database '%s' created or already exists", database)
			}
		}
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open TiDB database: %w", err)
	}
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping TiDB database: %w", err)
	}
	return db, nil
}

func (b *TiDBDBBackend) GetInitSQL() []string {
	return []string{`CREATE TABLE IF NOT EXISTS queuefs_registry (
			queue_name VARCHAR(255) PRIMARY KEY,
			table_name VARCHAR(255) NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`}
}

func (b *TiDBDBBackend) SupportsSkipLocked() bool { return true }

func (b *TiDBDBBackend) QueueTableDDL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		message_id VARCHAR(64) NOT NULL,
		data LONGBLOB NOT NULL,
		timestamp BIGINT NOT NULL,
		attempt INT NOT NULL DEFAULT 0,
		recovery_count INT NOT NULL DEFAULT 0,
		receipt VARCHAR(128) NULL,
		claimed_at BIGINT NULL,
		lease_until BIGINT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		deleted TINYINT(1) DEFAULT 0,
		deleted_at TIMESTAMP NULL,
		INDEX idx_deleted_receipt_id (deleted, receipt, id),
		INDEX idx_deleted_lease_until (deleted, lease_until)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, tableName)
}

func (b *TiDBDBBackend) EnsureQueueIndexes(db *sql.DB, tableName string) error {
	// TODO(queuefs): re-check this migration path against real MySQL variants.
	// TiDB accepts these IF NOT EXISTS forms, but backend=mysql currently reuses
	// this adapter and may need narrower compatibility handling.
	alterSQL := []string{
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS attempt INT NOT NULL DEFAULT 0", tableName),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS recovery_count INT NOT NULL DEFAULT 0", tableName),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS receipt VARCHAR(128) NULL", tableName),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS claimed_at BIGINT NULL", tableName),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS lease_until BIGINT NULL", tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_deleted_receipt_id ON %s(deleted, receipt, id)", strings.TrimPrefix(tableName, "queuefs_queue_"), tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_deleted_lease_until ON %s(deleted, lease_until)", strings.TrimPrefix(tableName, "queuefs_queue_"), tableName),
	}
	for _, stmt := range alterSQL {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to evolve queue schema: %w", err)
		}
	}
	return nil
}
func (b *TiDBDBBackend) RegistryInsertSQL() string {
	return "INSERT IGNORE INTO queuefs_registry (queue_name, table_name) VALUES (?, ?)"
}
func (b *TiDBDBBackend) Rebind(query string) string { return query }
func (b *TiDBDBBackend) BoolLiteral(value bool) string {
	if value {
		return "1"
	}
	return "0"
}
