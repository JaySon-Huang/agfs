package queuefs

import (
	"fmt"

	"github.com/c4pt0r/agfs/agfs-server/pkg/plugin/config"
)

var queueFSAllowedConfigKeys = []string{
	"backend", "mount_path",
	"mode",
	"db_path", "dsn", "admin_dsn", "user", "password", "host", "port", "database",
	"enable_tls", "tls_server_name", "tls_skip_verify",
}

var queueFSValidBackends = map[string]bool{
	"memory":     true,
	"tidb":       true,
	"mysql":      true,
	"pgsql":      true,
	"postgres":   true,
	"postgresql": true,
	"sqlite":     true,
	"sqlite3":    true,
}

func queueFSBackendType(cfg map[string]interface{}) string {
	return config.GetStringConfig(cfg, "backend", "memory")
}

func queueFSMode(cfg map[string]interface{}) string {
	return config.GetStringConfig(cfg, "mode", queueModeFIFO)
}

func validateQueueFSConfig(cfg map[string]interface{}) error {
	if err := config.ValidateOnlyKnownKeys(cfg, queueFSAllowedConfigKeys); err != nil {
		return err
	}
	if err := config.ValidateStringType(cfg, "backend"); err != nil {
		return err
	}
	if err := config.ValidateStringType(cfg, "mode"); err != nil {
		return err
	}

	backendType := queueFSBackendType(cfg)
	if !queueFSValidBackends[backendType] {
		return fmt.Errorf("unsupported backend: %s (valid options: memory, tidb/mysql, pgsql/postgres/postgresql, sqlite/sqlite3)", backendType)
	}
	mode := queueFSMode(cfg)
	if mode != queueModeFIFO && mode != queueModeDurable {
		return fmt.Errorf("unsupported queue mode: %s (valid options: %s, %s)", mode, queueModeFIFO, queueModeDurable)
	}

	if backendType == "memory" {
		return nil
	}

	for _, key := range []string{"db_path", "dsn", "admin_dsn", "user", "password", "host", "database", "tls_server_name"} {
		if err := config.ValidateStringType(cfg, key); err != nil {
			return err
		}
	}

	if err := config.ValidateIntType(cfg, "port"); err != nil {
		return err
	}

	for _, key := range []string{"enable_tls", "tls_skip_verify"} {
		if err := config.ValidateBoolType(cfg, key); err != nil {
			return err
		}
	}

	return nil
}

func newQueueBackendFromConfig(cfg map[string]interface{}) (QueueBackend, string, error) {
	backendType := queueFSBackendType(cfg)

	var backend QueueBackend
	switch backendType {
	case "memory":
		backend = NewMemoryBackend()
	case "sqlite", "sqlite3":
		backend = NewSQLiteQueueBackend()
	case "pgsql", "postgres", "postgresql":
		backend = NewPostgresQueueBackend()
	case "tidb", "mysql":
		backend = NewTiDBQueueBackend()
	default:
		return nil, "", fmt.Errorf("unsupported backend: %s", backendType)
	}

	return backend, backendType, nil
}
