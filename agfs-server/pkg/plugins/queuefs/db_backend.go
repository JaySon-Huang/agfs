package queuefs

import sqlqueue "github.com/c4pt0r/agfs/agfs-server/pkg/plugins/queuefs/internal/sqlqueue"

// DBBackend remains exported from the root package for compatibility.
type DBBackend = sqlqueue.DBBackend

// SQLiteDBBackend remains exported from the root package for compatibility.
type SQLiteDBBackend = sqlqueue.SQLiteDBBackend

// TiDBDBBackend remains exported from the root package for compatibility.
type TiDBDBBackend = sqlqueue.TiDBDBBackend

// PostgreSQLDBBackend remains exported from the root package for compatibility.
type PostgreSQLDBBackend = sqlqueue.PostgreSQLDBBackend

func NewSQLiteDBBackend() *SQLiteDBBackend {
	return sqlqueue.NewSQLiteDBBackend()
}

func NewTiDBDBBackend() *TiDBDBBackend {
	return sqlqueue.NewTiDBDBBackend()
}

func NewPostgreSQLDBBackend() *PostgreSQLDBBackend {
	return sqlqueue.NewPostgreSQLDBBackend()
}

func CreateBackend(cfg map[string]interface{}) (DBBackend, error) {
	return sqlqueue.CreateBackend(cfg)
}
