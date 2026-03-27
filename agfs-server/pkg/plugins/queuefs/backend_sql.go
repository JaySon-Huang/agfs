package queuefs

import sqlqueue "github.com/c4pt0r/agfs/agfs-server/pkg/plugins/queuefs/internal/sqlqueue"

// SQLQueueBackend remains exported from the root package for compatibility.
type SQLQueueBackend = sqlqueue.Backend

// NewSQLQueueBackend keeps the root-package constructor stable.
func NewSQLQueueBackend() *SQLQueueBackend {
	return sqlqueue.NewBackend()
}

// NewSQLiteQueueBackend keeps the root-package constructor stable.
func NewSQLiteQueueBackend() *SQLQueueBackend {
	return sqlqueue.NewSQLiteBackend()
}

// NewTiDBQueueBackend keeps the root-package constructor stable.
func NewTiDBQueueBackend() *SQLQueueBackend {
	return sqlqueue.NewTiDBBackend()
}

// NewPostgresQueueBackend keeps the root-package constructor stable.
func NewPostgresQueueBackend() *SQLQueueBackend {
	return sqlqueue.NewPostgresBackend()
}
