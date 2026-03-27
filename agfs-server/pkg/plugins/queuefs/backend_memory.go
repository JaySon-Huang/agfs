package queuefs

import memorybackend "github.com/c4pt0r/agfs/agfs-server/pkg/plugins/queuefs/internal/memory"

// Queue remains exported from the root package for compatibility.
type Queue = memorybackend.Queue

// MemoryBackend remains exported from the root package for compatibility.
type MemoryBackend = memorybackend.Backend

// NewMemoryBackend keeps the root-package constructor stable.
func NewMemoryBackend() *MemoryBackend {
	return memorybackend.NewBackend()
}
