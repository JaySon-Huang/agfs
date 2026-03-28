package queuefs

import model "github.com/c4pt0r/agfs/agfs-server/pkg/plugins/queuefs/internal"

// QueueBackend remains exported from the root package for compatibility.
type QueueBackend = model.QueueBackend

// DurableQueueBackend remains exported from the root package for compatibility.
type DurableQueueBackend = model.DurableQueueBackend

// QueueMessage remains exported from the root package for compatibility.
type QueueMessage = model.QueueMessage

// ClaimRequest remains exported from the root package for compatibility.
type ClaimRequest = model.ClaimRequest

// ClaimedMessage remains exported from the root package for compatibility.
type ClaimedMessage = model.ClaimedMessage

// ReleaseRequest remains exported from the root package for compatibility.
type ReleaseRequest = model.ReleaseRequest

// QueueStats remains exported from the root package for compatibility.
type QueueStats = model.QueueStats
