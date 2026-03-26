package queuefs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/plugin/config"
)

const (
	defaultLeaseDuration   = 5 * time.Minute
	defaultMaxAttempts     = 16
	defaultRecoverLookback = time.Second
)

// QueueMode controls whether queuefs behaves like the legacy message queue or
// the new durable task queue.
type QueueMode string

const (
	// QueueModeMessage keeps the historical "dequeue implies completion"
	// behavior for compatibility.
	QueueModeMessage QueueMode = "message"
	// QueueModeTask enables durable lease/ack/nack semantics.
	QueueModeTask QueueMode = "task"
)

// TaskStatus is the durable lifecycle state tracked for every queued task.
type TaskStatus string

const (
	TaskStatusQueued       TaskStatus = "queued"
	TaskStatusProcessing   TaskStatus = "processing"
	TaskStatusSucceeded    TaskStatus = "succeeded"
	TaskStatusFailed       TaskStatus = "failed"
	TaskStatusDeadLettered TaskStatus = "dead_lettered"
)

// QueueConfig contains runtime settings shared by the queuefs filesystem layer
// and the durable backends.
type QueueConfig struct {
	Mode             QueueMode
	LeaseDuration    time.Duration
	DefaultMaxRetry  int
	EnableDeadLetter bool
	ConsumerID       string
}

func parseQueueConfig(cfg map[string]interface{}) (QueueConfig, error) {
	mode := QueueMode(config.GetStringConfig(cfg, "mode", string(QueueModeTask)))
	switch mode {
	case QueueModeMessage, QueueModeTask:
	default:
		return QueueConfig{}, fmt.Errorf("unsupported mode: %s (valid options: message, task)", mode)
	}

	leaseSeconds := config.GetIntConfig(cfg, "lease_seconds", int(defaultLeaseDuration/time.Second))
	if leaseSeconds <= 0 {
		return QueueConfig{}, fmt.Errorf("lease_seconds must be greater than 0")
	}

	maxAttempts := config.GetIntConfig(cfg, "max_attempts", defaultMaxAttempts)
	if maxAttempts <= 0 {
		return QueueConfig{}, fmt.Errorf("max_attempts must be greater than 0")
	}

	return QueueConfig{
		Mode:             mode,
		LeaseDuration:    time.Duration(leaseSeconds) * time.Second,
		DefaultMaxRetry:  maxAttempts,
		EnableDeadLetter: config.GetBoolConfig(cfg, "enable_dead_letter", true),
		ConsumerID:       config.GetStringConfig(cfg, "consumer_id", ""),
	}, nil
}

// QueueTask is the durable task payload returned by dequeue/peek and accepted by
// backend enqueue operations.
//
// The queuefs runtime keeps these fields generic so higher layers such as dat9
// can bind task typing and resource-version metadata without changing the queue
// contract itself.
type QueueTask struct {
	ID              string          `json:"id"`
	Data            json.RawMessage `json:"data,omitempty"`
	Timestamp       time.Time       `json:"timestamp"`
	TaskType        string          `json:"task_type,omitempty"`
	ResourceID      string          `json:"resource_id,omitempty"`
	ResourceVersion string          `json:"resource_version,omitempty"`
	Attempt         int             `json:"attempt,omitempty"`
	MaxAttempts     int             `json:"max_attempts,omitempty"`
	DedupeKey       string          `json:"dedupe_key,omitempty"`
	Priority        int             `json:"priority,omitempty"`
	LeaseUntil      *time.Time      `json:"lease_until,omitempty"`
}

func (t QueueTask) clone() QueueTask {
	clone := t
	clone.Data = cloneRawMessage(t.Data)
	if t.LeaseUntil != nil {
		leaseUntil := *t.LeaseUntil
		clone.LeaseUntil = &leaseUntil
	}
	return clone
}

// QueueStats contains per-state counts for a queue.
type QueueStats struct {
	Queued       int `json:"queued"`
	Processing   int `json:"processing"`
	Succeeded    int `json:"succeeded"`
	Failed       int `json:"failed"`
	DeadLettered int `json:"dead_lettered"`
}

type enqueuePayload struct {
	ID              string          `json:"id"`
	Data            json.RawMessage `json:"data"`
	TaskType        string          `json:"task_type"`
	ResourceID      string          `json:"resource_id"`
	ResourceVersion string          `json:"resource_version"`
	MaxAttempts     int             `json:"max_attempts"`
	DedupeKey       string          `json:"dedupe_key"`
	Priority        int             `json:"priority"`
}

type ackPayload struct {
	ID string `json:"id"`
}

type nackPayload struct {
	ID                string `json:"id"`
	Error             string `json:"error,omitempty"`
	Retry             bool   `json:"retry"`
	RetryAfterSeconds int    `json:"retry_after_seconds,omitempty"`
}

func cloneRawMessage(data json.RawMessage) json.RawMessage {
	if len(data) == 0 {
		return nil
	}
	cloned := make([]byte, len(data))
	copy(cloned, data)
	return json.RawMessage(cloned)
}

func newRawStringPayload(data []byte) json.RawMessage {
	quoted, _ := json.Marshal(string(data))
	return json.RawMessage(quoted)
}

func parseEnqueueTask(raw []byte, now time.Time, cfg QueueConfig, newTaskID func() (string, error)) (QueueTask, error) {
	trimmed := bytes.TrimSpace(raw)
	task := QueueTask{
		Timestamp:   now,
		MaxAttempts: cfg.DefaultMaxRetry,
	}

	if len(trimmed) == 0 {
		task.Data = newRawStringPayload(nil)
	} else if shouldTreatAsEnvelope(trimmed) {
		var payload enqueuePayload
		if err := json.Unmarshal(trimmed, &payload); err != nil {
			return QueueTask{}, fmt.Errorf("invalid enqueue payload: %w", err)
		}
		task.ID = payload.ID
		task.Data = cloneRawMessage(payload.Data)
		task.TaskType = payload.TaskType
		task.ResourceID = payload.ResourceID
		task.ResourceVersion = payload.ResourceVersion
		task.DedupeKey = payload.DedupeKey
		task.Priority = payload.Priority
		if payload.MaxAttempts > 0 {
			task.MaxAttempts = payload.MaxAttempts
		}
	} else if json.Valid(trimmed) {
		task.Data = cloneRawMessage(trimmed)
	} else {
		task.Data = newRawStringPayload(trimmed)
	}

	if task.ID == "" {
		id, err := newTaskID()
		if err != nil {
			return QueueTask{}, err
		}
		task.ID = id
	}
	if task.MaxAttempts <= 0 {
		task.MaxAttempts = cfg.DefaultMaxRetry
	}

	return task, nil
}

func shouldTreatAsEnvelope(raw []byte) bool {
	if len(raw) == 0 || raw[0] != '{' {
		return false
	}

	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return false
	}

	for _, key := range []string{
		"id",
		"data",
		"task_type",
		"resource_id",
		"resource_version",
		"max_attempts",
		"dedupe_key",
		"priority",
	} {
		if _, ok := object[key]; ok {
			return true
		}
	}
	return false
}

func parseAckPayload(raw []byte) (ackPayload, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return ackPayload{}, fmt.Errorf("ack payload is required")
	}

	if trimmed[0] == '{' {
		var payload ackPayload
		if err := json.Unmarshal(trimmed, &payload); err != nil {
			return ackPayload{}, fmt.Errorf("invalid ack payload: %w", err)
		}
		if payload.ID == "" {
			return ackPayload{}, fmt.Errorf("ack payload must include id")
		}
		return payload, nil
	}

	return ackPayload{ID: string(trimmed)}, nil
}

func parseNackPayload(raw []byte) (nackPayload, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nackPayload{}, fmt.Errorf("nack payload is required")
	}

	var payload nackPayload
	if trimmed[0] != '{' {
		payload.ID = string(trimmed)
		return payload, nil
	}

	if err := json.Unmarshal(trimmed, &payload); err != nil {
		return nackPayload{}, fmt.Errorf("invalid nack payload: %w", err)
	}
	if payload.ID == "" {
		return nackPayload{}, fmt.Errorf("nack payload must include id")
	}
	if payload.RetryAfterSeconds < 0 {
		return nackPayload{}, fmt.Errorf("retry_after_seconds must be non-negative")
	}
	return payload, nil
}
