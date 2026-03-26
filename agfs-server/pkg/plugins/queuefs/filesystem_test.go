package queuefs

import (
	"encoding/json"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
)

func TestQueueFSDurableControlFiles(t *testing.T) {
	plugin := NewQueueFSPlugin()
	if err := plugin.Initialize(map[string]interface{}{
		"backend":       "memory",
		"mode":          "task",
		"lease_seconds": 30,
		"max_attempts":  3,
	}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	fs := plugin.GetFileSystem().(*queueFS)
	if err := fs.Mkdir("/jobs", 0o755); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	if _, err := fs.Write("/jobs/enqueue", []byte(`{"data":"hello","task_type":"summary","resource_id":"doc-1","resource_version":"v1"}`), -1, filesystem.WriteFlagAppend); err != nil {
		t.Fatalf("enqueue write failed: %v", err)
	}

	sizeBytes, err := fs.Read("/jobs/size", 0, -1)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("size read failed: %v", err)
	}
	if string(sizeBytes) != "1" {
		t.Fatalf("size = %q, want 1", sizeBytes)
	}

	taskBytes, err := fs.Read("/jobs/dequeue", 0, -1)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("dequeue read failed: %v", err)
	}

	var task QueueTask
	if err := json.Unmarshal(taskBytes, &task); err != nil {
		t.Fatalf("unmarshal dequeue payload failed: %v", err)
	}
	if task.ID == "" || task.TaskType != "summary" || task.ResourceID != "doc-1" || task.Attempt != 1 {
		t.Fatalf("unexpected dequeued task: %+v", task)
	}

	ackPayload := []byte(`{"id":"` + task.ID + `"}`)
	if _, err := fs.Write("/jobs/ack", ackPayload, -1, filesystem.WriteFlagAppend); err != nil {
		t.Fatalf("ack write failed: %v", err)
	}

	statsBytes, err := fs.Read("/jobs/stats", 0, -1)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("stats read failed: %v", err)
	}

	var stats QueueStats
	if err := json.Unmarshal(statsBytes, &stats); err != nil {
		t.Fatalf("unmarshal stats failed: %v", err)
	}
	if stats.Succeeded != 1 || stats.Processing != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	infos, err := fs.ReadDir("/jobs")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	names := make(map[string]bool, len(infos))
	for _, info := range infos {
		names[info.Name] = true
	}
	for _, name := range []string{"enqueue", "dequeue", "peek", "size", "stats", "ack", "nack", "recover", "clear"} {
		if !names[name] {
			t.Fatalf("ReadDir missing %s", name)
		}
	}
}

func TestQueueFSHandleBuffersAckUntilClose(t *testing.T) {
	plugin := NewQueueFSPlugin()
	if err := plugin.Initialize(map[string]interface{}{
		"backend":       "memory",
		"mode":          "task",
		"lease_seconds": 30,
		"max_attempts":  3,
	}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	handleFS := plugin.GetFileSystem().(*queueFS)
	if err := handleFS.Mkdir("/jobs", 0o755); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	if _, err := handleFS.Write("/jobs/enqueue", []byte("hello"), -1, filesystem.WriteFlagAppend); err != nil {
		t.Fatalf("enqueue write failed: %v", err)
	}

	taskBytes, err := handleFS.Read("/jobs/dequeue", 0, -1)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("dequeue read failed: %v", err)
	}

	var task QueueTask
	if err := json.Unmarshal(taskBytes, &task); err != nil {
		t.Fatalf("unmarshal dequeue payload failed: %v", err)
	}

	handle, err := handleFS.OpenHandle("/jobs/ack", filesystem.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenHandle failed: %v", err)
	}

	if _, err := handle.Write([]byte(`{"id":"`)); err != nil {
		t.Fatalf("first handle write failed: %v", err)
	}
	if _, err := handle.Write([]byte(task.ID + `"}`)); err != nil {
		t.Fatalf("second handle write failed: %v", err)
	}

	statsBeforeClose, err := handleFS.stats("jobs")
	if err != nil {
		t.Fatalf("stats before close failed: %v", err)
	}

	var before QueueStats
	if err := json.Unmarshal(statsBeforeClose, &before); err != nil {
		t.Fatalf("unmarshal stats before close failed: %v", err)
	}
	if before.Processing != 1 || before.Succeeded != 0 {
		t.Fatalf("unexpected stats before close: %+v", before)
	}

	if err := handleFS.CloseHandle(handle.ID()); err != nil {
		t.Fatalf("CloseHandle failed: %v", err)
	}

	statsAfterClose, err := handleFS.stats("jobs")
	if err != nil {
		t.Fatalf("stats after close failed: %v", err)
	}

	var after QueueStats
	if err := json.Unmarshal(statsAfterClose, &after); err != nil {
		t.Fatalf("unmarshal stats after close failed: %v", err)
	}
	if after.Succeeded != 1 || after.Processing != 0 {
		t.Fatalf("unexpected stats after close: %+v", after)
	}
}

func TestQueueFSMessageModeCompatibility(t *testing.T) {
	plugin := NewQueueFSPlugin()
	if err := plugin.Initialize(map[string]interface{}{
		"backend":       "memory",
		"mode":          "message",
		"lease_seconds": 30,
		"max_attempts":  3,
	}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	fs := plugin.GetFileSystem().(*queueFS)
	if err := fs.Mkdir("/jobs", 0o755); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	if _, err := fs.Write("/jobs/enqueue", []byte("hello"), -1, filesystem.WriteFlagAppend); err != nil {
		t.Fatalf("enqueue write failed: %v", err)
	}

	taskBytes, err := fs.Read("/jobs/dequeue", 0, -1)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("dequeue read failed: %v", err)
	}

	var task QueueTask
	if err := json.Unmarshal(taskBytes, &task); err != nil {
		t.Fatalf("unmarshal dequeue payload failed: %v", err)
	}
	if task.ID == "" {
		t.Fatalf("unexpected dequeued task: %+v", task)
	}

	sizeBytes, err := fs.Read("/jobs/size", 0, -1)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("size read failed: %v", err)
	}
	if string(sizeBytes) != "0" {
		t.Fatalf("size after message dequeue = %q, want 0", sizeBytes)
	}

	if _, err := fs.Stat("/jobs/stats"); err == nil {
		t.Fatal("stats should not exist in message mode")
	}

	infos, err := fs.ReadDir("/jobs")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	for _, info := range infos {
		switch info.Name {
		case "stats", "ack", "nack", "recover":
			t.Fatalf("message mode should not expose %s", info.Name)
		}
	}
}
