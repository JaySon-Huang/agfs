package queuefs

import (
	"strings"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
)

func TestQueueFSDefaultModeIsFIFO(t *testing.T) {
	fs := newConfiguredTestQueueFS(t, map[string]interface{}{"backend": "memory"})

	rootInfo, err := fs.Stat("/")
	if err != nil {
		t.Fatalf("stat root: %v", err)
	}
	if got := rootInfo.Meta.Content["mode"]; got != queueModeFIFO {
		t.Fatalf("root mode = %q, want %q", got, queueModeFIFO)
	}

	if err := fs.Mkdir("/jobs", 0o755); err != nil {
		t.Fatalf("mkdir /jobs: %v", err)
	}
	entries, err := fs.ReadDir("/jobs")
	if err != nil {
		t.Fatalf("readdir /jobs: %v", err)
	}
	if _, ok := queueDirEntryNames(entries)["ack"]; ok {
		t.Fatalf("fifo mode should not expose ack control file: %+v", entries)
	}
	if _, err := fs.Stat("/jobs/ack"); err == nil || !strings.Contains(err.Error(), "no such file") {
		t.Fatalf("stat /jobs/ack error = %v, want missing path", err)
	}
	if _, err := fs.OpenHandle("/jobs/ack", filesystem.O_WRONLY, 0); err == nil || !strings.Contains(err.Error(), "unknown operation") {
		t.Fatalf("open fifo ack handle error = %v, want unknown operation", err)
	}
}

func TestQueueFSDurableModeExposesAckControlFile(t *testing.T) {
	fs := newConfiguredTestQueueFS(t, map[string]interface{}{
		"backend": "memory",
		"mode":    queueModeDurable,
	})

	rootInfo, err := fs.Stat("/")
	if err != nil {
		t.Fatalf("stat root: %v", err)
	}
	if got := rootInfo.Meta.Content["mode"]; got != queueModeDurable {
		t.Fatalf("root mode = %q, want %q", got, queueModeDurable)
	}

	if err := fs.Mkdir("/jobs", 0o755); err != nil {
		t.Fatalf("mkdir /jobs: %v", err)
	}
	entries, err := fs.ReadDir("/jobs")
	if err != nil {
		t.Fatalf("readdir /jobs: %v", err)
	}
	files := queueDirEntryNames(entries)
	ack, ok := files["ack"]
	if !ok {
		t.Fatalf("durable mode missing ack control file in %+v", entries)
	}
	if ack.Mode != 0o222 {
		t.Fatalf("ack mode = %#o, want 0222", ack.Mode)
	}
	if ack.Meta.Type != MetaValueQueueControl {
		t.Fatalf("ack meta type = %q, want %q", ack.Meta.Type, MetaValueQueueControl)
	}

	info, err := fs.Stat("/jobs/ack")
	if err != nil {
		t.Fatalf("stat /jobs/ack: %v", err)
	}
	if info.Mode != 0o222 {
		t.Fatalf("ack stat mode = %#o, want 0222", info.Mode)
	}

	if _, err := fs.Read("/jobs/ack", 0, -1); err == nil || !strings.Contains(err.Error(), "write-only") {
		t.Fatalf("read /jobs/ack error = %v, want write-only", err)
	}
	handle, err := fs.OpenHandle("/jobs/ack", filesystem.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open durable ack handle: %v", err)
	}
	t.Cleanup(func() { _ = handle.Close() })
	if handle.Flags() != filesystem.O_WRONLY {
		t.Fatalf("ack handle flags = %v, want O_WRONLY", handle.Flags())
	}
	if _, err := handle.Write([]byte("msg-id")); err == nil || !strings.Contains(err.Error(), "not implemented") {
		t.Fatalf("write ack handle error = %v, want not implemented", err)
	}
}
