package queuefs

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
)

type durableTestBackend struct {
	queues            map[string][]QueueMessage
	lastEnqueueTime   map[string]time.Time
	claimResponse     ClaimedMessage
	claimFound        bool
	statsResponse     QueueStats
	claimCalls        int
	lastClaimRequest  ClaimRequest
	dequeueCalls      int
	ackCalls          int
	lastAckMessageID  string
	lastAckReceipt    string
	lastRecoverLimit  int
	recoveriesApplied int
}

func newDurableTestBackend() *durableTestBackend {
	now := time.Unix(1711600000, 0).UTC()
	return &durableTestBackend{
		queues: map[string][]QueueMessage{
			"jobs": {{ID: "pending-1", Data: "payload", Timestamp: now}},
		},
		lastEnqueueTime: map[string]time.Time{
			"jobs": now,
		},
		claimResponse: ClaimedMessage{
			MessageID:  "claimed-1",
			QueueName:  "jobs",
			Data:       "payload",
			Receipt:    "receipt-1",
			ClaimedAt:  now,
			LeaseUntil: now.Add(30 * time.Second),
			Attempt:    1,
		},
		claimFound:    true,
		statsResponse: QueueStats{Pending: 1, Processing: 2, Recoveries: 3},
	}
}

func (b *durableTestBackend) Initialize(config map[string]interface{}) error { return nil }

func (b *durableTestBackend) Close() error { return nil }

func (b *durableTestBackend) GetType() string { return "durable-test" }

func (b *durableTestBackend) Enqueue(queueName string, msg QueueMessage) error {
	b.queues[queueName] = append(b.queues[queueName], msg)
	b.lastEnqueueTime[queueName] = msg.Timestamp
	return nil
}

func (b *durableTestBackend) Dequeue(queueName string) (QueueMessage, bool, error) {
	b.dequeueCalls++
	return QueueMessage{}, false, nil
}

func (b *durableTestBackend) Peek(queueName string) (QueueMessage, bool, error) {
	queue := b.queues[queueName]
	if len(queue) == 0 {
		return QueueMessage{}, false, nil
	}
	return queue[0], true, nil
}

func (b *durableTestBackend) Size(queueName string) (int, error) {
	return b.statsResponse.Pending, nil
}

func (b *durableTestBackend) Clear(queueName string) error {
	b.queues[queueName] = nil
	b.statsResponse = QueueStats{}
	return nil
}

func (b *durableTestBackend) ListQueues(prefix string) ([]string, error) {
	queues := []string{}
	for queueName := range b.queues {
		if prefix == "" || queueName == prefix || strings.HasPrefix(queueName, prefix+"/") {
			queues = append(queues, queueName)
		}
	}
	return queues, nil
}

func (b *durableTestBackend) GetLastEnqueueTime(queueName string) (time.Time, error) {
	return b.lastEnqueueTime[queueName], nil
}

func (b *durableTestBackend) RemoveQueue(queueName string) error {
	delete(b.queues, queueName)
	delete(b.lastEnqueueTime, queueName)
	return nil
}

func (b *durableTestBackend) CreateQueue(queueName string) error {
	if _, ok := b.queues[queueName]; !ok {
		b.queues[queueName] = nil
	}
	return nil
}

func (b *durableTestBackend) QueueExists(queueName string) (bool, error) {
	_, exists := b.queues[queueName]
	return exists, nil
}

func (b *durableTestBackend) Claim(queueName string, req ClaimRequest) (ClaimedMessage, bool, error) {
	b.claimCalls++
	b.lastClaimRequest = req
	return b.claimResponse, b.claimFound, nil
}

func (b *durableTestBackend) Ack(queueName string, messageID string, receipt string) error {
	b.ackCalls++
	b.lastAckMessageID = messageID
	b.lastAckReceipt = receipt
	return nil
}

func (b *durableTestBackend) Release(queueName string, messageID string, req ReleaseRequest) error {
	return nil
}

func (b *durableTestBackend) RecoverExpired(queueName string, now time.Time, limit int) (int, error) {
	b.lastRecoverLimit = limit
	b.recoveriesApplied++
	return b.recoveriesApplied, nil
}

func (b *durableTestBackend) Stats(queueName string) (QueueStats, error) {
	return b.statsResponse, nil
}

func newDurableTestQueueFS(t *testing.T) (*queueFS, *durableTestBackend) {
	t.Helper()

	plugin := NewQueueFSPlugin()
	backend := newDurableTestBackend()
	plugin.backend = backend
	plugin.mode = queueModeDurable

	fs, ok := plugin.GetFileSystem().(*queueFS)
	if !ok {
		t.Fatalf("unexpected filesystem type %T", plugin.GetFileSystem())
	}
	return fs, backend
}

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
	files := queueDirEntryNames(entries)
	if _, ok := files["peek"]; !ok {
		t.Fatalf("fifo mode missing peek control file in %+v", entries)
	}
	for _, name := range []string{"ack", "recover", "stats"} {
		if _, ok := files[name]; ok {
			t.Fatalf("fifo mode should not expose %q in %+v", name, entries)
		}
	}
	if _, err := fs.Stat("/jobs/ack"); err == nil || !strings.Contains(err.Error(), "no such file") {
		t.Fatalf("stat /jobs/ack error = %v, want missing path", err)
	}

	for _, queueName := range []string{"/ack", "/recover", "/stats"} {
		if err := fs.Mkdir(queueName, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", queueName, err)
		}
		entries, err := fs.ReadDir(queueName)
		if err != nil {
			t.Fatalf("readdir %s: %v", queueName, err)
		}
		files := queueDirEntryNames(entries)
		if _, ok := files["peek"]; !ok {
			t.Fatalf("fifo queue %s missing peek control file in %+v", queueName, entries)
		}
		for _, name := range []string{"ack", "recover", "stats"} {
			if _, ok := files[name]; ok {
				t.Fatalf("fifo queue %s should not expose %q in %+v", queueName, name, entries)
			}
		}
	}
}

func TestQueueFSDurableModeExposesDurableOnlySurface(t *testing.T) {
	fs, _ := newDurableTestQueueFS(t)

	rootInfo, err := fs.Stat("/")
	if err != nil {
		t.Fatalf("stat root: %v", err)
	}
	if got := rootInfo.Meta.Content["mode"]; got != queueModeDurable {
		t.Fatalf("root mode = %q, want %q", got, queueModeDurable)
	}

	entries, err := fs.ReadDir("/jobs")
	if err != nil {
		t.Fatalf("readdir /jobs: %v", err)
	}
	files := queueDirEntryNames(entries)
	if len(files) != 7 {
		t.Fatalf("unexpected durable control files: %+v", entries)
	}
	for _, name := range []string{"enqueue", "dequeue", "size", "stats", "clear", "ack", "recover"} {
		if _, ok := files[name]; !ok {
			t.Fatalf("durable mode missing %q in %+v", name, entries)
		}
	}
	if _, ok := files["peek"]; ok {
		t.Fatalf("durable mode should not expose peek in %+v", entries)
	}
	if got := files["stats"].Mode; got != 0o444 {
		t.Fatalf("stats mode = %#o, want 0444", got)
	}
	if got := files["stats"].Meta.Type; got != MetaValueQueueStatus {
		t.Fatalf("stats meta type = %q, want %q", got, MetaValueQueueStatus)
	}
	if got := files["recover"].Mode; got != 0o222 {
		t.Fatalf("recover mode = %#o, want 0222", got)
	}

	if _, err := fs.Stat("/jobs/peek"); err == nil || !strings.Contains(err.Error(), "no such file") {
		t.Fatalf("stat /jobs/peek error = %v, want missing path", err)
	}
	if err := fs.Mkdir("/peek", 0o755); err != nil {
		t.Fatalf("mkdir /peek: %v", err)
	}
	peekEntries, err := fs.ReadDir("/peek")
	if err != nil {
		t.Fatalf("readdir /peek: %v", err)
	}
	peekFiles := queueDirEntryNames(peekEntries)
	if _, ok := peekFiles["peek"]; ok {
		t.Fatalf("durable queue /peek should not expose peek control file in %+v", peekEntries)
	}
	for _, name := range []string{"enqueue", "dequeue", "size", "stats", "clear", "ack", "recover"} {
		if _, ok := peekFiles[name]; !ok {
			t.Fatalf("durable queue /peek missing %q in %+v", name, peekEntries)
		}
	}
	if handle, err := fs.OpenHandle("/peek/dequeue", filesystem.O_RDONLY, 0); err != nil {
		t.Fatalf("open durable queue named peek handle: %v", err)
	} else if err := handle.Close(); err != nil {
		t.Fatalf("close durable queue named peek handle: %v", err)
	}
	if _, err := fs.Read("/jobs/recover", 0, -1); err == nil || !strings.Contains(err.Error(), "write-only") {
		t.Fatalf("read /jobs/recover error = %v, want write-only", err)
	}
	if _, err := fs.Write("/jobs/recover", []byte("{"), -1, filesystem.WriteFlagAppend); err == nil || !strings.Contains(err.Error(), "invalid durable recover payload") {
		t.Fatalf("write /jobs/recover error = %v, want invalid payload", err)
	}
}

func TestQueueFSDurableModeMapsDequeueToClaimAndStats(t *testing.T) {
	fs, backend := newDurableTestQueueFS(t)

	claimedBytes := mustReadAll(t, fs, "/jobs/dequeue")
	var claimed ClaimedMessage
	if err := json.Unmarshal(claimedBytes, &claimed); err != nil {
		t.Fatalf("unmarshal durable dequeue payload: %v (payload=%q)", err, string(claimedBytes))
	}
	if claimed.MessageID != backend.claimResponse.MessageID {
		t.Fatalf("claimed message_id = %q, want %q", claimed.MessageID, backend.claimResponse.MessageID)
	}
	if claimed.Receipt != backend.claimResponse.Receipt {
		t.Fatalf("claimed receipt = %q, want %q", claimed.Receipt, backend.claimResponse.Receipt)
	}
	if backend.claimCalls != 1 {
		t.Fatalf("claim calls = %d, want 1", backend.claimCalls)
	}
	if backend.lastClaimRequest.LeaseDuration != queueFSDurableDefaultLeaseDuration {
		t.Fatalf("claim lease duration = %s, want %s", backend.lastClaimRequest.LeaseDuration, queueFSDurableDefaultLeaseDuration)
	}
	if backend.dequeueCalls != 0 {
		t.Fatalf("dequeue calls = %d, want 0", backend.dequeueCalls)
	}

	statsBytes := mustReadAll(t, fs, "/jobs/stats")
	var stats QueueStats
	if err := json.Unmarshal(statsBytes, &stats); err != nil {
		t.Fatalf("unmarshal durable stats payload: %v (payload=%q)", err, string(statsBytes))
	}
	if stats != backend.statsResponse {
		t.Fatalf("stats = %+v, want %+v", stats, backend.statsResponse)
	}

	sizeBytes := mustReadAll(t, fs, "/jobs/size")
	if got := string(sizeBytes); got != "1" {
		t.Fatalf("durable size payload = %q, want pending count 1", got)
	}
}

func TestQueueFSDurableModeMapsAckAndRecoverWrites(t *testing.T) {
	fs, backend := newDurableTestQueueFS(t)

	ackPayload := []byte(`{"message_id":"claimed-1","receipt":"receipt-1"}`)
	if _, err := fs.Write("/jobs/ack", ackPayload, -1, filesystem.WriteFlagAppend); err != nil {
		t.Fatalf("write /jobs/ack: %v", err)
	}
	if backend.ackCalls != 1 {
		t.Fatalf("ack calls = %d, want 1", backend.ackCalls)
	}
	if backend.lastAckMessageID != "claimed-1" || backend.lastAckReceipt != "receipt-1" {
		t.Fatalf("ack payload = (%q, %q), want (claimed-1, receipt-1)", backend.lastAckMessageID, backend.lastAckReceipt)
	}

	recoverPayload := []byte(`{"limit":5}`)
	if _, err := fs.Write("/jobs/recover", recoverPayload, -1, filesystem.WriteFlagAppend); err != nil {
		t.Fatalf("write /jobs/recover: %v", err)
	}
	if backend.recoveriesApplied != 1 {
		t.Fatalf("recover calls = %d, want 1", backend.recoveriesApplied)
	}
	if backend.lastRecoverLimit != 5 {
		t.Fatalf("recover limit = %d, want 5", backend.lastRecoverLimit)
	}

	if _, err := fs.Write("/jobs/ack", []byte(`{"message_id":"claimed-1"}`), -1, filesystem.WriteFlagAppend); err == nil || !strings.Contains(err.Error(), "message_id and receipt are required") {
		t.Fatalf("write /jobs/ack missing receipt error = %v, want validation error", err)
	}
}

func TestQueueFSDurableMemoryLifecycle(t *testing.T) {
	plugin := newConfiguredTestPlugin(t, map[string]interface{}{
		"backend": "memory",
		"mode":    queueModeDurable,
	})
	fs, ok := plugin.GetFileSystem().(*queueFS)
	if !ok {
		t.Fatalf("unexpected filesystem type %T", plugin.GetFileSystem())
	}

	if err := fs.Mkdir("/jobs", 0o755); err != nil {
		t.Fatalf("mkdir /jobs: %v", err)
	}
	if _, err := fs.Write("/jobs/enqueue", []byte("first"), -1, filesystem.WriteFlagAppend); err != nil {
		t.Fatalf("enqueue durable message: %v", err)
	}

	claimedBytes := mustReadAll(t, fs, "/jobs/dequeue")
	var claimed ClaimedMessage
	if err := json.Unmarshal(claimedBytes, &claimed); err != nil {
		t.Fatalf("unmarshal durable claim: %v (payload=%q)", err, string(claimedBytes))
	}
	if got := claimed.Data; got != "first" {
		t.Fatalf("claimed data = %q, want first", got)
	}
	if claimed.Receipt == "" {
		t.Fatal("claimed receipt should not be empty")
	}

	statsBytes := mustReadAll(t, fs, "/jobs/stats")
	var stats QueueStats
	if err := json.Unmarshal(statsBytes, &stats); err != nil {
		t.Fatalf("unmarshal durable stats: %v (payload=%q)", err, string(statsBytes))
	}
	if stats.Pending != 0 || stats.Processing != 1 {
		t.Fatalf("stats after claim = %+v, want pending=0 processing=1", stats)
	}

	ackPayload := []byte(`{"message_id":"` + claimed.MessageID + `","receipt":"` + claimed.Receipt + `"}`)
	if _, err := fs.Write("/jobs/ack", ackPayload, -1, filesystem.WriteFlagAppend); err != nil {
		t.Fatalf("ack durable message: %v", err)
	}
	statsBytes = mustReadAll(t, fs, "/jobs/stats")
	if err := json.Unmarshal(statsBytes, &stats); err != nil {
		t.Fatalf("unmarshal durable stats after ack: %v (payload=%q)", err, string(statsBytes))
	}
	if stats != (QueueStats{}) {
		t.Fatalf("stats after ack = %+v, want zero values", stats)
	}

	durableBackend, ok := plugin.backend.(DurableQueueBackend)
	if !ok {
		t.Fatal("memory backend should implement DurableQueueBackend")
	}
	if _, err := fs.Write("/jobs/enqueue", []byte("recover-me"), -1, filesystem.WriteFlagAppend); err != nil {
		t.Fatalf("enqueue recovery message: %v", err)
	}
	claimedAgain, found, err := durableBackend.Claim("jobs", ClaimRequest{LeaseDuration: time.Second})
	if err != nil {
		t.Fatalf("claim via durable backend: %v", err)
	}
	if !found {
		t.Fatal("expected durable backend claim to find queued message")
	}
	if recovered, err := durableBackend.RecoverExpired("jobs", time.Now().UTC().Add(2*time.Second), 0); err != nil {
		t.Fatalf("recover expired claim: %v", err)
	} else if recovered != 1 {
		t.Fatalf("recovered count = %d, want 1", recovered)
	}
	claimedAfterRecover, found, err := durableBackend.Claim("jobs", ClaimRequest{})
	if err != nil {
		t.Fatalf("claim after recover: %v", err)
	}
	if !found {
		t.Fatal("expected recovered message to become claimable again")
	}
	if claimedAfterRecover.MessageID != claimedAgain.MessageID {
		t.Fatalf("reclaimed message_id = %q, want %q", claimedAfterRecover.MessageID, claimedAgain.MessageID)
	}
	if claimedAfterRecover.Attempt != claimedAgain.Attempt+1 {
		t.Fatalf("reclaimed attempt = %d, want %d", claimedAfterRecover.Attempt, claimedAgain.Attempt+1)
	}
}
