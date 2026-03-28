package memory

import (
	"testing"
	"time"

	model "github.com/c4pt0r/agfs/agfs-server/pkg/plugins/queuefs/internal"
)

func TestBackendDurableLifecycle(t *testing.T) {
	backend := NewBackend()
	queueName := "jobs"
	now := time.Unix(1711600000, 0).UTC()

	if err := backend.CreateQueue(queueName); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	for i, payload := range []string{"first", "second"} {
		if err := backend.Enqueue(queueName, model.QueueMessage{
			ID:        payload,
			Data:      payload,
			Timestamp: now.Add(time.Duration(i) * time.Second),
		}); err != nil {
			t.Fatalf("enqueue %q: %v", payload, err)
		}
	}

	claimed, found, err := backend.Claim(queueName, model.ClaimRequest{LeaseDuration: time.Second})
	if err != nil {
		t.Fatalf("claim first message: %v", err)
	}
	if !found || claimed.MessageID != "first" || claimed.Data != "first" || claimed.Attempt != 1 {
		t.Fatalf("unexpected claim result: found=%v claimed=%+v", found, claimed)
	}

	stats, err := backend.Stats(queueName)
	if err != nil {
		t.Fatalf("stats after claim: %v", err)
	}
	if stats.Pending != 1 || stats.Processing != 1 || stats.Recoveries != 0 {
		t.Fatalf("stats after claim = %+v, want pending=1 processing=1 recoveries=0", stats)
	}
	if size, err := backend.Size(queueName); err != nil || size != 1 {
		t.Fatalf("size after claim = (%d, %v), want (1, nil)", size, err)
	}

	if err := backend.Ack(queueName, claimed.MessageID, "wrong-receipt"); err == nil {
		t.Fatal("ack with wrong receipt should fail")
	}
	if err := backend.Release(queueName, claimed.MessageID, model.ReleaseRequest{Receipt: claimed.Receipt}); err != nil {
		t.Fatalf("release claimed message: %v", err)
	}

	reclaimed, found, err := backend.Claim(queueName, model.ClaimRequest{})
	if err != nil {
		t.Fatalf("claim released message: %v", err)
	}
	if !found || reclaimed.MessageID != claimed.MessageID || reclaimed.Attempt != claimed.Attempt+1 {
		t.Fatalf("unexpected reclaimed message: found=%v claimed=%+v", found, reclaimed)
	}
	if err := backend.Ack(queueName, reclaimed.MessageID, reclaimed.Receipt); err != nil {
		t.Fatalf("ack reclaimed message: %v", err)
	}

	second, found, err := backend.Claim(queueName, model.ClaimRequest{LeaseDuration: time.Second})
	if err != nil {
		t.Fatalf("claim second message: %v", err)
	}
	if !found || second.MessageID != "second" {
		t.Fatalf("unexpected second claim: found=%v claimed=%+v", found, second)
	}

	recovered, err := backend.RecoverExpired(queueName, time.Now().UTC().Add(2*time.Second), 0)
	if err != nil {
		t.Fatalf("recover expired message: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("recovered count = %d, want 1", recovered)
	}
	recoveredAgain, err := backend.RecoverExpired(queueName, time.Now().UTC().Add(2*time.Second), 0)
	if err != nil {
		t.Fatalf("recover expired message twice: %v", err)
	}
	if recoveredAgain != 0 {
		t.Fatalf("second recovery count = %d, want 0", recoveredAgain)
	}

	stats, err = backend.Stats(queueName)
	if err != nil {
		t.Fatalf("stats after recover: %v", err)
	}
	if stats.Pending != 1 || stats.Processing != 0 || stats.Recoveries != 1 {
		t.Fatalf("stats after recover = %+v, want pending=1 processing=0 recoveries=1", stats)
	}

	reclaimedAfterRecover, found, err := backend.Claim(queueName, model.ClaimRequest{})
	if err != nil {
		t.Fatalf("claim recovered message: %v", err)
	}
	if !found || reclaimedAfterRecover.MessageID != second.MessageID || reclaimedAfterRecover.Attempt != second.Attempt+1 {
		t.Fatalf("unexpected recovered claim: found=%v claimed=%+v", found, reclaimedAfterRecover)
	}
	if err := backend.Ack(queueName, reclaimedAfterRecover.MessageID, reclaimedAfterRecover.Receipt); err != nil {
		t.Fatalf("ack recovered message: %v", err)
	}

	stats, err = backend.Stats(queueName)
	if err != nil {
		t.Fatalf("final stats: %v", err)
	}
	if stats.Pending != 0 || stats.Processing != 0 || stats.Recoveries != 1 {
		t.Fatalf("final stats = %+v, want pending=0 processing=0 recoveries=1", stats)
	}
}

func TestBackendRecoverExpiredPreservesClaimOrder(t *testing.T) {
	backend := NewBackend()
	queueName := "jobs"
	now := time.Unix(1711601000, 0).UTC()
	payloads := []string{"first", "second", "third", "fourth", "fifth", "sixth", "seventh"}

	if err := backend.CreateQueue(queueName); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	for i, payload := range payloads {
		if err := backend.Enqueue(queueName, model.QueueMessage{
			ID:        payload,
			Data:      payload,
			Timestamp: now.Add(time.Duration(i) * time.Second),
		}); err != nil {
			t.Fatalf("enqueue %q: %v", payload, err)
		}
	}

	for _, payload := range payloads[:len(payloads)-1] {
		claimed, found, err := backend.Claim(queueName, model.ClaimRequest{LeaseDuration: time.Second})
		if err != nil {
			t.Fatalf("claim %q: %v", payload, err)
		}
		if !found || claimed.MessageID != payload {
			t.Fatalf("unexpected claim result for %q: found=%v claimed=%+v", payload, found, claimed)
		}
	}

	recovered, err := backend.RecoverExpired(queueName, time.Now().UTC().Add(2*time.Second), 0)
	if err != nil {
		t.Fatalf("recover expired messages: %v", err)
	}
	if recovered != len(payloads)-1 {
		t.Fatalf("recovered count = %d, want %d", recovered, len(payloads)-1)
	}

	for _, want := range payloads {
		claimed, found, err := backend.Claim(queueName, model.ClaimRequest{})
		if err != nil {
			t.Fatalf("claim after recovery for %q: %v", want, err)
		}
		if !found {
			t.Fatalf("expected recovered queue to yield %q", want)
		}
		if claimed.MessageID != want {
			t.Fatalf("claim order after recovery = %q, want %q", claimed.MessageID, want)
		}
	}
	if _, found, err := backend.Claim(queueName, model.ClaimRequest{}); err != nil {
		t.Fatalf("claim empty recovered queue: %v", err)
	} else if found {
		t.Fatal("expected recovered queue to be empty after draining")
	}
}
