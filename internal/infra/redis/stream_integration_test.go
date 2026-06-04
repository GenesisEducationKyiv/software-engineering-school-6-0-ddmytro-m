//go:build integration

package redis

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

func TestStream_Integration(t *testing.T) {
	rc := getCleanRedis(t)
	ctx := context.Background()
	streamName := "test-stream"
	groupName := "test-group"
	consumerName := "test-consumer"

	stream := NewStream(rc, streamName)

	if err := stream.EnsureConsumerGroup(ctx, groupName); err != nil {
		t.Fatalf("EnsureConsumerGroup failed: %v", err)
	}

	// Idempotency: should ignore BUSYGROUP error
	if err := stream.EnsureConsumerGroup(ctx, groupName); err != nil {
		t.Fatalf("EnsureConsumerGroup failed on second call: %v", err)
	}

	msgPayload := map[string]string{"event": "test"}
	if err := stream.Publish(ctx, msgPayload); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if err := stream.PublishDeadLetter(ctx, msgPayload); err != nil {
		t.Fatalf("PublishDeadLetter failed: %v", err)
	}

	msgs, err := stream.ReadGroup(ctx, groupName, consumerName, 10, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("ReadGroup failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	msgID := msgs[0].ID()
	if msgID == "" {
		t.Error("expected non-empty message ID")
	}

	payload := msgs[0].Payload()
	if string(payload) != `{"event":"test"}` {
		t.Errorf("expected payload `{\"event\":\"test\"}`, got %s", payload)
	}

	if err := stream.Ack(ctx, groupName, msgID); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}

	// Should block and timeout, returning no messages natively mapped to nil
	msgs, err = stream.ReadGroup(ctx, groupName, consumerName, 10, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("ReadGroup failed: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected 0 messages after Ack, got %d", len(msgs))
	}
}

func TestStream_AutoClaim(t *testing.T) {
	rc := getCleanRedis(t)
	ctx := context.Background()
	streamName := "test-stream-autoclaim"
	groupName := "test-group"
	consumer1 := "consumer-1"
	consumer2 := "consumer-2"

	stream := NewStream(rc, streamName)
	if err := stream.EnsureConsumerGroup(ctx, groupName); err != nil {
		t.Fatalf("EnsureConsumerGroup failed: %v", err)
	}

	if err := stream.Publish(ctx, "claim-me"); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Consumer 1 reads it but does not ACK
	msgs, err := stream.ReadGroup(ctx, groupName, consumer1, 1, time.Millisecond)
	if err != nil {
		t.Fatalf("ReadGroup failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	// Ensure the message has a tiny idle time before claiming
	time.Sleep(5 * time.Millisecond)

	// Consumer 2 claims from consumer 1
	claimedMsgs, next, err := stream.AutoClaim(ctx, groupName, consumer2, 0, "0-0", 10)
	if err != nil {
		t.Fatalf("AutoClaim failed: %v", err)
	}
	if len(claimedMsgs) != 1 {
		t.Fatalf("expected 1 claimed message, got %d", len(claimedMsgs))
	}
	if next == "" {
		t.Error("expected valid next ID")
	}

	payload := claimedMsgs[0].Payload()
	if string(payload) != `"claim-me"` {
		t.Errorf("expected `\"claim-me\"`, got %s", payload)
	}

	if err := stream.Ack(ctx, groupName, claimedMsgs[0].ID()); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}
}

func TestNewStream_NilClient(t *testing.T) {
	if stream := NewStream(nil, "test"); stream != nil {
		t.Error("expected nil Stream when client is nil")
	}
}

func TestMessageAdapter_MissingPayload(t *testing.T) {
	msg := NewMessageAdapter(goredis.XMessage{
		ID: "1-0",
		Values: map[string]interface{}{
			"wrong-field": "value",
		},
	})

	if msg.Payload() != nil {
		t.Errorf("expected nil payload, got %s", msg.Payload())
	}
}

func TestStream_ReadGroupInvalidGroup(t *testing.T) {
	rc := getCleanRedis(t)
	ctx := context.Background()
	stream := NewStream(rc, "stream-no-group")

	_, err := stream.ReadGroup(ctx, "nonexistent-group", "consumer", 1, time.Millisecond)
	if err == nil {
		t.Fatal("expected error reading from nonexistent group")
	}
}

func TestStream_AutoClaimInvalidGroup(t *testing.T) {
	rc := getCleanRedis(t)
	ctx := context.Background()
	stream := NewStream(rc, "stream-no-group-2")

	_, _, err := stream.AutoClaim(ctx, "nonexistent-group", "consumer", 0, "0-0", 10)
	if err == nil {
		t.Fatal("expected error on autoclaim with nonexistent group")
	}
}

func TestStream_PublishMarshalError(t *testing.T) {
	rc := getCleanRedis(t)
	ctx := context.Background()
	stream := NewStream(rc, "test-stream-marshal")

	err := stream.Publish(ctx, make(chan int))
	if err == nil {
		t.Fatal("expected error on unmarshalable publish payload")
	}

	err = stream.PublishDeadLetter(ctx, make(chan int))
	if err == nil {
		t.Fatal("expected error on unmarshalable dead-letter payload")
	}
}
