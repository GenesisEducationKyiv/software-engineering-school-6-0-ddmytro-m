//go:build unit

package mq

import (
	"encoding/json"
	"testing"
)

func TestDeliveryMessage_JSONRoundTrip(t *testing.T) {
	in := DeliveryMessage{
		Event:   EventNewRelease,
		Email:   "u@example.com",
		Repo:    "owner/repo",
		Release: "v1.0.0",
		Payload: map[string]any{"token": "abc"},
	}

	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out DeliveryMessage
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.Event != in.Event || out.Email != in.Email || out.Repo != in.Repo || out.Release != in.Release {
		t.Errorf("round trip mismatch: got %+v, want %+v", out, in)
	}
	if out.Payload["token"] != "abc" {
		t.Errorf("payload token = %v, want abc", out.Payload["token"])
	}
}

func TestDeliveryMessage_OmitsEmptyFields(t *testing.T) {
	data, err := json.Marshal(DeliveryMessage{Event: EventEmailVerification, Email: "u@example.com"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	for _, k := range []string{"repo", "release", "payload"} {
		if _, ok := m[k]; ok {
			t.Errorf("expected %q to be omitted, got %v", k, m[k])
		}
	}
}