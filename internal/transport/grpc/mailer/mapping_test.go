//go:build unit

package mailer

import (
	"testing"

	mailerv1 "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/grpcgen/mailer/v1"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
)

func TestToProto_MapsFieldsAndPayload(t *testing.T) {
	msg := mq.DeliveryMessage{
		Event:   mq.EventEmailVerification,
		Email:   "u@example.com",
		Repo:    "owner/repo",
		Release: "v1.2.3",
		Payload: map[string]any{"token": "tok123", "ignored": 7},
	}

	got := toProto(msg)

	if got.GetType() != mailerv1.EmailType_EMAIL_TYPE_EMAIL_VERIFICATION {
		t.Errorf("type = %v, want EMAIL_VERIFICATION", got.GetType())
	}
	if got.GetEmail() != msg.Email || got.GetRepo() != msg.Repo || got.GetRelease() != msg.Release {
		t.Errorf("scalar fields not mapped: %+v", got)
	}
	if got.GetPayload()["token"] != "tok123" {
		t.Errorf("payload token = %q, want tok123", got.GetPayload()["token"])
	}
	if _, ok := got.GetPayload()["ignored"]; ok {
		t.Error("non-string payload value should be dropped")
	}
}

func TestFromProto_MapsFields(t *testing.T) {
	cmd := &mailerv1.DeliveryCommand{
		Type:    mailerv1.EmailType_EMAIL_TYPE_NEW_RELEASE,
		Email:   "u@example.com",
		Repo:    "owner/repo",
		Payload: map[string]string{"token": "tok"},
	}

	got := fromProto(cmd)

	if got.Event != mq.EventNewRelease {
		t.Errorf("event = %q, want %q", got.Event, mq.EventNewRelease)
	}
	if got.Payload["token"] != "tok" {
		t.Errorf("payload token = %v, want tok", got.Payload["token"])
	}
}

func TestFromProto_UnknownEnum_EmptyEvent(t *testing.T) {
	got := fromProto(&mailerv1.DeliveryCommand{Type: mailerv1.EmailType_EMAIL_TYPE_UNSPECIFIED})
	if got.Event != "" {
		t.Errorf("event = %q, want empty (poison)", got.Event)
	}
}

func TestProtoRoundTrip_PreservesEvent(t *testing.T) {
	events := []mq.EventType{mq.EventNewRelease, mq.EventRepoMoved, mq.EventEmailVerification}
	for _, ev := range events {
		in := mq.DeliveryMessage{Event: ev, Email: "e@x.com", Repo: "o/r", Release: "v1"}
		out := fromProto(toProto(in))
		if out.Event != in.Event || out.Email != in.Email || out.Repo != in.Repo || out.Release != in.Release {
			t.Errorf("round trip mismatch for %q: %+v", ev, out)
		}
	}
}
