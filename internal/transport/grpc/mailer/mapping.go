// Package mailer provides the gRPC server and client for the mailer service,
// an alternative to the RabbitMQ commands exchange on the notifier -> mailer link.
package mailer

import (
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	workermailer "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/worker/mailer"
	mailerv1 "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/proto/mailer/v1"
)

// eventToProto maps a domain email type to its protobuf enum.
var eventToProto = map[mq.EventType]mailerv1.EmailType{
	mq.EventNewRelease:        mailerv1.EmailType_EMAIL_TYPE_NEW_RELEASE,
	mq.EventRepoMoved:         mailerv1.EmailType_EMAIL_TYPE_REPO_MOVED,
	mq.EventEmailVerification: mailerv1.EmailType_EMAIL_TYPE_EMAIL_VERIFICATION,
}

// protoToEvent is the inverse of eventToProto.
var protoToEvent = map[mailerv1.EmailType]mq.EventType{
	mailerv1.EmailType_EMAIL_TYPE_NEW_RELEASE:        mq.EventNewRelease,
	mailerv1.EmailType_EMAIL_TYPE_REPO_MOVED:         mq.EventRepoMoved,
	mailerv1.EmailType_EMAIL_TYPE_EMAIL_VERIFICATION: mq.EventEmailVerification,
}

// outcomeToProto maps a workermailer outcome label to its gRPC response code.
var outcomeToProto = map[string]mailerv1.DeliveryOutcome{
	workermailer.OutcomeDelivered: mailerv1.DeliveryOutcome_DELIVERY_OUTCOME_DELIVERED,
	workermailer.OutcomeFailed:    mailerv1.DeliveryOutcome_DELIVERY_OUTCOME_FAILED,
	workermailer.OutcomePoison:    mailerv1.DeliveryOutcome_DELIVERY_OUTCOME_POISON,
}

// protoToOutcome is the inverse of outcomeToProto. An unrecognized code (e.g.
// DELIVERY_OUTCOME_UNSPECIFIED from an older peer) maps to OutcomeFailed so
// the client retries rather than silently treating it as delivered.
var protoToOutcome = map[mailerv1.DeliveryOutcome]string{
	mailerv1.DeliveryOutcome_DELIVERY_OUTCOME_DELIVERED: workermailer.OutcomeDelivered,
	mailerv1.DeliveryOutcome_DELIVERY_OUTCOME_FAILED:    workermailer.OutcomeFailed,
	mailerv1.DeliveryOutcome_DELIVERY_OUTCOME_POISON:    workermailer.OutcomePoison,
}

// toProto converts a domain delivery message into a gRPC command.
func toProto(msg mq.DeliveryMessage) *mailerv1.DeliveryCommand {
	payload := make(map[string]string, len(msg.Payload))
	for k, v := range msg.Payload {
		if s, ok := v.(string); ok {
			payload[k] = s
		}
	}
	return &mailerv1.DeliveryCommand{
		Type:    eventToProto[msg.Event],
		Email:   msg.Email,
		Repo:    msg.Repo,
		Release: msg.Release,
		Payload: payload,
	}
}

// fromProto converts a gRPC command into a domain delivery message. An unknown
// enum maps to an empty event type, which the mailer treats as a poison command.
func fromProto(cmd *mailerv1.DeliveryCommand) mq.DeliveryMessage {
	payload := make(map[string]any, len(cmd.GetPayload()))
	for k, v := range cmd.GetPayload() {
		payload[k] = v
	}
	return mq.DeliveryMessage{
		Event:   protoToEvent[cmd.GetType()],
		Email:   cmd.GetEmail(),
		Repo:    cmd.GetRepo(),
		Release: cmd.GetRelease(),
		Payload: payload,
	}
}
