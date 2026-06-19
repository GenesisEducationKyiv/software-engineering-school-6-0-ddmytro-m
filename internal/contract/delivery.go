// Package contract defines the message contract shared between the server and
// mailer services. It deliberately has no infrastructure or domain dependencies
// so both services can depend on it without coupling to each other's internals.
package contract

// DeliveryStream is the Redis stream used for sending email delivery messages.
const DeliveryStream = "messages:delivery"

// EventType represents the type of event in a delivery message.
type EventType string

// Event types for email delivery.
const (
	EventNewRelease        EventType = "new_release"
	EventRepoMoved         EventType = "repo_moved"
	EventEmailVerification EventType = "email_verification"
)

// DeliveryMessage represents a message sent to the delivery stream.
type DeliveryMessage struct {
	Event   EventType      `json:"event"`
	Email   string         `json:"email"`
	Repo    string         `json:"repo,omitempty"`
	Release string         `json:"release,omitempty"`
	Payload map[string]any `json:"payload,omitempty"`
}
