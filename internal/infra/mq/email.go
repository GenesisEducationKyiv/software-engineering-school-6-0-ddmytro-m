// Package mq defines the email delivery command schema shared between the
// notifier (producer) and the mailer (consumer).
package mq

// EventType represents the kind of email a delivery command requests.
type EventType string

// Email delivery command types.
const (
	EventNewRelease        EventType = "new_release"
	EventRepoMoved         EventType = "repo_moved"
	EventEmailVerification EventType = "email_verification"
)

// DeliveryMessage is an email delivery command consumed by the mailer.
type DeliveryMessage struct {
	Event   EventType      `json:"event"`
	Email   string         `json:"email"`
	Repo    string         `json:"repo,omitempty"`
	Release string         `json:"release,omitempty"`
	Payload map[string]any `json:"payload,omitempty"`
}
