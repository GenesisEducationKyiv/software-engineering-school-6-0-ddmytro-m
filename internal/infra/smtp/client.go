package smtp

import (
	"context"
	"time"

	"github.com/wneessen/go-mail"
)

type Client struct {
	host        string
	port        int
	username    string
	password    string
	from        string
	senderEmail string
}

func NewClient(host string, port int, username, password, from, senderEmail string) *Client {
	return &Client{
		host:        host,
		port:        port,
		username:    username,
		password:    password,
		from:        from,
		senderEmail: senderEmail,
	}
}

func (c *Client) SendEmail(ctx context.Context, to, subject, body string) error {
	m := mail.NewMsg()

	// try to parse c.from as a full address (e.g., "Name <email@domain.com>").
	// fall back to formatting it using the senderEmail if c.from is just a name.
	if err := m.From(c.from); err != nil {
		if err := m.FromFormat(c.from, c.senderEmail); err != nil {
			if err := m.From(c.senderEmail); err != nil {
				return err
			}
		}
	}

	if err := m.To(to); err != nil {
		return err
	}

	m.Subject(subject)
	m.SetBodyString(mail.TypeTextPlain, body)

	opts := []mail.Option{
		mail.WithPort(c.port),
		mail.WithSMTPAuth(mail.SMTPAuthPlain),
		mail.WithUsername(c.username),
		mail.WithPassword(c.password),
		mail.WithTimeout(15 * time.Second),
	}

	if c.port == 465 {
		opts = append(opts, mail.WithSSL())
	}

	client, err := mail.NewClient(c.host, opts...)
	if err != nil {
		return err
	}

	return client.DialAndSendWithContext(ctx, m)
}
