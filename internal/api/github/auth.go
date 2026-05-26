package github

import "net/http"

// AuthTransport handles injecting the Bearer token into requests.
type AuthTransport struct {
	Transport http.RoundTripper
	Token     string
}

// NewAuthTransport creates a new AuthTransport with the provided configuration.
func NewAuthTransport(transport http.RoundTripper, token string) *AuthTransport {
	return &AuthTransport{
		Transport: transport,
		Token:     token,
	}
}

// RoundTrip implements the http.RoundTripper interface, adding the Authorization header
// to the request if a token is present.
func (t *AuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone the request to avoid modifying the original (required by RoundTripper rules)
	clonedReq := req.Clone(req.Context())

	if t.Token != "" {
		clonedReq.Header.Set("Authorization", "Bearer "+t.Token)
	}

	// Fallback to default transport if none is provided
	transport := t.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	return transport.RoundTrip(clonedReq)
}
