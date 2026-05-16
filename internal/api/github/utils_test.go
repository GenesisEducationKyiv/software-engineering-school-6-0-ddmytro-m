//go:build unit || integration

package github

import "net/http"

// mockTransport lets us intercept the RoundTrip to simulate GitHub responses
type mockTransport struct {
	roundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTripFunc(req)
}
