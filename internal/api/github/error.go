package github

import "fmt"

// NetworkError represents an error that occurred during a network operation.
type NetworkError struct{ Err error }

func (e *NetworkError) Error() string { return fmt.Sprintf("network failure: %v", e.Err) }
func (e *NetworkError) Unwrap() error { return e.Err }

// DecodingError represents an error that occurred while decoding a response.
type DecodingError struct{ Err error }

func (e *DecodingError) Error() string { return fmt.Sprintf("failed to decode response: %v", e.Err) }
func (e *DecodingError) Unwrap() error { return e.Err }

// APIError represents an error returned by the GitHub API.
type APIError struct {
	StatusCode       int    `json:"-"`
	Message          string `json:"message"`
	DocumentationURL string `json:"documentation_url"`
}

func (e *APIError) Error() string { return fmt.Sprintf("github api error: %s", e.Message) }

// UnexpectedStatusError represents an unexpected HTTP status code received from the API.
type UnexpectedStatusError struct{ StatusCode int }

func (e *UnexpectedStatusError) Error() string {
	return fmt.Sprintf("unexpected status code: %d", e.StatusCode)
}
