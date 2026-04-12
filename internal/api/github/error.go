package github

import "fmt"

type NetworkError struct{ Err error }

func (e *NetworkError) Error() string { return fmt.Sprintf("network failure: %v", e.Err) }
func (e *NetworkError) Unwrap() error { return e.Err }

type DecodingError struct{ Err error }

func (e *DecodingError) Error() string { return fmt.Sprintf("failed to decode response: %v", e.Err) }
func (e *DecodingError) Unwrap() error { return e.Err }

type APIError struct {
	StatusCode       int    `json:"-"`
	Message          string `json:"message"`
	DocumentationURL string `json:"documentation_url"`
}

func (e *APIError) Error() string { return fmt.Sprintf("github api error: %s", e.Message) }

type UnexpectedStatusError struct{ StatusCode int }

func (e *UnexpectedStatusError) Error() string {
	return fmt.Sprintf("unexpected status code: %d", e.StatusCode)
}

type cachedError struct {
	Type             string `json:"type"`
	Message          string `json:"message,omitempty"`
	Code             int    `json:"code,omitempty"`
	DocumentationURL string `json:"documentation_url,omitempty"`
}
