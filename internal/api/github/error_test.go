package github

import (
	"errors"
	"testing"
)

func TestNetworkError_MessageAndUnwrap(t *testing.T) {
	inner := errors.New("connection refused")
	e := &NetworkError{Err: inner}

	if want := "network failure: connection refused"; e.Error() != want {
		t.Errorf("Error() = %q, want %q", e.Error(), want)
	}
	if !errors.Is(e, inner) {
		t.Error("errors.Is should reach inner error via Unwrap")
	}
}

func TestDecodingError_MessageAndUnwrap(t *testing.T) {
	inner := errors.New("unexpected EOF")
	e := &DecodingError{Err: inner}

	if want := "failed to decode response: unexpected EOF"; e.Error() != want {
		t.Errorf("Error() = %q, want %q", e.Error(), want)
	}
	if !errors.Is(e, inner) {
		t.Error("errors.Is should reach inner error via Unwrap")
	}
}

func TestAPIError_Message(t *testing.T) {
	e := &APIError{StatusCode: 404, Message: "Not Found"}
	if want := "github api error: Not Found"; e.Error() != want {
		t.Errorf("Error() = %q, want %q", e.Error(), want)
	}
}

func TestUnexpectedStatusError_Message(t *testing.T) {
	e := &UnexpectedStatusError{StatusCode: 503}
	if want := "unexpected status code: 503"; e.Error() != want {
		t.Errorf("Error() = %q, want %q", e.Error(), want)
	}
}
