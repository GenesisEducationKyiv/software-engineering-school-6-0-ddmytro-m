package github

import (
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestRateLimits_IsValid(t *testing.T) {
	reset := time.Now().Add(time.Hour)
	cases := []struct {
		name  string
		rl    RateLimits
		valid bool
	}{
		{"fully populated", RateLimits{Limit: 5000, Remaining: 4999, ResetAt: reset}, true},
		{"limit -1 (header absent)", RateLimits{Limit: -1, Remaining: 100, ResetAt: reset}, false},
		{"remaining -1 (header absent)", RateLimits{Limit: 5000, Remaining: -1, ResetAt: reset}, false},
		{"zero ResetAt", RateLimits{Limit: 5000, Remaining: 100}, false},
		{"all defaults", RateLimits{Limit: -1, Remaining: -1}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.rl.IsValid(); got != tc.valid {
				t.Errorf("IsValid() = %v, want %v", got, tc.valid)
			}
		})
	}
}

func TestGetInt64Header(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  int64
	}{
		{"valid number", "5000", 5000},
		{"zero", "0", 0},
		{"header absent", "", -1},
		{"non-numeric", "abc", -1},
		{"float", "1.5", -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			if tc.value != "" {
				h.Set("X-Test", tc.value)
			}
			if got := getInt64Header(h, "X-Test"); got != tc.want {
				t.Errorf("getInt64Header() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestGetResetTime(t *testing.T) {
	epoch := int64(1_700_000_000)

	t.Run("valid unix timestamp", func(t *testing.T) {
		h := http.Header{}
		h.Set("X-RateLimit-Reset", strconv.FormatInt(epoch, 10))
		got := getResetTime(h)
		if !got.Equal(time.Unix(epoch, 0)) {
			t.Errorf("got %v, want %v", got, time.Unix(epoch, 0))
		}
	})

	t.Run("absent header returns zero time", func(t *testing.T) {
		if got := getResetTime(http.Header{}); !got.IsZero() {
			t.Errorf("expected zero time, got %v", got)
		}
	})

	t.Run("malformed value returns zero time", func(t *testing.T) {
		h := http.Header{}
		h.Set("X-RateLimit-Reset", "not-a-number")
		if got := getResetTime(h); !got.IsZero() {
			t.Errorf("expected zero time, got %v", got)
		}
	})
}

func TestGetRetryAfterTime(t *testing.T) {
	now := time.Now()
	t.Run("absent header returns zero time", func(t *testing.T) {
		if got := getRetryAfterTime(http.Header{}, now); !got.IsZero() {
			t.Errorf("expected zero time, got %v", got)
		}
	})

	t.Run("seconds value", func(t *testing.T) {
		h := http.Header{}
		h.Set("Retry-After", "60")
		got := getRetryAfterTime(h, now)
		if !got.Equal(now.Add(60 * time.Second)) {
			t.Errorf("got %v, want %v", got, now.Add(60*time.Second))
		}
	})

	t.Run("HTTP-date value", func(t *testing.T) {
		future := now.Add(5 * time.Minute).UTC().Truncate(time.Second)
		h := http.Header{}
		h.Set("Retry-After", future.Format(http.TimeFormat))
		if got := getRetryAfterTime(h, now); !got.Equal(future) {
			t.Errorf("got %v, want %v", got, future)
		}
	})

	t.Run("garbage value returns zero time", func(t *testing.T) {
		h := http.Header{}
		h.Set("Retry-After", "garbage$$")
		if got := getRetryAfterTime(h, now); !got.IsZero() {
			t.Errorf("expected zero time, got %v", got)
		}
	})
}

func TestFormatResponse_NilHTTPResponse(t *testing.T) {
	err := errors.New("boom")
	resp := formatResponse[string](nil, "data", err)

	if resp.Data != "data" {
		t.Errorf("Data = %q, want %q", resp.Data, "data")
	}
	if resp.Error != err {
		t.Error("Error field should be preserved")
	}
	if resp.StatusCode != 0 {
		t.Errorf("StatusCode = %d, want 0 for nil response", resp.StatusCode)
	}
}

func TestFormatResponse_PopulatesAllFields(t *testing.T) {
	epoch := int64(1_700_000_000)
	res := fakeResponse(http.StatusOK, "", map[string]string{
		"X-RateLimit-Limit":     "5000",
		"X-RateLimit-Remaining": "4999",
		"X-RateLimit-Reset":     strconv.FormatInt(epoch, 10),
		"ETag":                  `"abc123"`,
	})

	resp := formatResponse(res, "payload", nil)

	if resp.StatusCode != 200 {
		t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
	}
	if resp.Data != "payload" {
		t.Errorf("Data = %q, want payload", resp.Data)
	}
	if resp.ETag != `"abc123"` {
		t.Errorf("ETag = %q, want \"abc123\"", resp.ETag)
	}
	if resp.RateLimits.Limit != 5000 {
		t.Errorf("Limit = %d, want 5000", resp.RateLimits.Limit)
	}
	if resp.RateLimits.Remaining != 4999 {
		t.Errorf("Remaining = %d, want 4999", resp.RateLimits.Remaining)
	}
	if !resp.RateLimits.ResetAt.Equal(time.Unix(epoch, 0)) {
		t.Errorf("ResetAt = %v, want %v", resp.RateLimits.ResetAt, time.Unix(epoch, 0))
	}
}

func TestFormatResponse_MissingRateLimitHeaders(t *testing.T) {
	res := fakeResponse(http.StatusOK, "", nil)
	resp := formatResponse(res, "", nil)

	if resp.RateLimits.Limit != -1 {
		t.Errorf("Limit = %d, want -1 sentinel when header absent", resp.RateLimits.Limit)
	}
	if resp.RateLimits.Remaining != -1 {
		t.Errorf("Remaining = %d, want -1 sentinel when header absent", resp.RateLimits.Remaining)
	}
	if !resp.RateLimits.ResetAt.IsZero() {
		t.Errorf("ResetAt = %v, want zero when header absent", resp.RateLimits.ResetAt)
	}
}
