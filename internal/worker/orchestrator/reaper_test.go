//go:build unit

package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReaper_CompensatesStaleTokens(t *testing.T) {
	store := newFakeStore()
	store.stale = []string{"tok1", "tok2"}

	r := NewReaper(store, time.Hour, 30*time.Minute)
	r.sweep(context.Background())

	if len(store.canceled) != 2 || len(store.compensated) != 2 {
		t.Fatalf("expected both stale sagas compensated, got canceled=%v compensated=%v", store.canceled, store.compensated)
	}
}

func TestReaper_NoStaleTokens_NoOp(t *testing.T) {
	store := newFakeStore()

	r := NewReaper(store, time.Hour, 30*time.Minute)
	r.sweep(context.Background())

	if len(store.compensated) != 0 {
		t.Errorf("expected no compensation, got %v", store.compensated)
	}
}

func TestReaper_QueryError_LogsAndReturns(t *testing.T) {
	store := newFakeStore()
	store.staleErr = errors.New("db down")

	r := NewReaper(store, time.Hour, 30*time.Minute)
	r.sweep(context.Background())

	if len(store.compensated) != 0 {
		t.Errorf("expected no compensation on query error, got %v", store.compensated)
	}
}

func TestReaper_CompensateError_ContinuesRemainingTokens(t *testing.T) {
	store := newFakeStore()
	store.stale = []string{"tok1", "tok2"}
	store.compensateFailToken = map[string]bool{"tok1": true}

	r := NewReaper(store, time.Hour, 30*time.Minute)
	r.sweep(context.Background())

	if len(store.compensated) != 1 || store.compensated[0] != "tok2" {
		t.Errorf("expected tok2 compensated despite tok1 failing, got %v", store.compensated)
	}
}
