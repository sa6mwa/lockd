package lockd

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/magiconair/properties"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
)

func TestParseConfigPhaseMetrics(t *testing.T) {
	p := properties.NewProperties()
	p.Set(propEndpoints, "https://127.0.0.1:9341")
	p.Set(propDisableMTLS, "true")
	p.Set(propPhaseMetrics, "true")

	cfg, err := parseConfig(p)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if !cfg.phaseMetrics {
		t.Fatalf("expected phaseMetrics to be true")
	}
}

func TestRecordPhaseInvokesRecorder(t *testing.T) {
	called := false
	cfg := &driverConfig{
		phaseRecorder: func(op string, start time.Time, dur time.Duration) {
			called = true
			if op != "LOCKD_ACQUIRE" {
				t.Fatalf("unexpected op %q", op)
			}
			if dur <= 0 {
				t.Fatalf("expected positive duration")
			}
		},
	}
	db := &lockdDB{cfg: cfg}
	start := time.Now().Add(-time.Millisecond)
	db.recordPhase("LOCKD_ACQUIRE", start)
	if !called {
		t.Fatalf("expected recorder to be called")
	}
}

func TestRecordPhaseNoop(t *testing.T) {
	db := &lockdDB{cfg: &driverConfig{}}
	db.recordPhase("LOCKD_ACQUIRE", time.Now())
}

func TestIsAttachmentNotFoundError(t *testing.T) {
	t.Run("status 404", func(t *testing.T) {
		err := &lockdclient.APIError{
			Status:   http.StatusNotFound,
			Response: api.ErrorResponse{ErrorCode: "unexpected_code"},
		}
		if !isAttachmentNotFoundError(err) {
			t.Fatalf("expected true for 404 APIError")
		}
	})
	t.Run("error code", func(t *testing.T) {
		err := &lockdclient.APIError{
			Status:   http.StatusConflict,
			Response: api.ErrorResponse{ErrorCode: "attachment_not_found"},
		}
		if !isAttachmentNotFoundError(err) {
			t.Fatalf("expected true for attachment_not_found code")
		}
	})
	t.Run("non-matching", func(t *testing.T) {
		err := &lockdclient.APIError{
			Status:   http.StatusConflict,
			Response: api.ErrorResponse{ErrorCode: "lease_conflict"},
		}
		if isAttachmentNotFoundError(err) {
			t.Fatalf("expected false for non-not-found APIError")
		}
	})
}

func TestIsStateNotFoundError(t *testing.T) {
	t.Run("api 404", func(t *testing.T) {
		err := &lockdclient.APIError{
			Status:   http.StatusNotFound,
			Response: api.ErrorResponse{ErrorCode: "not_found"},
		}
		if !isStateNotFoundError(err) {
			t.Fatalf("expected true for API 404 not_found")
		}
	})
	t.Run("message", func(t *testing.T) {
		err := errors.New("lockd ycsb: key usertable/user0001 not found")
		if !isStateNotFoundError(err) {
			t.Fatalf("expected true for not-found message")
		}
	})
	t.Run("negative", func(t *testing.T) {
		err := &lockdclient.APIError{
			Status:   http.StatusConflict,
			Response: api.ErrorResponse{ErrorCode: "lease_conflict"},
		}
		if isStateNotFoundError(err) {
			t.Fatalf("expected false for non-not-found API error")
		}
	})
}

func TestIsStateReadRetryable(t *testing.T) {
	t.Run("staged state missing", func(t *testing.T) {
		err := &lockdclient.APIError{
			Status:   http.StatusConflict,
			Response: api.ErrorResponse{ErrorCode: "staged_state_missing"},
		}
		if !isStateReadRetryable(err) {
			t.Fatalf("expected staged_state_missing to be retryable")
		}
	})

	t.Run("non-retryable code", func(t *testing.T) {
		err := &lockdclient.APIError{
			Status:   http.StatusConflict,
			Response: api.ErrorResponse{ErrorCode: "lease_conflict"},
		}
		if isStateReadRetryable(err) {
			t.Fatalf("expected non-retryable code to return false")
		}
	})
}

func TestWaitStateReadRetryContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitStateReadRetry(ctx, 0); err == nil {
		t.Fatalf("expected context cancellation error")
	}
}
