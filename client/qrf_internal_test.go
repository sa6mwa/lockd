package client

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"pkt.systems/lockd/api"
)

func TestDecodeErrorCapturesQRFHeaders(t *testing.T) {
	t.Helper()
	c := &Client{}
	req := httptest.NewRequest(http.MethodPost, "http://lockd/v1/acquire", nil)
	resp := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewBufferString(`{"error":"throttled","retry_after_seconds":1}`)),
		Request:    req,
	}
	resp.Header.Set("Content-Type", "application/json")
	resp.Header.Set("Retry-After", "2")
	resp.Header.Set("X-Lockd-QRF-State", "engaged")

	err := c.decodeError(resp)
	t.Helper()
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("expected *APIError, got %T", err)
	}
	if got, want := apiErr.RetryAfterDuration(), 2*time.Second; got != want {
		t.Fatalf("retry-after duration mismatch: got %s want %s", got, want)
	}
	if got, want := apiErr.QRFState, "engaged"; got != want {
		t.Fatalf("qrf state mismatch: got %q want %q", got, want)
	}
}

func TestParseRetryAfterHeader(t *testing.T) {
	future := time.Now().Add(1500 * time.Millisecond).UTC()
	for name, tc := range map[string]struct {
		header string
		want   time.Duration
	}{
		"empty":          {"", 0},
		"seconds":        {"3", 3 * time.Second},
		"fractional":     {"1.5", 1500 * time.Millisecond},
		"http-date":      {future.Format(http.TimeFormat), time.Until(future)},
		"invalid":        {"bogus", 0},
		"zero_seconds":   {"0", 0},
		"negative":       {"-1", 0},
		"past_http_date": {time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat), 0},
	} {
		t.Run(name, func(t *testing.T) {
			got := parseRetryAfterHeader(tc.header)
			if tc.header == future.Format(http.TimeFormat) {
				if got <= 0 {
					t.Fatalf("expected positive delay for http-date header, got %s", got)
				}
				if got > 2*time.Second {
					t.Fatalf("unexpectedly large delay for http-date header: %s", got)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("unexpected delay: got %s want %s", got, tc.want)
			}
		})
	}
}

func TestAPIErrorRetryAfterFallback(t *testing.T) {
	err := &APIError{
		Status:   http.StatusTooManyRequests,
		Response: api.ErrorResponse{RetryAfterSeconds: 7},
	}
	if got := err.RetryAfterDuration(); got != 7*time.Second {
		t.Fatalf("retry-after fallback mismatch: got %s want 7s", got)
	}
	if got := retryAfterFromError(err); got != 7*time.Second {
		t.Fatalf("retryAfterFromError mismatch: got %s want 7s", got)
	}
	if state := qrfStateFromError(err); state != "" {
		t.Fatalf("expected empty QRF state, got %q", state)
	}

	err.QRFState = "soft_arm"
	if state := qrfStateFromError(err); state != "soft_arm" {
		t.Fatalf("expected soft_arm, got %q", state)
	}
}

func TestIsAlreadyExistsErrorHelpers(t *testing.T) {
	err := &APIError{
		Status:   http.StatusConflict,
		Response: api.ErrorResponse{ErrorCode: "already_exists"},
	}
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected errors.Is(..., ErrAlreadyExists) to match")
	}
	if !IsAlreadyExists(err) {
		t.Fatalf("expected IsAlreadyExists to match")
	}
	wrapped := errors.Join(err)
	if !IsAlreadyExists(wrapped) {
		t.Fatalf("expected IsAlreadyExists to match wrapped APIError")
	}
	var apiErr *APIError
	if !errors.As(wrapped, &apiErr) {
		t.Fatalf("expected errors.As to recover APIError from wrapped error")
	}
}
