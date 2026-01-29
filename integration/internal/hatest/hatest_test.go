package hatest

import (
	"errors"
	"net/http"
	"testing"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
)

func TestIsRetryableProbeErrorWaiting(t *testing.T) {
	apiErr := &lockdclient.APIError{
		Status: http.StatusConflict,
		Response: api.ErrorResponse{
			ErrorCode: "waiting",
			Detail:    "lease already held",
		},
	}
	if !isRetryableProbeError(apiErr) {
		t.Fatalf("expected waiting to be retryable")
	}
}

func TestIsRetryableProbeErrorNonWaiting(t *testing.T) {
	apiErr := &lockdclient.APIError{
		Status: http.StatusServiceUnavailable,
		Response: api.ErrorResponse{
			ErrorCode: "node_passive",
		},
	}
	if isRetryableProbeError(apiErr) {
		t.Fatalf("unexpected retryable for non-waiting error")
	}
	if isRetryableProbeError(errors.New("boom")) {
		t.Fatalf("unexpected retryable for non-API error")
	}
}
