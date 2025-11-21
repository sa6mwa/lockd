package httpapi

import (
	"net/http"
	"testing"

	"pkt.systems/lockd/internal/core"
)

func TestConvertCoreError(t *testing.T) {
	err := core.Failure{
		Code:       "lease_required",
		Detail:     "active lease required",
		HTTPStatus: http.StatusForbidden,
		RetryAfter: 3,
		Version:    2,
		ETag:       "etag",
	}
	httpErr := convertCoreError(err).(httpError)
	if httpErr.Status != http.StatusForbidden || httpErr.Code != "lease_required" || httpErr.Detail != "active lease required" {
		t.Fatalf("unexpected httpErr: %+v", httpErr)
	}
	if httpErr.RetryAfter != 3 || httpErr.Version != 2 || httpErr.ETag != "etag" {
		t.Fatalf("expected metadata to propagate")
	}
}
