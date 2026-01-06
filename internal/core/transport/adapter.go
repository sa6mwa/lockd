package transport

import (
	"errors"
	"net/http"

	"pkt.systems/lockd/internal/core"
)

// HTTPError converts a core.Failure into an HTTP-aware error struct.
// Handlers can wrap this in their own response writers.
type HTTPError struct {
	Status     int
	Code       string
	Detail     string
	RetryAfter int64
	Version    int64
	ETag       string
	LeaderEndpoint string
}

// ToHTTP maps a core error into HTTP-friendly fields.
func ToHTTP(err error) (*HTTPError, bool) {
	var failure core.Failure
	if !errorAs(err, &failure) {
		return nil, false
	}
	status := failure.HTTPStatus
	if status == 0 {
		status = http.StatusBadRequest
	}
	return &HTTPError{
		Status:     status,
		Code:       failure.Code,
		Detail:     failure.Detail,
		RetryAfter: failure.RetryAfter,
		Version:    failure.Version,
		ETag:       failure.ETag,
		LeaderEndpoint: failure.LeaderEndpoint,
	}, true
}

// errorAs is a tiny local helper to avoid importing errors in callers.
func errorAs(err error, target interface{}) bool {
	return errors.As(err, target)
}
