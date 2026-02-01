package transport

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"pkt.systems/lockd/internal/core"
)

// ToGRPC maps core.Failure to a gRPC status with details encoded in the
// status message; transports can add richer metadata if needed.
func ToGRPC(err error) error {
	var failure core.Failure
	if !errorAs(err, &failure) {
		return err
	}
	code := codes.InvalidArgument
	switch failure.HTTPStatus {
	case 0, 400:
		code = codes.InvalidArgument
	case 401, 403:
		code = codes.PermissionDenied
	case 404:
		code = codes.NotFound
	case 409:
		code = codes.Aborted
	case 429:
		code = codes.ResourceExhausted
	case 500:
		code = codes.Internal
	case 501:
		code = codes.Unimplemented
	case 503:
		code = codes.Unavailable
	default:
		if failure.RetryAfter > 0 {
			code = codes.Unavailable
		}
	}
	msg := failure.Code
	if failure.Detail != "" {
		msg += ": " + failure.Detail
	}
	st := status.New(code, msg)
	// Additional metadata (retry_after, version, etag) could be attached as details later.
	return st.Err()
}
