package core

import "fmt"

// Failure captures transport-neutral error details that adapters can map to
// HTTP, gRPC, or other protocols.
type Failure struct {
	Code       string
	Detail     string
	RetryAfter int64 // seconds
	Version    int64
	ETag       string
	HTTPStatus int // optional hint for HTTP adapters
}

func (f Failure) Error() string {
	if f.Detail != "" {
		return fmt.Sprintf("%s: %s", f.Code, f.Detail)
	}
	return f.Code
}
