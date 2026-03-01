package aws

import (
	"errors"
	"fmt"
	"testing"

	smithy "github.com/aws/smithy-go"

	"pkt.systems/lockd/internal/storage"
)

func TestClassifyCopyObjectError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want error
	}{
		{
			name: "nil",
			err:  nil,
			want: nil,
		},
		{
			name: "not_found",
			err:  &smithy.GenericAPIError{Code: "NoSuchKey", Message: "missing"},
			want: storage.ErrNotFound,
		},
		{
			name: "not_found_wrapped",
			err:  fmt.Errorf("copy failed: %w", &smithy.GenericAPIError{Code: "NotFound", Message: "missing"}),
			want: storage.ErrNotFound,
		},
		{
			name: "precondition_failed",
			err:  &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "etag mismatch"},
			want: storage.ErrCASMismatch,
		},
		{
			name: "precondition_failed_wrapped",
			err:  fmt.Errorf("copy failed: %w", &smithy.GenericAPIError{Code: "ConditionalRequestConflict", Message: "etag mismatch"}),
			want: storage.ErrCASMismatch,
		},
		{
			name: "other_error",
			err:  errors.New("boom"),
			want: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := classifyCopyObjectError(tt.err)
			if !errors.Is(got, tt.want) {
				t.Fatalf("classifyCopyObjectError() = %v, want %v", got, tt.want)
			}
		})
	}
}
