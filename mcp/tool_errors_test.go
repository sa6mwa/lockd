package mcp

import (
	"errors"
	"testing"
)

func TestClassifyToolErrorValidationHeuristics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		code string
	}{
		{
			name: "mutually exclusive",
			err:  errors.New("payload_text and payload_base64 are mutually exclusive"),
			code: "invalid_argument",
		},
		{
			name: "oversized inline payload",
			err:  errors.New("payload 1024 bytes exceeds mcp.inline_max_bytes=512"),
			code: "invalid_argument",
		},
		{
			name: "decode failure",
			err:  errors.New("decode chunk_base64: illegal base64 data at input byte 0"),
			code: "invalid_argument",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := classifyToolError(tc.err)
			if env.ErrorCode != tc.code {
				t.Fatalf("expected error_code=%q, got %+v", tc.code, env)
			}
		})
	}
}
