package azure

import (
	"errors"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

func TestIsCopySourceAuthError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "cannot verify copy source",
			err: &azcore.ResponseError{
				StatusCode: http.StatusUnauthorized,
				ErrorCode:  "CannotVerifyCopySource",
			},
			want: true,
		},
		{
			name: "copy source no auth",
			err: &azcore.ResponseError{
				StatusCode: http.StatusUnauthorized,
				ErrorCode:  "NoAuthenticationInformation",
			},
			want: true,
		},
		{
			name: "other response error",
			err: &azcore.ResponseError{
				StatusCode: http.StatusUnauthorized,
				ErrorCode:  "AuthenticationFailed",
			},
			want: false,
		},
		{
			name: "wrapped response error",
			err: errors.Join(
				errors.New("outer"),
				&azcore.ResponseError{StatusCode: http.StatusUnauthorized, ErrorCode: "CannotVerifyCopySource"},
			),
			want: true,
		},
		{
			name: "non response error",
			err:  errors.New("plain"),
			want: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isCopySourceAuthError(tc.err); got != tc.want {
				t.Fatalf("isCopySourceAuthError() = %v, want %v", got, tc.want)
			}
		})
	}
}
