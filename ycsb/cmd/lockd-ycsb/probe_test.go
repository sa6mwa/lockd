package main

import (
	"errors"
	"testing"
)

func TestIsProbeExpectedReadError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "not found", err: errors.New("lockd ycsb: key users/1 not found"), want: true},
		{name: "not exist", err: errors.New("resource does not exist"), want: true},
		{name: "other", err: errors.New("permission denied"), want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isProbeExpectedReadError(tc.err)
			if got != tc.want {
				t.Fatalf("isProbeExpectedReadError()=%v want=%v err=%v", got, tc.want, tc.err)
			}
		})
	}
}
