package queue

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

func TestStartEncryptedPipeStopsOnContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	defer pr.Close()

	src := bytes.NewReader(bytes.Repeat([]byte("x"), 1<<20))
	done := startEncryptedPipe(ctx, src, pw, pw)

	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("encrypted pipe producer did not stop after context cancellation")
	}
}
