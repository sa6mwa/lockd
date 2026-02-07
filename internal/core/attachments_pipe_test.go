package core

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"pkt.systems/lockd/internal/storage"
)

func TestStartEncryptedPipeStopsOnContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	defer pr.Close()

	// Keep writer pressure high so the producer blocks without a consumer.
	src := bytes.NewReader(bytes.Repeat([]byte("x"), 1<<20))
	done := startEncryptedPipe(ctx, src, pw, pw)

	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("encrypted pipe producer did not stop after context cancellation")
	}
}

type failAttachmentPutStore struct {
	storage.Backend
}

func (f failAttachmentPutStore) PutObject(context.Context, string, string, io.Reader, storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	return nil, errors.New("put failed")
}

func TestPutAttachmentObjectClosesPipeReaderOnError(t *testing.T) {
	t.Parallel()

	svc := newTestService(t)
	svc.store = failAttachmentPutStore{Backend: svc.store}

	pr, pw := io.Pipe()
	writeDone := make(chan error, 1)
	go func() {
		_, err := pw.Write([]byte("payload"))
		writeDone <- err
	}()

	err := svc.putAttachmentObject(context.Background(), "default", "obj", pr, storage.ContentTypeOctetStream, nil)
	if err == nil {
		t.Fatal("expected put error")
	}

	select {
	case werr := <-writeDone:
		if werr == nil {
			t.Fatal("expected writer to fail after pipe reader close")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("writer remained blocked after putAttachmentObject error")
	}
}
