package client_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/client"
)

func TestClientMutateLocalStreamsTextFile(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tempDir, "blob.txt"), []byte("hello\n\"quoted\""), 0o600); err != nil {
		t.Fatalf("write text payload: %v", err)
	}

	var (
		getCalls    int
		updateCalls int
		mutateCalls int
		updateBody  map[string]any
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/get":
			getCalls++
			w.Header().Set("ETag", `"etag-7"`)
			w.Header().Set("X-Key-Version", "7")
			w.Header().Set("X-Fencing-Token", "19")
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"content":"old","filename":"old.txt"}`)
		case "/v1/update":
			updateCalls++
			if got := r.Header.Get("X-Lease-ID"); got != "lease-1" {
				t.Fatalf("unexpected lease header %q", got)
			}
			if got := r.Header.Get("X-Txn-ID"); got != "txn-1" {
				t.Fatalf("unexpected txn header %q", got)
			}
			if got := r.Header.Get("X-If-State-ETag"); got != "etag-7" {
				t.Fatalf("unexpected if-etag %q", got)
			}
			if got := r.Header.Get("X-If-Version"); got != "7" {
				t.Fatalf("unexpected if-version %q", got)
			}
			if err := json.NewDecoder(r.Body).Decode(&updateBody); err != nil {
				t.Fatalf("decode update body: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(api.UpdateResponse{
				NewVersion:   8,
				NewStateETag: "etag-8",
				Bytes:        48,
			})
		case "/v1/mutate":
			mutateCalls++
			t.Fatalf("remote mutate endpoint should not be called")
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL,
		client.WithDisableMTLS(true),
		client.WithEndpointShuffle(false),
		client.WithHTTPClient(srv.Client()),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	res, err := cli.MutateLocal(context.Background(), client.MutateLocalRequest{
		Key:       "orders",
		LeaseID:   "lease-1",
		Mutations: []string{`textfile:/content=blob.txt`, `/filename=blob.txt`},
		Options: client.UpdateOptions{
			Namespace:    "default",
			TxnID:        "txn-1",
			FencingToken: client.Int64(9),
		},
		FileValueBaseDir: tempDir,
	})
	if err != nil {
		t.Fatalf("MutateLocal: %v", err)
	}
	if res.NewVersion != 8 || res.NewStateETag != "etag-8" || res.BytesWritten != 48 {
		t.Fatalf("unexpected mutate local response: %+v", res)
	}
	if getCalls != 1 || updateCalls != 1 || mutateCalls != 0 {
		t.Fatalf("unexpected call counts get=%d update=%d mutate=%d", getCalls, updateCalls, mutateCalls)
	}
	if got := updateBody["content"]; got != "hello\n\"quoted\"" {
		t.Fatalf("unexpected streamed text content %#v", got)
	}
	if got := updateBody["filename"]; got != "blob.txt" {
		t.Fatalf("unexpected filename %#v", got)
	}
}

func TestClientMutateLocalCreatesObjectFromEmptyState(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tempDir, "blob.bin"), []byte{0x00, 0x01, 0x02, 'a'}, 0o600); err != nil {
		t.Fatalf("write binary payload: %v", err)
	}

	var updateBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/get":
			w.WriteHeader(http.StatusNoContent)
		case "/v1/update":
			if got := r.Header.Get("X-If-State-ETag"); got != "" {
				t.Fatalf("expected empty if-etag header, got %q", got)
			}
			if got := r.Header.Get("X-If-Version"); got != "" {
				t.Fatalf("expected empty if-version header, got %q", got)
			}
			if err := json.NewDecoder(r.Body).Decode(&updateBody); err != nil {
				t.Fatalf("decode update body: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(api.UpdateResponse{
				NewVersion:   1,
				NewStateETag: "etag-1",
				Bytes:        40,
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL,
		client.WithDisableMTLS(true),
		client.WithEndpointShuffle(false),
		client.WithHTTPClient(srv.Client()),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = cli.MutateLocal(context.Background(), client.MutateLocalRequest{
		Key:       "orders",
		LeaseID:   "lease-1",
		Mutations: []string{`/filename=blob.bin`, `base64file:/payload=blob.bin`},
		Options: client.UpdateOptions{
			FencingToken: client.Int64(9),
		},
		FileValueBaseDir: tempDir,
	})
	if err != nil {
		t.Fatalf("MutateLocal: %v", err)
	}
	if got := updateBody["filename"]; got != "blob.bin" {
		t.Fatalf("unexpected filename %#v", got)
	}
	if got := updateBody["payload"]; got != "AAECYQ==" {
		t.Fatalf("unexpected base64 payload %#v", got)
	}
}

func TestClientMutateLocalRejectsRelativeFileWithoutBaseDir(t *testing.T) {
	t.Parallel()

	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		t.Fatalf("server should not be called")
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL,
		client.WithDisableMTLS(true),
		client.WithEndpointShuffle(false),
		client.WithHTTPClient(srv.Client()),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = cli.MutateLocal(context.Background(), client.MutateLocalRequest{
		Key:       "orders",
		LeaseID:   "lease-1",
		Mutations: []string{`textfile:/content=blob.txt`},
		Options: client.UpdateOptions{
			FencingToken: client.Int64(9),
		},
	})
	if err == nil {
		t.Fatal("expected relative file path error")
	}
	if called {
		t.Fatal("server should not be called")
	}
}
