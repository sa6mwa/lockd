package httpapi

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/storage"
)

func TestQueueBlockModeLabel(t *testing.T) {
	tests := []struct {
		in   int64
		want string
	}{
		{api.BlockNoWait, "nowait"},
		{0, "forever"},
		{-5, "custom"},
		{7, "7s"},
	}
	for _, tt := range tests {
		if got := queueBlockModeLabel(tt.in); got != tt.want {
			t.Fatalf("queueBlockModeLabel(%d) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestParseMetadataHeaders(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set(headerMetadataQueryHidden, "true")
	mut, err := parseMetadataHeaders(r)
	if err != nil {
		t.Fatalf("parseMetadataHeaders returned error: %v", err)
	}
	if mut.QueryHidden == nil || !*mut.QueryHidden {
		t.Fatalf("expected QueryHidden true, got %+v", mut)
	}
}

func TestParseMetadataHeadersInvalid(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set(headerMetadataQueryHidden, "notabool")
	if _, err := parseMetadataHeaders(r); err == nil {
		t.Fatalf("expected error for invalid header")
	}
}

func TestDecodeMetadataMutation(t *testing.T) {
	body := bytes.NewBufferString(`{"query_hidden":false}`)
	mut, err := decodeMetadataMutation(body)
	if err != nil {
		t.Fatalf("decodeMetadataMutation error: %v", err)
	}
	if mut.QueryHidden == nil || *mut.QueryHidden {
		t.Fatalf("expected QueryHidden false, got %+v", mut)
	}
}

func TestDecodeMetadataMutationEmpty(t *testing.T) {
	mut, err := decodeMetadataMutation(io.NopCloser(bytes.NewReader(nil)))
	if err != nil {
		t.Fatalf("decodeMetadataMutation empty error: %v", err)
	}
	if !mut.empty() {
		t.Fatalf("expected empty mutation, got %+v", mut)
	}
}

func TestMetadataAttributesFromMeta(t *testing.T) {
	var meta storage.Meta
	meta.MarkQueryExcluded()
	attr := metadataAttributesFromMeta(&meta)
	if attr.QueryHidden == nil || !*attr.QueryHidden {
		t.Fatalf("expected QueryHidden=true from meta")
	}
}
