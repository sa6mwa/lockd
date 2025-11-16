package client

import (
	"errors"
	"io"
	"strings"
	"testing"

	"pkt.systems/lockd/api"
)

func TestQueryResponseStreamDocumentReader(t *testing.T) {
	body := `{"ns":"default","key":"doc-1","ver":7,"doc":{"status":"ok","count":5}}
`
	resp := newDocumentQueryResponse("default", "", 0, nil, io.NopCloser(strings.NewReader(body)))
	defer resp.Close()
	count := 0
	err := resp.ForEach(func(row QueryRow) error {
		count++
		reader, err := row.DocumentReader()
		if err != nil {
			return err
		}
		if reader == nil {
			t.Fatalf("expected document reader")
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		reader.Close()
		if string(data) != `{"status":"ok","count":5}` {
			t.Fatalf("unexpected document %s", data)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("for each: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row, got %d", count)
	}
}

func TestQueryResponseStreamKeys(t *testing.T) {
	body := `{"ns":"default","key":"doc-1","doc":{}}
{"ns":"default","key":"doc-2","doc":[]}
`
	resp := newDocumentQueryResponse("default", "", 0, nil, io.NopCloser(strings.NewReader(body)))
	keys := resp.Keys()
	expected := []string{"doc-1", "doc-2"}
	if len(keys) != len(expected) {
		t.Fatalf("expected %d keys, got %d", len(expected), len(keys))
	}
	for i, k := range expected {
		if keys[i] != k {
			t.Fatalf("unexpected key %s", keys[i])
		}
	}
	if err := resp.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestQueryRowDocumentReaderKeysMode(t *testing.T) {
	resp := newKeyQueryResponse(api.QueryResponse{
		Namespace: "default",
		Keys:      []string{"doc-1"},
	}, QueryReturnKeys)
	var row QueryRow
	err := resp.ForEach(func(r QueryRow) error {
		row = r
		return nil
	})
	if err != nil {
		t.Fatalf("foreach: %v", err)
	}
	if _, err := row.DocumentReader(); err == nil || !errors.Is(err, errDocumentMissing) {
		t.Fatalf("expected document missing error, got %v", err)
	}
}

func TestQueryResponseKeysDefensiveCopy(t *testing.T) {
	resp := newKeyQueryResponse(api.QueryResponse{Keys: []string{"a", "b"}}, QueryReturnKeys)
	first := resp.Keys()
	first[0] = "mutated"
	second := resp.Keys()
	if second[0] != "a" {
		t.Fatalf("expected defensive copy in keys mode, got %v", second)
	}
}

func TestQueryResponseDocumentKeysDefensiveCopy(t *testing.T) {
	body := `{"ns":"default","key":"doc-1","doc":{}}
{"ns":"default","key":"doc-2","doc":[]}
`
	resp := newDocumentQueryResponse("default", "", 0, nil, io.NopCloser(strings.NewReader(body)))
	keys := resp.Keys()
	keys[0] = "mutated"
	second := resp.Keys()
	if second[0] != "doc-1" {
		t.Fatalf("expected defensive copy in documents mode, got %v", second)
	}
	resp.Close()
}
