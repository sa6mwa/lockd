package search

import (
	"context"
	"errors"
	"io"
	"testing"
)

func TestDispatcherQueryDocumentsFallsBackToScanWhenIndexStreamingUnsupported(t *testing.T) {
	index := adapterStub{
		queryDocsErr: ErrDocumentStreamingUnsupported,
	}
	scan := adapterStub{
		queryDocsResult: Result{
			Keys:     []string{"orders/1"},
			Metadata: map[string]string{"source": "scan"},
		},
	}
	dispatcher := NewDispatcher(DispatcherConfig{Index: index, Scan: scan})
	result, err := dispatcher.QueryDocuments(context.Background(), Request{
		Namespace: "default",
		Engine:    EngineAuto,
	}, noopSink{})
	if err != nil {
		t.Fatalf("query documents: %v", err)
	}
	if len(result.Keys) != 1 || result.Keys[0] != "orders/1" {
		t.Fatalf("unexpected result keys: %+v", result.Keys)
	}
	if result.Metadata["engine"] != string(EngineScan) {
		t.Fatalf("expected engine metadata scan, got %+v", result.Metadata)
	}
}

func TestDispatcherQueryDocumentsIndexErrorsDoNotFallback(t *testing.T) {
	boom := errors.New("index exploded")
	index := adapterStub{
		queryDocsErr: boom,
	}
	scan := adapterStub{
		queryDocsResult: Result{Keys: []string{"orders/1"}},
	}
	dispatcher := NewDispatcher(DispatcherConfig{Index: index, Scan: scan})
	_, err := dispatcher.QueryDocuments(context.Background(), Request{
		Namespace: "default",
		Engine:    EngineIndex,
	}, noopSink{})
	if !errors.Is(err, boom) {
		t.Fatalf("expected %v, got %v", boom, err)
	}
}

type adapterStub struct {
	queryResult     Result
	queryErr        error
	queryDocsResult Result
	queryDocsErr    error
}

func (a adapterStub) Capabilities(context.Context, string) (Capabilities, error) {
	return Capabilities{Index: true, Scan: true}, nil
}

func (a adapterStub) Query(context.Context, Request) (Result, error) {
	return a.queryResult, a.queryErr
}

func (a adapterStub) QueryDocuments(context.Context, Request, DocumentSink) (Result, error) {
	return a.queryDocsResult, a.queryDocsErr
}

type noopSink struct{}

func (noopSink) OnDocument(context.Context, string, string, int64, io.Reader) error {
	return nil
}
