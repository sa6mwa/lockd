package main

import (
	"strings"
	"testing"

	"github.com/magiconair/properties"
)

func withGlobalProps(t *testing.T, p *properties.Properties) {
	t.Helper()
	prev := globalProps
	globalProps = p
	t.Cleanup(func() {
		globalProps = prev
	})
}

func TestPerfMetadataLockdIncludesQueryFields(t *testing.T) {
	p := properties.NewProperties()
	p.Set("workload", "core")
	p.Set("recordcount", "1000")
	p.Set("operationcount", "2000")
	p.Set("threadcount", "8")
	p.Set("target", "0")
	p.Set("lockd.endpoints", "https://127.0.0.1:9341")
	p.Set("lockd.query.engine", "index")
	p.Set("lockd.query.return", "documents")
	withGlobalProps(t, p)

	meta := perfMetadata("lockd", "run")
	if !strings.Contains(meta, "query_engine=index") {
		t.Fatalf("expected query_engine in metadata, got: %s", meta)
	}
	if !strings.Contains(meta, "query_return=documents") {
		t.Fatalf("expected query_return in metadata, got: %s", meta)
	}
}

func TestPerfMetadataEtcdQueryFieldsEmpty(t *testing.T) {
	p := properties.NewProperties()
	p.Set("workload", "core")
	p.Set("recordcount", "1000")
	p.Set("operationcount", "2000")
	p.Set("threadcount", "8")
	p.Set("target", "0")
	p.Set("etcd.endpoints", "127.0.0.1:2379")
	withGlobalProps(t, p)

	meta := perfMetadata("etcd", "run")
	if !strings.Contains(meta, "query_engine=") {
		t.Fatalf("expected query_engine field to exist, got: %s", meta)
	}
	if !strings.Contains(meta, "query_return=") {
		t.Fatalf("expected query_return field to exist, got: %s", meta)
	}
	if strings.Contains(meta, "query_engine=index") || strings.Contains(meta, "query_return=documents") {
		t.Fatalf("expected empty query metadata for etcd, got: %s", meta)
	}
}
