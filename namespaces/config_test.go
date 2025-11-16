package namespaces_test

import (
	"context"
	"testing"

	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage/memory"
	namespaces "pkt.systems/lockd/namespaces"
)

func TestConfigSelectEngine(t *testing.T) {
	cfg := namespaces.DefaultConfig()
	engine, err := cfg.SelectEngine(search.EngineAuto, search.Capabilities{Index: true, Scan: true})
	if err != nil {
		t.Fatalf("select engine: %v", err)
	}
	if engine != search.EngineIndex {
		t.Fatalf("expected index, got %s", engine)
	}

	cfg.Query.Preferred = search.EngineIndex
	cfg.Query.Fallback = namespaces.FallbackNone
	_, err = cfg.SelectEngine(search.EngineAuto, search.Capabilities{Scan: true})
	if err == nil {
		t.Fatalf("expected error when index unavailable and fallback disabled")
	}

	cfg.Query.Preferred = search.EngineScan
	cfg.Query.Fallback = namespaces.FallbackScan
	engine, err = cfg.SelectEngine(search.EngineAuto, search.Capabilities{Scan: true})
	if err != nil {
		t.Fatalf("select fallback: %v", err)
	}
	if engine != search.EngineScan {
		t.Fatalf("expected scan fallback, got %s", engine)
	}
}

func TestConfigStoreRoundTrip(t *testing.T) {
	store := memory.New()
	cfgStore := namespaces.NewConfigStore(store, nil, nil, namespaces.DefaultConfig())
	ctx := context.Background()
	ns := "default"

	cfg, etag, err := cfgStore.Load(ctx, ns)
	if err != nil || etag != "" {
		t.Fatalf("load default: cfg=%v etag=%q err=%v", cfg, etag, err)
	}

	cfg.Query.Preferred = search.EngineScan
	cfg.Query.Fallback = namespaces.FallbackNone
	newETag, err := cfgStore.Save(ctx, ns, cfg, "")
	if err != nil {
		t.Fatalf("save config: %v", err)
	}
	loaded, etag, err := cfgStore.Load(ctx, ns)
	if err != nil {
		t.Fatalf("reload config: %v", err)
	}
	if etag == "" || newETag != etag {
		t.Fatalf("etag mismatch: %q vs %q", newETag, etag)
	}
	if loaded.Query.Preferred != search.EngineScan || loaded.Query.Fallback != namespaces.FallbackNone {
		t.Fatalf("unexpected config %+v", loaded)
	}
	if _, err := cfgStore.Save(ctx, ns, cfg, "bogus"); err == nil {
		t.Fatalf("expected cas mismatch when saving with stale etag")
	}
}
