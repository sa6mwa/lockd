package namespaces_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	namespaces "pkt.systems/lockd/namespaces"
)

type countingBackend struct {
	storage.Backend
	getCalls atomic.Int64
}

func (b *countingBackend) GetObject(ctx context.Context, namespace, key string) (storage.GetObjectResult, error) {
	b.getCalls.Add(1)
	return b.Backend.GetObject(ctx, namespace, key)
}

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

	loadRes, err := cfgStore.Load(ctx, ns)
	if err != nil || loadRes.ETag != "" {
		t.Fatalf("load default: cfg=%v etag=%q err=%v", loadRes.Config, loadRes.ETag, err)
	}
	cfg := loadRes.Config

	cfg.Query.Preferred = search.EngineScan
	cfg.Query.Fallback = namespaces.FallbackNone
	newETag, err := cfgStore.Save(ctx, ns, cfg, "")
	if err != nil {
		t.Fatalf("save config: %v", err)
	}
	loadedRes, err := cfgStore.Load(ctx, ns)
	if err != nil {
		t.Fatalf("reload config: %v", err)
	}
	if loadedRes.ETag == "" || newETag != loadedRes.ETag {
		t.Fatalf("etag mismatch: %q vs %q", newETag, loadedRes.ETag)
	}
	if loadedRes.Config.Query.Preferred != search.EngineScan || loadedRes.Config.Query.Fallback != namespaces.FallbackNone {
		t.Fatalf("unexpected config %+v", loadedRes.Config)
	}
	if _, err := cfgStore.Save(ctx, ns, cfg, "bogus"); err == nil {
		t.Fatalf("expected cas mismatch when saving with stale etag")
	}
}

func TestConfigStoreLoadCachesResults(t *testing.T) {
	backend := &countingBackend{Backend: memory.New()}
	cfgStore := namespaces.NewConfigStore(backend, nil, nil, namespaces.DefaultConfig())
	ctx := context.Background()
	ns := "default"

	if _, err := cfgStore.Load(ctx, ns); err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, err := cfgStore.Load(ctx, ns); err != nil {
		t.Fatalf("load cached: %v", err)
	}
	if got := backend.getCalls.Load(); got != 1 {
		t.Fatalf("expected one backend load within cache ttl, got %d", got)
	}
}

func TestConfigStoreLoadCacheExpires(t *testing.T) {
	backend := &countingBackend{Backend: memory.New()}
	cfgStore := namespaces.NewConfigStore(backend, nil, nil, namespaces.DefaultConfig())
	ctx := context.Background()
	ns := "default"

	if _, err := cfgStore.Load(ctx, ns); err != nil {
		t.Fatalf("load: %v", err)
	}
	time.Sleep(350 * time.Millisecond)
	if _, err := cfgStore.Load(ctx, ns); err != nil {
		t.Fatalf("load after ttl: %v", err)
	}
	if got := backend.getCalls.Load(); got != 2 {
		t.Fatalf("expected cache miss after ttl expiry, got %d backend loads", got)
	}
}
