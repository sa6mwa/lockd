package disk

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/namespaces"
)

func TestLogstoreCompactionRewritesLinkedStateIntoSnapshot(t *testing.T) {
	root := t.TempDir()
	now := time.Unix(1_700_000_000, 0).UTC()
	store, err := New(Config{
		Root:                             root,
		Now:                              func() time.Time { return now },
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       time.Hour,
		LogstoreCompactionMinSegments:    2,
		LogstoreCompactionMinReclaimSize: 1,
		LogstoreCompactionDeleteGrace:    time.Minute,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	const (
		ns    = namespaces.Default
		key   = "orders/linked"
		txnID = "txn-1"
	)
	payload := []byte("linked-state-payload")
	if _, err := store.StageState(ctx, ns, key, txnID, bytes.NewReader(payload), storage.PutStateOptions{}); err != nil {
		t.Fatalf("stage state: %v", err)
	}
	ln := mustNamespace(t, store.logstore, ns)
	sealActiveForTest(t, ln)
	if _, err := store.PromoteStagedState(ctx, ns, key, txnID, storage.PromoteStagedOptions{}); err != nil {
		t.Fatalf("promote staged: %v", err)
	}
	sealActiveForTest(t, ln)
	writeStateAndSeal(t, store, ns, "roll/after-promote", []byte("roll-segment"))

	ln.mu.Lock()
	current := ln.stateIndex[key]
	if current == nil || current.segment == nil || current.segment == ln.active {
		ln.mu.Unlock()
		t.Fatalf("expected linked state to be in a sealed segment")
	}
	ln.mu.Unlock()

	if err := store.CompactLogstore(ctx, ns); err != nil {
		t.Fatalf("compact logstore: %v", err)
	}
	assertStateEquals(t, store, ns, key, payload)

	ln.mu.Lock()
	if ln.installedSnapshot == nil {
		ln.mu.Unlock()
		t.Fatalf("expected installed snapshot")
	}
	obsolete := obsoletePathsLocked(ln)
	ln.mu.Unlock()
	if len(obsolete) == 0 {
		t.Fatalf("expected obsolete compacted files")
	}

	now = now.Add(2 * time.Minute)
	if err := ln.cleanupObsolete(time.Minute); err != nil {
		t.Fatalf("cleanup obsolete: %v", err)
	}
	for _, path := range obsolete {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expected obsolete file %q removed, got %v", path, err)
		}
	}
	assertStateEquals(t, store, ns, key, payload)
}

func TestLogstoreCompactionSkipsActiveTail(t *testing.T) {
	root := t.TempDir()
	store, err := New(Config{
		Root:                             root,
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       time.Hour,
		LogstoreCompactionMinSegments:    1,
		LogstoreCompactionMinReclaimSize: 1,
		LogstoreCompactionDeleteGrace:    time.Minute,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	const ns = namespaces.Default
	writeStateAndSeal(t, store, ns, "orders/item", []byte("older"))
	if _, err := store.WriteState(ctx, ns, "orders/item", bytes.NewReader([]byte("newest")), storage.PutStateOptions{}); err != nil {
		t.Fatalf("write tail: %v", err)
	}

	ln := mustNamespace(t, store.logstore, ns)
	ln.mu.Lock()
	active := ln.active
	current := ln.stateIndex["orders/item"]
	ln.mu.Unlock()
	if active == nil || current == nil || current.segment != active {
		t.Fatalf("expected latest state to remain in active segment")
	}

	if err := store.CompactLogstore(ctx, ns); err != nil {
		t.Fatalf("compact logstore: %v", err)
	}
	assertStateEquals(t, store, ns, "orders/item", []byte("newest"))

	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.stateIndex["orders/item"] != current {
		t.Fatalf("expected active tail ref to remain current after compaction")
	}
	if _, ok := ln.obsoleteSegments[active.name]; ok {
		t.Fatalf("active segment %q should not be obsolete", active.name)
	}
}

func TestLogstoreCompactionValidationRejectsStaleCapture(t *testing.T) {
	root := t.TempDir()
	store := newLogStore(root, time.Now, nil, 0, 0, false)
	ln, err := store.namespace("default")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	if err := ln.refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	cfg := logCompactionConfig{
		enabled:         true,
		minSegments:     1,
		minReclaimBytes: 1,
		deleteGrace:     time.Minute,
	}
	ctx := context.Background()
	if _, err := ln.appendRecord(ctx, logRecordStatePut, "key", recordMeta{gen: 1}, bytes.NewReader([]byte("old"))); err != nil {
		t.Fatalf("append old: %v", err)
	}
	sealActiveForTest(t, ln)

	ln.mu.Lock()
	capture := ln.captureCompactionLocked(cfg)
	ln.mu.Unlock()
	if capture == nil {
		t.Fatalf("expected compaction capture")
	}
	if _, err := ln.appendRecord(ctx, logRecordStatePut, "key", recordMeta{gen: 2}, bytes.NewReader([]byte("new"))); err != nil {
		t.Fatalf("append new: %v", err)
	}
	result, err := ln.buildCompactionSnapshot(capture, cfg)
	if err != nil {
		t.Fatalf("build compaction snapshot: %v", err)
	}
	defer os.Remove(result.tempPath)

	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.validateCompactionLocked(capture) {
		t.Fatalf("expected stale capture validation failure")
	}
}

func TestLogstoreCompactionRestoresFromSnapshotAndTailOnRestart(t *testing.T) {
	root := t.TempDir()
	store, err := New(Config{
		Root:                             root,
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       time.Hour,
		LogstoreCompactionMinSegments:    1,
		LogstoreCompactionMinReclaimSize: 1,
		LogstoreCompactionDeleteGrace:    time.Minute,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	ctx := context.Background()
	const ns = namespaces.Default
	writeStateAndSeal(t, store, ns, "orders/restart", []byte("before-snapshot"))
	if err := store.CompactLogstore(ctx, ns); err != nil {
		t.Fatalf("compact logstore: %v", err)
	}
	if _, err := store.WriteState(ctx, ns, "orders/restart", bytes.NewReader([]byte("tail-update")), storage.PutStateOptions{}); err != nil {
		t.Fatalf("write tail update: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := New(Config{Root: root})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()
	assertStateEquals(t, reopened, ns, "orders/restart", []byte("tail-update"))
}

func TestLogstoreBackgroundCompactionRuns(t *testing.T) {
	root := t.TempDir()
	store, err := New(Config{
		Root:                             root,
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       10 * time.Millisecond,
		LogstoreCompactionMinSegments:    1,
		LogstoreCompactionMinReclaimSize: 1,
		LogstoreCompactionDeleteGrace:    time.Hour,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	const ns = namespaces.Default
	writeStateAndSeal(t, store, ns, "orders/background", []byte("background"))
	waitFor(t, 2*time.Second, func() bool {
		ln := mustNamespace(t, store.logstore, ns)
		ln.mu.Lock()
		defer ln.mu.Unlock()
		return ln.installedSnapshot != nil
	})
	assertStateEquals(t, store, ns, "orders/background", []byte("background"))
}

func TestLogstoreCompactionPreservesObjects(t *testing.T) {
	root := t.TempDir()
	store, err := New(Config{
		Root:                             root,
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       time.Hour,
		LogstoreCompactionMinSegments:    1,
		LogstoreCompactionMinReclaimSize: 1,
		LogstoreCompactionDeleteGrace:    time.Minute,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	const (
		ns  = namespaces.Default
		key = "objects/a.txt"
	)
	info, err := store.PutObject(ctx, ns, key, bytes.NewReader([]byte("object-body")), storage.PutObjectOptions{})
	if err != nil {
		t.Fatalf("put object: %v", err)
	}
	if info == nil || info.ETag == "" {
		t.Fatalf("expected object info")
	}
	sealActiveForTest(t, mustNamespace(t, store.logstore, ns))
	if err := store.CompactLogstore(ctx, ns); err != nil {
		t.Fatalf("compact logstore: %v", err)
	}
	res, err := store.GetObject(ctx, ns, key)
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	defer res.Reader.Close()
	body, err := io.ReadAll(res.Reader)
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	if !bytes.Equal(body, []byte("object-body")) {
		t.Fatalf("object body mismatch: %q", string(body))
	}
}

func TestLogstoreCompactionDisabledSkipsSnapshot(t *testing.T) {
	root := t.TempDir()
	store, err := New(Config{
		Root:                             root,
		LogstoreCompactionEnabled:        false,
		LogstoreCompactionInterval:       time.Hour,
		LogstoreCompactionMinSegments:    1,
		LogstoreCompactionMinReclaimSize: 1,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	writeStateAndSeal(t, store, namespaces.Default, "disabled/key", []byte("payload"))
	if err := store.CompactLogstore(context.Background(), namespaces.Default); err != nil {
		t.Fatalf("compact disabled store: %v", err)
	}
	ln := mustNamespace(t, store.logstore, namespaces.Default)
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.installedSnapshot != nil {
		t.Fatalf("expected no snapshot when compaction disabled")
	}
}

func TestLogstoreCompactionBelowThresholdSkipsSnapshot(t *testing.T) {
	root := t.TempDir()
	store, err := New(Config{
		Root:                             root,
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       time.Hour,
		LogstoreCompactionMinSegments:    2,
		LogstoreCompactionMinReclaimSize: 1 << 20,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	writeStateAndSeal(t, store, namespaces.Default, "threshold/key", []byte("small"))
	if err := store.CompactLogstore(context.Background(), namespaces.Default); err != nil {
		t.Fatalf("compact below threshold: %v", err)
	}
	ln := mustNamespace(t, store.logstore, namespaces.Default)
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.installedSnapshot != nil {
		t.Fatalf("expected no snapshot below thresholds")
	}
}

func TestLogstoreCompactionSecondGenerationObsoletesPriorSnapshot(t *testing.T) {
	root := t.TempDir()
	now := time.Unix(1_700_000_000, 0).UTC()
	store, err := New(Config{
		Root:                             root,
		Now:                              func() time.Time { return now },
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       time.Hour,
		LogstoreCompactionMinSegments:    1,
		LogstoreCompactionMinReclaimSize: 1,
		LogstoreCompactionDeleteGrace:    time.Minute,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	const ns = namespaces.Default
	writeStateAndSeal(t, store, ns, "gen/key", []byte("v1"))
	if err := store.CompactLogstore(ctx, ns); err != nil {
		t.Fatalf("first compaction: %v", err)
	}
	ln := mustNamespace(t, store.logstore, ns)
	ln.mu.Lock()
	first := ln.installedSnapshot
	ln.mu.Unlock()
	if first == nil {
		t.Fatalf("expected first snapshot")
	}

	writeStateAndSeal(t, store, ns, "gen/roll", []byte("v2-roll"))
	if err := store.CompactLogstore(ctx, ns); err != nil {
		t.Fatalf("second compaction: %v", err)
	}
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.installedSnapshot == nil || ln.installedSnapshot == first {
		t.Fatalf("expected new installed snapshot generation")
	}
	if _, ok := ln.obsoleteSnapshots[first.name]; !ok {
		t.Fatalf("expected prior snapshot %q to be obsolete", first.name)
	}
}

func TestLogstoreCompactionBuildFailureLeavesStateUnchanged(t *testing.T) {
	root := t.TempDir()
	store := newLogStore(root, time.Now, nil, 0, 0, false)
	ln, err := store.namespace("default")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	if err := ln.refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	ctx := context.Background()
	if _, err := ln.appendRecord(ctx, logRecordStatePut, "key", recordMeta{gen: 1}, bytes.NewReader([]byte("payload"))); err != nil {
		t.Fatalf("append: %v", err)
	}
	sealActiveForTest(t, ln)

	ln.mu.Lock()
	capture := ln.captureCompactionLocked(logCompactionConfig{enabled: true, minSegments: 1, minReclaimBytes: 1})
	current := ln.stateIndex["key"]
	ln.mu.Unlock()
	if capture == nil || current == nil {
		t.Fatalf("expected capture and current ref")
	}

	badPath := filepath.Join(root, "bad-snapshots")
	if err := os.WriteFile(badPath, []byte("not-a-dir"), 0o644); err != nil {
		t.Fatalf("write bad snapshot path: %v", err)
	}
	ln.snapshotsDir = badPath
	if _, err := ln.buildCompactionSnapshot(capture, logCompactionConfig{enabled: true, minSegments: 1, minReclaimBytes: 1}); err == nil {
		t.Fatalf("expected build failure for invalid snapshot dir")
	}
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.stateIndex["key"] != current {
		t.Fatalf("expected state index unchanged after build failure")
	}
	if ln.installedSnapshot != nil {
		t.Fatalf("did not expect installed snapshot after build failure")
	}
}

func TestLogstoreCompactionManifestFailureLeavesStateUnchanged(t *testing.T) {
	root := t.TempDir()
	now := time.Unix(1_700_000_000, 0).UTC()
	store, err := New(Config{
		Root:                             root,
		Now:                              func() time.Time { return now },
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       time.Hour,
		LogstoreCompactionMinSegments:    1,
		LogstoreCompactionMinReclaimSize: 1,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	const ns = namespaces.Default
	writeStateAndSeal(t, store, ns, "manifest/key", []byte("payload"))
	ln := mustNamespace(t, store.logstore, ns)
	ln.mu.Lock()
	capture := ln.captureCompactionLocked(logCompactionConfig{enabled: true, minSegments: 1, minReclaimBytes: 1})
	current := ln.stateIndex["manifest/key"]
	ln.mu.Unlock()
	if capture == nil || current == nil {
		t.Fatalf("expected capture and state ref")
	}
	result, err := ln.buildCompactionSnapshot(capture, logCompactionConfig{enabled: true, minSegments: 1, minReclaimBytes: 1})
	if err != nil {
		t.Fatalf("build compaction snapshot: %v", err)
	}
	if err := os.Remove(ln.manifest.path); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove manifest file: %v", err)
	}
	if err := os.MkdirAll(ln.manifest.path, 0o755); err != nil {
		t.Fatalf("make manifest dir: %v", err)
	}
	defer os.RemoveAll(ln.manifest.path)

	ln.mu.Lock()
	err = ln.installCompactionLocked(result, capture, now)
	ln.mu.Unlock()
	if err == nil {
		t.Fatalf("expected manifest append failure")
	}
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.stateIndex["manifest/key"] != current {
		t.Fatalf("expected state index unchanged after manifest failure")
	}
	if ln.installedSnapshot != nil {
		t.Fatalf("did not expect installed snapshot after manifest failure")
	}
	if _, err := os.Stat(result.segment.path); !os.IsNotExist(err) {
		t.Fatalf("expected failed install snapshot removed, got %v", err)
	}
}

func TestLogstoreCompactionReadContinuityDuringBackgroundWork(t *testing.T) {
	root := t.TempDir()
	store, err := New(Config{
		Root:                               root,
		LogstoreCompactionEnabled:          true,
		LogstoreCompactionInterval:         time.Hour,
		LogstoreCompactionMinSegments:      1,
		LogstoreCompactionMinReclaimSize:   1,
		LogstoreCompactionMaxIOBytesPerSec: 1 << 20,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	const ns = namespaces.Default
	payload := bytes.Repeat([]byte("x"), 64<<10)
	writeStateAndSeal(t, store, ns, "read/key", payload)

	done := make(chan error, 1)
	go func() {
		for i := 0; i < 50; i++ {
			res, err := store.ReadState(context.Background(), ns, "read/key")
			if err != nil {
				done <- err
				return
			}
			body, err := io.ReadAll(res.Reader)
			res.Reader.Close()
			if err != nil {
				done <- err
				return
			}
			if !bytes.Equal(body, payload) {
				done <- io.ErrUnexpectedEOF
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
		done <- nil
	}()
	if err := store.CompactLogstore(context.Background(), ns); err != nil {
		t.Fatalf("compact logstore: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("read continuity failed: %v", err)
	}
}

func mustNamespace(t *testing.T, store *logStore, namespace string) *logNamespace {
	t.Helper()
	ln, err := store.namespace(namespace)
	if err != nil {
		t.Fatalf("namespace %q: %v", namespace, err)
	}
	if err := ln.refresh(); err != nil {
		t.Fatalf("refresh %q: %v", namespace, err)
	}
	return ln
}

func sealActiveForTest(t *testing.T, ln *logNamespace) {
	t.Helper()
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if err := ln.closeActiveLocked(); err != nil {
		t.Fatalf("close active: %v", err)
	}
}

func writeStateAndSeal(t *testing.T, store *Store, namespace, key string, payload []byte) {
	t.Helper()
	if _, err := store.WriteState(context.Background(), namespace, key, bytes.NewReader(payload), storage.PutStateOptions{}); err != nil {
		t.Fatalf("write state %q: %v", key, err)
	}
	sealActiveForTest(t, mustNamespace(t, store.logstore, namespace))
}

func assertStateEquals(t *testing.T, store *Store, namespace, key string, expected []byte) {
	t.Helper()
	res, err := store.ReadState(context.Background(), namespace, key)
	if err != nil {
		t.Fatalf("read state %q: %v", key, err)
	}
	defer res.Reader.Close()
	body, err := io.ReadAll(res.Reader)
	if err != nil {
		t.Fatalf("read state body %q: %v", key, err)
	}
	if !bytes.Equal(body, expected) {
		t.Fatalf("state %q mismatch: got %q want %q", key, string(body), string(expected))
	}
}

func obsoletePathsLocked(ln *logNamespace) []string {
	var out []string
	for name := range ln.obsoleteSegments {
		if seg := ln.segments[name]; seg != nil {
			out = append(out, seg.path)
		}
	}
	for name := range ln.obsoleteSnapshots {
		if seg := ln.snapshots[name]; seg != nil {
			out = append(out, seg.path)
		}
	}
	slices.Sort(out)
	return out
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func TestWriteCompactionRecordBytes(t *testing.T) {
	buf, err := writeCompactionRecordBytes(logRecordStatePut, "k", recordMeta{gen: 1, etag: string(make([]byte, 0))}, []byte("x"))
	if err == nil && len(buf) == 0 {
		t.Fatalf("expected record bytes")
	}
}

func TestCleanupObsoleteRemovesSnapshotFiles(t *testing.T) {
	root := t.TempDir()
	store := newLogStore(root, time.Now, nil, 0, 0, false)
	ln, err := store.namespace("default")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	if err := ln.ensureDirs(); err != nil {
		t.Fatalf("ensure dirs: %v", err)
	}
	path := filepath.Join(ln.snapshotsDir, "snap-test.log")
	if err := os.WriteFile(path, []byte("snapshot"), 0o644); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	store.now = func() time.Time { return now }
	ln.mu.Lock()
	ln.snapshots["snap-test.log"] = &logSegment{name: "snap-test.log", path: path}
	ln.obsoleteSnapshots = map[string]time.Time{"snap-test.log": now.Add(-time.Minute)}
	ln.mu.Unlock()
	if err := ln.cleanupObsolete(10 * time.Second); err != nil {
		t.Fatalf("cleanup obsolete: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected snapshot removed, got %v", err)
	}
}
