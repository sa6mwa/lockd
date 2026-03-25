package disk

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

type recordedLogEntry struct {
	level string
	msg   string
	kv    []any
}

type recordingLogCore struct {
	mu      sync.Mutex
	entries []recordedLogEntry
}

type recordingLogger struct {
	core   *recordingLogCore
	fields []any
}

func (l *recordingLogger) record(level, msg string, kv ...any) {
	if l.core == nil {
		l.core = &recordingLogCore{}
	}
	l.core.mu.Lock()
	defer l.core.mu.Unlock()
	entryKV := make([]any, 0, len(l.fields)+len(kv))
	entryKV = append(entryKV, l.fields...)
	entryKV = append(entryKV, kv...)
	l.core.entries = append(l.core.entries, recordedLogEntry{
		level: level,
		msg:   msg,
		kv:    entryKV,
	})
}

func (l *recordingLogger) Trace(msg string, keyvals ...any) { l.record("trace", msg, keyvals...) }
func (l *recordingLogger) Debug(msg string, keyvals ...any) { l.record("debug", msg, keyvals...) }
func (l *recordingLogger) Info(msg string, keyvals ...any)  { l.record("info", msg, keyvals...) }
func (l *recordingLogger) Warn(msg string, keyvals ...any)  { l.record("warn", msg, keyvals...) }
func (l *recordingLogger) Error(msg string, keyvals ...any) { l.record("error", msg, keyvals...) }
func (l *recordingLogger) Fatal(msg string, keyvals ...any) { l.record("fatal", msg, keyvals...) }
func (l *recordingLogger) Panic(msg string, keyvals ...any) { l.record("panic", msg, keyvals...) }
func (l *recordingLogger) Log(level pslog.Level, msg string, keyvals ...any) {
	switch level {
	case pslog.TraceLevel:
		l.record("trace", msg, keyvals...)
	case pslog.DebugLevel:
		l.record("debug", msg, keyvals...)
	case pslog.InfoLevel:
		l.record("info", msg, keyvals...)
	case pslog.WarnLevel:
		l.record("warn", msg, keyvals...)
	case pslog.ErrorLevel:
		l.record("error", msg, keyvals...)
	case pslog.FatalLevel:
		l.record("fatal", msg, keyvals...)
	case pslog.PanicLevel:
		l.record("panic", msg, keyvals...)
	default:
		l.record("log", msg, keyvals...)
	}
}

func (l *recordingLogger) With(keyvals ...any) pslog.Logger {
	child := &recordingLogger{
		core: l.core,
	}
	child.fields = append(append([]any(nil), l.fields...), keyvals...)
	return child
}

func (l *recordingLogger) WithLogLevel() pslog.Logger        { return l }
func (l *recordingLogger) LogLevel(pslog.Level) pslog.Logger { return l }
func (l *recordingLogger) LogLevelFromEnv(string) pslog.Logger {
	return l
}

func (l *recordingLogger) snapshot() []recordedLogEntry {
	if l.core == nil {
		return nil
	}
	l.core.mu.Lock()
	defer l.core.mu.Unlock()
	out := make([]recordedLogEntry, len(l.core.entries))
	copy(out, l.core.entries)
	return out
}

func findRecordedLog(entries []recordedLogEntry, level, msg string) (recordedLogEntry, bool) {
	for _, entry := range entries {
		if entry.level == level && entry.msg == msg {
			return entry, true
		}
	}
	return recordedLogEntry{}, false
}

func logField(entry recordedLogEntry, key string) any {
	for i := 0; i+1 < len(entry.kv); i += 2 {
		if k, ok := entry.kv[i].(string); ok && k == key {
			return entry.kv[i+1]
		}
	}
	return nil
}

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

func TestLogstoreCompactionProtectsActiveLinkedPayloadSegments(t *testing.T) {
	root := t.TempDir()
	now := time.Unix(1_700_000_000, 0).UTC()
	store, err := New(Config{
		Root:                             root,
		Now:                              func() time.Time { return now },
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       0,
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
		ns    = namespaces.Default
		key   = "orders/active-link"
		txnID = "txn-active-link"
	)

	writeStateAndSeal(t, store, ns, "roll/protected", []byte("older-sealed"))
	payload := []byte("linked-active-payload")
	if _, err := store.StageState(ctx, ns, key, txnID, bytes.NewReader(payload), storage.PutStateOptions{}); err != nil {
		t.Fatalf("stage state: %v", err)
	}
	ln := mustNamespace(t, store.logstore, ns)
	sealActiveForTest(t, ln)
	if _, err := store.PromoteStagedState(ctx, ns, key, txnID, storage.PromoteStagedOptions{}); err != nil {
		t.Fatalf("promote staged: %v", err)
	}

	ln.mu.Lock()
	current := ln.stateIndex[key]
	if current == nil || current.segment == nil || current.segment != ln.active {
		ln.mu.Unlock()
		t.Fatalf("expected live state link to remain in active segment")
	}
	if current.link == nil {
		ln.mu.Unlock()
		t.Fatalf("expected live state link payload reference")
	}
	protectedName := filepath.Base(current.link.path)
	ln.mu.Unlock()

	if err := store.CompactLogstore(ctx, ns); err != nil {
		t.Fatalf("compact logstore: %v", err)
	}
	assertStateEquals(t, store, ns, key, payload)

	ln.mu.Lock()
	if _, ok := ln.obsoleteSegments[protectedName]; ok {
		ln.mu.Unlock()
		t.Fatalf("expected active linked payload segment %q to remain protected", protectedName)
	}
	ln.mu.Unlock()

	now = now.Add(2 * time.Minute)
	if err := ln.cleanupObsolete(time.Minute); err != nil {
		t.Fatalf("cleanup obsolete: %v", err)
	}
	assertStateEquals(t, store, ns, key, payload)
}

func TestLogstoreCompactionValidationRejectsStaleCapture(t *testing.T) {
	root := t.TempDir()
	store := newLogStore(root, time.Now, nil, pslog.NoopLogger(), 0, 0, false)
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
	capture, _ := ln.captureCompactionLocked(cfg)
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

func TestLogstoreCompactionSeesSealedHistoryAfterRestart(t *testing.T) {
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
	writeStateAndSeal(t, store, ns, "orders/a", []byte("a"))
	writeStateAndSeal(t, store, ns, "orders/b", []byte("b"))
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := New(Config{
		Root:                             root,
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       time.Hour,
		LogstoreCompactionMinSegments:    1,
		LogstoreCompactionMinReclaimSize: 1,
		LogstoreCompactionDeleteGrace:    time.Minute,
	})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()

	if err := reopened.CompactLogstore(ctx, ns); err != nil {
		t.Fatalf("compact after restart: %v", err)
	}
	ln := mustNamespace(t, reopened.logstore, ns)
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.installedSnapshot == nil {
		t.Fatalf("expected compaction snapshot after restart with sealed history")
	}
}

func TestLogstoreCompactionSeesSealedHistoryAfterCrashStyleRestart(t *testing.T) {
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
	writeStateAndSeal(t, store, ns, "orders/sealed", []byte("sealed"))
	if _, err := store.WriteState(ctx, ns, "orders/live", bytes.NewReader([]byte("live")), storage.PutStateOptions{}); err != nil {
		t.Fatalf("write live tail: %v", err)
	}

	ln := mustNamespace(t, store.logstore, ns)
	ln.mu.Lock()
	if ln.active == nil || ln.active.file == nil {
		ln.mu.Unlock()
		t.Fatalf("expected active tail segment")
	}
	if err := ln.active.file.Close(); err != nil {
		ln.mu.Unlock()
		t.Fatalf("close active file for crash simulation: %v", err)
	}
	ln.active.file = nil
	ln.mu.Unlock()

	reopened, err := New(Config{
		Root:                             root,
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       time.Hour,
		LogstoreCompactionMinSegments:    1,
		LogstoreCompactionMinReclaimSize: 1,
		LogstoreCompactionDeleteGrace:    time.Minute,
	})
	if err != nil {
		t.Fatalf("reopen store after crash simulation: %v", err)
	}
	defer reopened.Close()

	if err := reopened.CompactLogstore(ctx, ns); err != nil {
		t.Fatalf("compact after crash-style restart: %v", err)
	}
	assertStateEquals(t, reopened, ns, "orders/sealed", []byte("sealed"))
	assertStateEquals(t, reopened, ns, "orders/live", []byte("live"))
	ln = mustNamespace(t, reopened.logstore, ns)
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.installedSnapshot == nil {
		t.Fatalf("expected compaction snapshot after crash-style restart with sealed history")
	}
	liveRef := ln.stateIndex["orders/live"]
	if liveRef == nil || liveRef.segment == nil {
		t.Fatalf("expected live tail ref after crash-style restart compaction")
	}
	if liveRef.segment == ln.installedSnapshot || liveRef.segment.name == ln.installedSnapshot.name {
		t.Fatalf("expected open tail segment to stay out of compaction snapshot")
	}
}

func TestLogstoreCompactionBackfillsPreCompactionManifestlessHistory(t *testing.T) {
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
	writeStateAndSeal(t, store, ns, "orders/legacy-a", []byte("legacy-a"))
	writeStateAndSeal(t, store, ns, "orders/legacy-b", []byte("legacy-b"))
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	legacy := filepath.Join(root, ns, "logstore", "manifest")
	if err := os.RemoveAll(legacy); err != nil {
		t.Fatalf("remove manifest dir for legacy simulation: %v", err)
	}

	reopened, err := New(Config{
		Root:                             root,
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       time.Hour,
		LogstoreCompactionMinSegments:    1,
		LogstoreCompactionMinReclaimSize: 1,
		LogstoreCompactionDeleteGrace:    time.Minute,
	})
	if err != nil {
		t.Fatalf("reopen legacy store: %v", err)
	}
	defer reopened.Close()

	if err := reopened.CompactLogstore(ctx, ns); err != nil {
		t.Fatalf("compact legacy history: %v", err)
	}
	assertStateEquals(t, reopened, ns, "orders/legacy-a", []byte("legacy-a"))
	assertStateEquals(t, reopened, ns, "orders/legacy-b", []byte("legacy-b"))
	ln := mustNamespace(t, reopened.logstore, ns)
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.installedSnapshot == nil {
		t.Fatalf("expected compaction snapshot for legacy manifestless history")
	}
}

func TestLogstoreCompactionSeesLegacyOpenOnlyManifestHistory(t *testing.T) {
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
	writeStateAndSeal(t, store, ns, "orders/legacy-open-a", []byte("legacy-open-a"))
	writeStateAndSeal(t, store, ns, "orders/legacy-open-b", []byte("legacy-open-b"))
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	rewriteManifestAsOpenOnly(t, filepath.Join(root, ns, "logstore", "manifest", "manifest.log"))

	reopened, err := New(Config{
		Root:                             root,
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       time.Hour,
		LogstoreCompactionMinSegments:    1,
		LogstoreCompactionMinReclaimSize: 1,
		LogstoreCompactionDeleteGrace:    time.Minute,
	})
	if err != nil {
		t.Fatalf("reopen legacy open-only store: %v", err)
	}
	defer reopened.Close()

	if err := reopened.CompactLogstore(ctx, ns); err != nil {
		t.Fatalf("compact legacy open-only history: %v", err)
	}
	assertStateEquals(t, reopened, ns, "orders/legacy-open-a", []byte("legacy-open-a"))
	assertStateEquals(t, reopened, ns, "orders/legacy-open-b", []byte("legacy-open-b"))
	ln := mustNamespace(t, reopened.logstore, ns)
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.installedSnapshot == nil {
		t.Fatalf("expected compaction snapshot for legacy open-only manifest history")
	}
}

func TestLogstoreCompactionLegacyOpenOnlyUpgradeLeavesNoPermanentTail(t *testing.T) {
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
	writeStateAndSeal(t, store, ns, "orders/legacy-upgrade-a", []byte("legacy-upgrade-a"))
	writeStateAndSeal(t, store, ns, "orders/legacy-upgrade-b", []byte("legacy-upgrade-b"))
	writeStateAndSeal(t, store, ns, "orders/legacy-upgrade-tail", []byte("legacy-upgrade-tail"))
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	legacyTail := newestSegmentName(t, filepath.Join(root, ns, "logstore", "segments"))
	rewriteManifestAsOpenOnly(t, filepath.Join(root, ns, "logstore", "manifest", "manifest.log"))

	reopened, err := New(Config{
		Root:                             root,
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       time.Hour,
		LogstoreCompactionMinSegments:    1,
		LogstoreCompactionMinReclaimSize: 1,
		LogstoreCompactionDeleteGrace:    time.Minute,
	})
	if err != nil {
		t.Fatalf("reopen legacy upgrade store: %v", err)
	}
	defer reopened.Close()

	if err := reopened.CompactLogstore(ctx, ns); err != nil {
		t.Fatalf("first compaction on legacy upgrade store: %v", err)
	}
	if _, err := reopened.WriteState(ctx, ns, "orders/post-upgrade", bytes.NewReader([]byte("post-upgrade")), storage.PutStateOptions{}); err != nil {
		t.Fatalf("write post-upgrade state: %v", err)
	}
	sealActiveForTest(t, mustNamespace(t, reopened.logstore, ns))
	if err := reopened.CompactLogstore(ctx, ns); err != nil {
		t.Fatalf("second compaction on legacy upgrade store: %v", err)
	}

	ln := mustNamespace(t, reopened.logstore, ns)
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if _, ok := ln.obsoleteSegments[legacyTail]; !ok {
		t.Fatalf("expected legacy tail segment %q to become compactable and obsolete after upgrade", legacyTail)
	}
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

func TestLogstoreCompactionKeepsProtectedLinkedSnapshotOutOfObsoleteSet(t *testing.T) {
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
	writeStateAndSeal(t, store, ns, "snapshot/source", []byte("snapshot-payload"))
	if err := store.CompactLogstore(ctx, ns); err != nil {
		t.Fatalf("first compaction: %v", err)
	}

	ln := mustNamespace(t, store.logstore, ns)
	ln.mu.Lock()
	first := ln.installedSnapshot
	source := ln.stateIndex["snapshot/source"]
	ln.mu.Unlock()
	if first == nil || source == nil {
		t.Fatalf("expected installed snapshot-backed source record")
	}

	writeStateAndSeal(t, store, ns, "roll/next", []byte("next-sealed"))

	linkPayload, err := encodeStateLinkPayload(stateLinkPayload{
		segment: first.name,
		offset:  source.payloadOffset,
		length:  source.payloadLen,
	})
	if err != nil {
		t.Fatalf("encode link payload: %v", err)
	}
	ref, err := ln.appendRecord(ctx, logRecordStateLink, "snapshot/live", recordMeta{
		gen:           1,
		modifiedAt:    now.Unix(),
		etag:          source.meta.etag,
		plaintextSize: source.meta.plaintextSize,
		cipherSize:    source.meta.cipherSize,
	}, bytes.NewReader(linkPayload))
	if err != nil {
		t.Fatalf("append live snapshot link: %v", err)
	}
	ref.link = &recordLink{
		path:   first.path,
		offset: source.payloadOffset,
		length: source.payloadLen,
	}
	ln.mu.Lock()
	ln.stateIndex["snapshot/live"] = ref
	ln.mu.Unlock()
	ln.mu.Lock()
	if _, ok := ln.obsoleteSnapshots[first.name]; ok {
		got := make(map[string]time.Time, len(ln.obsoleteSnapshots))
		for name, ts := range ln.obsoleteSnapshots {
			got[name] = ts
		}
		ln.mu.Unlock()
		t.Fatalf("expected protected snapshot %q to start non-obsolete before second compaction, obsolete=%v", first.name, got)
	}
	capture, skip := ln.captureCompactionLocked(logCompactionConfig{
		enabled:         true,
		minSegments:     1,
		minReclaimBytes: 1,
		deleteGrace:     time.Minute,
	})
	ln.mu.Unlock()
	if capture != nil {
		t.Fatalf("expected second compaction to be skipped while installed snapshot %q is a live link target", first.name)
	}
	if skip.reason != "protected_live_links" {
		t.Fatalf("expected protected_live_links skip, got %q", skip.reason)
	}

	if err := store.CompactLogstore(ctx, ns); err != nil {
		t.Fatalf("second compaction: %v", err)
	}

	ln.mu.Lock()
	if _, ok := ln.obsoleteSnapshots[first.name]; ok {
		got := make(map[string]time.Time, len(ln.obsoleteSnapshots))
		for name, ts := range ln.obsoleteSnapshots {
			got[name] = ts
		}
		ln.mu.Unlock()
		t.Fatalf("expected protected snapshot %q to remain out of obsolete set, obsolete=%v", first.name, got)
	}
	ln.mu.Unlock()

	now = now.Add(2 * time.Minute)
	if err := ln.cleanupObsolete(time.Minute); err != nil {
		t.Fatalf("cleanup obsolete: %v", err)
	}
	if _, err := os.Stat(first.path); err != nil {
		t.Fatalf("expected linked snapshot file %q to remain after cleanup: %v", first.path, err)
	}
	assertStateEquals(t, store, ns, "snapshot/live", []byte("snapshot-payload"))
}

func TestLogstoreCompactionBuildFailureLeavesStateUnchanged(t *testing.T) {
	root := t.TempDir()
	store := newLogStore(root, time.Now, nil, pslog.NoopLogger(), 0, 0, false)
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
	capture, _ := ln.captureCompactionLocked(logCompactionConfig{enabled: true, minSegments: 1, minReclaimBytes: 1})
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
	capture, _ := ln.captureCompactionLocked(logCompactionConfig{enabled: true, minSegments: 1, minReclaimBytes: 1})
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

func rewriteManifestAsOpenOnly(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var lines []string
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "open ") {
			lines = append(lines, line)
		}
	}
	if len(lines) == 0 {
		t.Fatalf("expected open entries in manifest %q", path)
	}
	contents := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write open-only manifest: %v", err)
	}
}

func newestSegmentName(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read segments dir: %v", err)
	}
	var newest string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name > newest {
			newest = name
		}
	}
	if newest == "" {
		t.Fatalf("expected at least one segment in %q", dir)
	}
	return newest
}

func TestWriteCompactionRecordBytes(t *testing.T) {
	buf, err := writeCompactionRecordBytes(logRecordStatePut, "k", recordMeta{gen: 1, etag: string(make([]byte, 0))}, []byte("x"))
	if err == nil && len(buf) == 0 {
		t.Fatalf("expected record bytes")
	}
}

func TestCleanupObsoleteRemovesSnapshotFiles(t *testing.T) {
	root := t.TempDir()
	store := newLogStore(root, time.Now, nil, pslog.NoopLogger(), 0, 0, false)
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

func TestLogstoreCompactionLogsStartAndCompletion(t *testing.T) {
	root := t.TempDir()
	logger := &recordingLogger{}
	store, err := New(Config{
		Root:                             root,
		Logger:                           logger,
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       time.Hour,
		LogstoreCompactionMinSegments:    1,
		LogstoreCompactionMinReclaimSize: 1,
		LogstoreCompactionDeleteGrace:    0,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	writeStateAndSeal(t, store, namespaces.Default, "logs/key", []byte("payload"))
	if err := store.CompactLogstore(context.Background(), namespaces.Default); err != nil {
		t.Fatalf("compact logstore: %v", err)
	}

	entries := logger.snapshot()
	start, ok := findRecordedLog(entries, "info", "logstore.compaction.start")
	if !ok {
		t.Fatalf("expected compaction start log, got %+v", entries)
	}
	if got := logField(start, "namespace"); got != namespaces.Default {
		t.Fatalf("expected namespace %q in start log, got %v", namespaces.Default, got)
	}
	complete, ok := findRecordedLog(entries, "info", "logstore.compaction.complete")
	if !ok {
		t.Fatalf("expected compaction completion log, got %+v", entries)
	}
	if got := logField(complete, "snapshot"); got == nil || got == "" {
		t.Fatalf("expected snapshot field in completion log, got %v", got)
	}
}

func TestLogstoreCompactionLogsDebugSkip(t *testing.T) {
	root := t.TempDir()
	logger := &recordingLogger{}
	store, err := New(Config{
		Root:                             root,
		Logger:                           logger,
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       time.Hour,
		LogstoreCompactionMinSegments:    2,
		LogstoreCompactionMinReclaimSize: 1 << 20,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	writeStateAndSeal(t, store, namespaces.Default, "skip/key", []byte("small"))
	if err := store.CompactLogstore(context.Background(), namespaces.Default); err != nil {
		t.Fatalf("compact logstore: %v", err)
	}

	entries := logger.snapshot()
	skip, ok := findRecordedLog(entries, "debug", "logstore.compaction.skip")
	if !ok {
		t.Fatalf("expected compaction skip log, got %+v", entries)
	}
	if got := logField(skip, "reason"); got != "thresholds_not_met" {
		t.Fatalf("expected thresholds_not_met skip reason, got %v", got)
	}
	if got := logField(skip, "min_segments"); got != 2 {
		t.Fatalf("expected min_segments=2, got %v", got)
	}
	if got := logField(skip, "min_reclaim_bytes"); got != int64(1<<20) {
		t.Fatalf("expected min_reclaim_bytes=1MiB, got %v", got)
	}
}

func TestLogstoreCompactionLogsCleanupComplete(t *testing.T) {
	root := t.TempDir()
	logger := &recordingLogger{}
	store := newLogStore(root, time.Now, nil, logger, 0, 0, false)
	ln, err := store.namespace("default")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	if err := ln.ensureDirs(); err != nil {
		t.Fatalf("ensure dirs: %v", err)
	}
	path := filepath.Join(ln.segmentsDir, "seg-old.log")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("write obsolete segment: %v", err)
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	store.now = func() time.Time { return now }
	ln.mu.Lock()
	ln.segments["seg-old.log"] = &logSegment{name: "seg-old.log", path: path, sealed: true}
	ln.obsoleteSegments = map[string]time.Time{"seg-old.log": now.Add(-time.Minute)}
	ln.mu.Unlock()

	if err := store.runNamespaceCompaction(ln, logCompactionConfig{enabled: false, interval: time.Hour, deleteGrace: 10 * time.Second}); err != nil {
		t.Fatalf("run namespace compaction: %v", err)
	}

	entries := logger.snapshot()
	complete, ok := findRecordedLog(entries, "debug", "logstore.compaction.cleanup.complete")
	if !ok {
		t.Fatalf("expected cleanup complete log, got %+v", entries)
	}
	if got := logField(complete, "deleted_segments"); got != 1 {
		t.Fatalf("expected deleted_segments=1, got %v", got)
	}
}

func TestCleanupObsoleteRetainsEntriesUntilDeleteSucceeds(t *testing.T) {
	root := t.TempDir()
	logger := &recordingLogger{}
	store, err := New(Config{
		Root:   root,
		Logger: logger,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	ln := mustNamespace(t, store.logstore, namespaces.Default)
	snapshotsDir := filepath.Join(root, namespaces.Default, "logstore", "snapshots")
	stalePath := filepath.Join(snapshotsDir, "snap-retry.log")
	if err := os.MkdirAll(stalePath, 0o755); err != nil {
		t.Fatalf("mkdir stale path: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stalePath, "busy"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write nested stale file: %v", err)
	}
	now := store.now()
	ln.mu.Lock()
	ln.snapshots["snap-retry.log"] = &logSegment{name: "snap-retry.log", path: stalePath}
	ln.obsoleteSnapshots = map[string]time.Time{"snap-retry.log": now.Add(-time.Minute)}
	ln.mu.Unlock()

	if _, err := ln.cleanupObsoleteDetailed(10 * time.Second); err == nil {
		t.Fatal("expected cleanup to fail while stale path is a directory")
	}

	ln.mu.Lock()
	if _, ok := ln.obsoleteSnapshots["snap-retry.log"]; !ok {
		ln.mu.Unlock()
		t.Fatal("expected obsolete snapshot to remain tracked after delete failure")
	}
	if _, ok := ln.snapshots["snap-retry.log"]; !ok {
		ln.mu.Unlock()
		t.Fatal("expected snapshot entry to remain after delete failure")
	}
	ln.mu.Unlock()

	if err := os.RemoveAll(stalePath); err != nil {
		t.Fatalf("remove stale dir: %v", err)
	}
	if err := os.WriteFile(stalePath, []byte("snapshot"), 0o644); err != nil {
		t.Fatalf("rewrite stale snapshot: %v", err)
	}

	out, err := ln.cleanupObsoleteDetailed(10 * time.Second)
	if err != nil {
		t.Fatalf("cleanup retry: %v", err)
	}
	if out.deletedSnapshots != 1 {
		t.Fatalf("expected one deleted snapshot on retry, got %+v", out)
	}
	ln.mu.Lock()
	if _, ok := ln.obsoleteSnapshots["snap-retry.log"]; ok {
		ln.mu.Unlock()
		t.Fatal("expected obsolete snapshot to be removed after successful retry")
	}
	if _, ok := ln.snapshots["snap-retry.log"]; ok {
		ln.mu.Unlock()
		t.Fatal("expected snapshot entry removed after successful retry")
	}
	ln.mu.Unlock()
}

func TestLogstoreCompactionLogsLegacyManifestMigration(t *testing.T) {
	root := t.TempDir()
	logger := &recordingLogger{}
	store, err := New(Config{
		Root:                             root,
		Logger:                           logger,
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
	writeStateAndSeal(t, store, ns, "legacy-log/a", []byte("a"))
	writeStateAndSeal(t, store, ns, "legacy-log/b", []byte("b"))
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	rewriteManifestAsOpenOnly(t, filepath.Join(root, ns, "logstore", "manifest", "manifest.log"))

	reopened, err := New(Config{
		Root:                             root,
		Logger:                           logger,
		LogstoreCompactionEnabled:        true,
		LogstoreCompactionInterval:       time.Hour,
		LogstoreCompactionMinSegments:    1,
		LogstoreCompactionMinReclaimSize: 1,
		LogstoreCompactionDeleteGrace:    time.Minute,
	})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()

	if err := reopened.CompactLogstore(ctx, ns); err != nil {
		t.Fatalf("compact logstore: %v", err)
	}

	entries := logger.snapshot()
	migrated, ok := findRecordedLog(entries, "info", "logstore.compaction.legacy_manifest_migrated")
	if !ok {
		t.Fatalf("expected legacy manifest migration log, got %+v", entries)
	}
	if got := logField(migrated, "sealed_segments"); got != 2 {
		t.Fatalf("expected sealed_segments=2, got %v", got)
	}
}

func TestLogstoreCompactionLogsValidationDriftWarning(t *testing.T) {
	root := t.TempDir()
	logger := &recordingLogger{}
	store, err := New(Config{
		Root:                               root,
		Logger:                             logger,
		LogstoreCompactionEnabled:          true,
		LogstoreCompactionInterval:         time.Hour,
		LogstoreCompactionMinSegments:      1,
		LogstoreCompactionMinReclaimSize:   1,
		LogstoreCompactionMaxIOBytesPerSec: 64 << 10,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	payload := bytes.Repeat([]byte("x"), 64<<10)
	writeStateAndSeal(t, store, namespaces.Default, "drift/key", payload)

	done := make(chan error, 1)
	go func() {
		done <- store.CompactLogstore(context.Background(), namespaces.Default)
	}()

	waitFor(t, 2*time.Second, func() bool {
		entries := logger.snapshot()
		_, ok := findRecordedLog(entries, "info", "logstore.compaction.start")
		return ok
	})

	if _, err := store.WriteState(context.Background(), namespaces.Default, "drift/key", bytes.NewReader([]byte("newer")), storage.PutStateOptions{}); err != nil {
		t.Fatalf("write drift update: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("compact logstore: %v", err)
	}

	entries := logger.snapshot()
	warn, ok := findRecordedLog(entries, "warn", "logstore.compaction.abandoned.validation_drift")
	if !ok {
		t.Fatalf("expected validation drift warning, got %+v", entries)
	}
	if got := logField(warn, "namespace"); got != namespaces.Default {
		t.Fatalf("expected namespace %q in warn log, got %v", namespaces.Default, got)
	}
}

func TestLogstoreCompactionLogsCleanupDeleteFailure(t *testing.T) {
	root := t.TempDir()
	logger := &recordingLogger{}
	store := newLogStore(root, time.Now, nil, logger, 0, 0, false)
	ln, err := store.namespace("default")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	if err := ln.ensureDirs(); err != nil {
		t.Fatalf("ensure dirs: %v", err)
	}
	path := filepath.Join(ln.snapshotsDir, "snap-dir.log")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("mkdir snapshot dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(path, "keep"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write nested file: %v", err)
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	store.now = func() time.Time { return now }
	ln.mu.Lock()
	ln.snapshots["snap-dir.log"] = &logSegment{name: "snap-dir.log", path: path}
	ln.obsoleteSnapshots = map[string]time.Time{"snap-dir.log": now.Add(-time.Minute)}
	ln.mu.Unlock()

	if _, err := ln.cleanupObsoleteDetailed(10 * time.Second); err == nil {
		t.Fatalf("expected cleanup delete failure")
	}
	entries := logger.snapshot()
	errEntry, ok := findRecordedLog(entries, "error", "logstore.compaction.cleanup.delete_failed")
	if !ok {
		t.Fatalf("expected cleanup delete failure log, got %+v", entries)
	}
	if got := logField(errEntry, "kind"); got != "snapshot" {
		t.Fatalf("expected snapshot cleanup failure kind, got %v", got)
	}
}
