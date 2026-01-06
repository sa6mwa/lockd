//go:build integration && mem && query

package memquery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	querydata "pkt.systems/lockd/integration/query/querydata"
	queriesuite "pkt.systems/lockd/integration/query/suite"
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

func TestMemQuerySelectors(t *testing.T) {
	queriesuite.RunSelectors(t, startMemQueryServer)
}

func TestMemQueryPagination(t *testing.T) {
	queriesuite.RunPagination(t, startMemQueryServer)
}

func TestMemQueryNamespaceIsolation(t *testing.T) {
	queriesuite.RunNamespaceIsolation(t, startMemQueryServer)
}

func TestMemQueryResultsSupportPublicRead(t *testing.T) {
	queriesuite.RunPublicRead(t, startMemQueryServer)
}

func TestMemQueryDocumentStreaming(t *testing.T) {
	queriesuite.RunDocumentStreaming(t, startMemQueryServer)
}

func TestMemQueryDomainDatasets(t *testing.T) {
	queriesuite.RunDomainDatasets(t, startMemQueryServer)
}

func TestMemQueryTxSmoke(t *testing.T) {
	queriesuite.RunTxSmoke(t, startMemQueryServer)
}

func TestMemQueryTxnMultiKey(t *testing.T) {
	queriesuite.RunTxnMultiKey(t, startMemQueryServer)
}

func TestMemQueryTxnMultiKeyNamespaces(t *testing.T) {
	queriesuite.RunTxnMultiKeyNamespaces(t, startMemQueryServer)
}

func TestMemQueryTxnSoak(t *testing.T) {
	queriesuite.RunTxnSoak(t, startMemQueryServer)
}

func TestMemQueryTxnKeepAliveParity(t *testing.T) {
	queriesuite.RunTxnKeepAliveParity(t, startMemQueryServer)
}

func TestMemQueryTxnRecoveryCommit(t *testing.T) {
	backend := memory.New()
	cfg := memQueryConfigWithSweeper(time.Hour)
	ts := startMemQueryServerWithBackend(t, backend, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("mem-recover-commit-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("mem-recover-commit-b-%d", time.Now().UnixNano())

	leaseA, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "txn-recover",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	leaseB, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "txn-recover",
		TTLSeconds: 30,
		TxnID:      leaseA.TxnID,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire B: %v", err)
	}
	if err := leaseA.Save(ctx, map[string]any{"value": "a"}); err != nil {
		t.Fatalf("save A: %v", err)
	}
	if err := leaseB.Save(ctx, map[string]any{"value": "b"}); err != nil {
		t.Fatalf("save B: %v", err)
	}

	now := time.Now()
	rec := core.TxnRecord{
		TxnID: leaseA.TxnID,
		State: core.TxnStateCommit,
		Participants: []core.TxnParticipant{
			{Namespace: namespaces.Default, Key: keyA},
			{Namespace: namespaces.Default, Key: keyB},
		},
		ExpiresAtUnix: now.Add(time.Minute).Unix(),
		UpdatedAtUnix: now.Unix(),
		CreatedAtUnix: now.Unix(),
	}
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(rec); err != nil {
		t.Fatalf("encode txn record: %v", err)
	}
	if _, err := backend.PutObject(ctx, ".txns", rec.TxnID, bytes.NewReader(buf.Bytes()), storage.PutObjectOptions{ContentType: storage.ContentTypeJSON}); err != nil {
		t.Fatalf("persist txn record: %v", err)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := ts.Stop(stopCtx); err != nil {
		t.Fatalf("stop initial server: %v", err)
	}

	cfg.SweeperInterval = 200 * time.Millisecond
	ts = startMemQueryServerWithBackend(t, backend, cfg)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}()

	waitCtx, waitCancel := context.WithTimeout(ctx, 8*time.Second)
	defer waitCancel()
	assertCommitted := func(key, expected string) {
		for {
			resp, err := ts.Client.Get(waitCtx, key, lockdclient.WithGetNamespace(namespaces.Default))
			if err == nil && resp.HasState {
				doc, derr := resp.Document()
				if derr != nil {
					t.Fatalf("decode %s: %v", key, derr)
				}
				if doc.Body["value"] == expected {
					return
				}
			}
			if waitCtx.Err() != nil {
				t.Fatalf("timeout waiting for %s commit: %v", key, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	assertCommitted(keyA, "a")
	assertCommitted(keyB, "b")

	recordCtx, recordCancel := context.WithTimeout(ctx, 8*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, backend, rec.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}

	reacquireCtx, reacquireCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reacquireCancel()
	if _, err := ts.Client.Acquire(reacquireCtx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "txn-recover",
		TTLSeconds: 5,
		BlockSecs:  lockdclient.BlockWaitForever,
	}); err != nil {
		t.Fatalf("reacquire keyB after recovery: %v", err)
	}
}

func TestMemQueryTxnRecoveryRollbackPending(t *testing.T) {
	backend := memory.New()
	cfg := memQueryConfigWithSweeper(time.Hour)
	ts := startMemQueryServerWithBackend(t, backend, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("mem-recover-rb-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("mem-recover-rb-b-%d", time.Now().UnixNano())

	leaseA, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "txn-recover",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	leaseB, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "txn-recover",
		TTLSeconds: 30,
		TxnID:      leaseA.TxnID,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire B: %v", err)
	}
	if err := leaseA.Save(ctx, map[string]any{"value": "rollback-a"}); err != nil {
		t.Fatalf("save A: %v", err)
	}
	if err := leaseB.Save(ctx, map[string]any{"value": "rollback-b"}); err != nil {
		t.Fatalf("save B: %v", err)
	}

	now := time.Now().Add(-time.Minute)
	rec := core.TxnRecord{
		TxnID: leaseA.TxnID,
		State: core.TxnStatePending,
		Participants: []core.TxnParticipant{
			{Namespace: namespaces.Default, Key: keyA},
			{Namespace: namespaces.Default, Key: keyB},
		},
		ExpiresAtUnix: now.Unix(),
		UpdatedAtUnix: now.Unix(),
		CreatedAtUnix: now.Unix(),
	}
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(rec); err != nil {
		t.Fatalf("encode txn record: %v", err)
	}
	if _, err := backend.PutObject(ctx, ".txns", rec.TxnID, bytes.NewReader(buf.Bytes()), storage.PutObjectOptions{ContentType: storage.ContentTypeJSON}); err != nil {
		t.Fatalf("persist txn record: %v", err)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := ts.Stop(stopCtx); err != nil {
		t.Fatalf("stop initial server: %v", err)
	}

	cfg.SweeperInterval = 200 * time.Millisecond
	ts = startMemQueryServerWithBackend(t, backend, cfg)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}()

	waitCtx, waitCancel := context.WithTimeout(ctx, 8*time.Second)
	defer waitCancel()
	assertRolledBack := func(key string) {
		for {
			resp, err := ts.Client.Get(waitCtx, key, lockdclient.WithGetNamespace(namespaces.Default))
			if err == nil && !resp.HasState {
				return
			}
			if waitCtx.Err() != nil {
				t.Fatalf("timeout waiting for rollback on %s: %v", key, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	assertRolledBack(keyA)
	assertRolledBack(keyB)

	recordCtx, recordCancel := context.WithTimeout(ctx, 8*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, backend, rec.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestMemQueryTxnRecoveryRollbackPendingNamespaces(t *testing.T) {
	backend := memory.New()
	cfg := memQueryConfigWithSweeper(time.Hour)
	ts := startMemQueryServerWithBackend(t, backend, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nsA := "alpha"
	nsB := "beta"
	keyA := fmt.Sprintf("mem-recover-rb-ns-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("mem-recover-rb-ns-b-%d", time.Now().UnixNano())

	leaseA, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  nsA,
		Key:        keyA,
		Owner:      "txn-recover",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	leaseB, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  nsB,
		Key:        keyB,
		Owner:      "txn-recover",
		TTLSeconds: 30,
		TxnID:      leaseA.TxnID,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire B: %v", err)
	}
	if err := leaseA.Save(ctx, map[string]any{"ns": nsA}); err != nil {
		t.Fatalf("save A: %v", err)
	}
	if err := leaseB.Save(ctx, map[string]any{"ns": nsB}); err != nil {
		t.Fatalf("save B: %v", err)
	}

	now := time.Now().Add(-time.Minute)
	rec := core.TxnRecord{
		TxnID: leaseA.TxnID,
		State: core.TxnStatePending,
		Participants: []core.TxnParticipant{
			{Namespace: nsA, Key: keyA},
			{Namespace: nsB, Key: keyB},
		},
		ExpiresAtUnix: now.Unix(),
		UpdatedAtUnix: now.Unix(),
		CreatedAtUnix: now.Unix(),
	}
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(rec); err != nil {
		t.Fatalf("encode txn record: %v", err)
	}
	if _, err := backend.PutObject(ctx, ".txns", rec.TxnID, bytes.NewReader(buf.Bytes()), storage.PutObjectOptions{ContentType: storage.ContentTypeJSON}); err != nil {
		t.Fatalf("persist txn record: %v", err)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := ts.Stop(stopCtx); err != nil {
		t.Fatalf("stop initial server: %v", err)
	}

	cfg.SweeperInterval = 200 * time.Millisecond
	ts = startMemQueryServerWithBackend(t, backend, cfg)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}()

	waitCtx, waitCancel := context.WithTimeout(ctx, 8*time.Second)
	defer waitCancel()
	assertRolledBack := func(ns, key string) {
		for {
			resp, err := ts.Client.Get(waitCtx, key, lockdclient.WithGetNamespace(ns))
			if err == nil && !resp.HasState {
				return
			}
			if waitCtx.Err() != nil {
				t.Fatalf("timeout waiting for rollback on %s/%s: %v", ns, key, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	assertRolledBack(nsA, keyA)
	assertRolledBack(nsB, keyB)

	recordCtx, recordCancel := context.WithTimeout(ctx, 8*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, backend, rec.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestMemQueryTxnReplayEndpointCommit(t *testing.T) {
	cfg := memQueryConfigWithSweeper(time.Hour) // disable sweeper, rely on endpoint
	ts := startMemQueryServerWithBackend(t, nil, cfg)
	httpClient := newHTTPClient(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("mem-replay-commit-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("mem-replay-commit-b-%d", time.Now().UnixNano())

	leaseA, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "txn-replay",
		TTLSeconds: 15,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	leaseB, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "txn-replay",
		TTLSeconds: 15,
		TxnID:      leaseA.TxnID,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire B: %v", err)
	}
	if err := leaseA.Save(ctx, map[string]any{"value": "a"}); err != nil {
		t.Fatalf("save A: %v", err)
	}
	if err := leaseB.Save(ctx, map[string]any{"value": "b"}); err != nil {
		t.Fatalf("save B: %v", err)
	}

	rec := core.TxnRecord{
		TxnID: leaseA.TxnID,
		State: core.TxnStateCommit,
		Participants: []core.TxnParticipant{
			{Namespace: namespaces.Default, Key: keyA},
			{Namespace: namespaces.Default, Key: keyB},
		},
		ExpiresAtUnix: time.Now().Add(30 * time.Second).Unix(),
		UpdatedAtUnix: time.Now().Unix(),
		CreatedAtUnix: time.Now().Unix(),
	}
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(rec); err != nil {
		t.Fatalf("encode txn record: %v", err)
	}
	if _, err := ts.Backend().PutObject(ctx, ".txns", rec.TxnID, bytes.NewReader(buf.Bytes()), storage.PutObjectOptions{ContentType: storage.ContentTypeJSON}); err != nil {
		t.Fatalf("persist txn record: %v", err)
	}

	reqBody, err := json.Marshal(api.TxnReplayRequest{TxnID: rec.TxnID})
	if err != nil {
		t.Fatalf("marshal replay request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL()+"/v1/txn/replay", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("new replay request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("replay request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("replay status=%d", resp.StatusCode)
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer waitCancel()
	assertCommitted := func(key, expected string) {
		for {
			state, err := ts.Client.Get(waitCtx, key)
			if err == nil && state.HasState {
				doc, derr := state.Document()
				if derr != nil {
					t.Fatalf("decode %s: %v", key, derr)
				}
				if doc.Body["value"] == expected {
					return
				}
			}
			if waitCtx.Err() != nil {
				t.Fatalf("timeout waiting for %s commit: %v", key, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	assertCommitted(keyA, "a")
	assertCommitted(keyB, "b")

	recordCtx, recordCancel := context.WithTimeout(ctx, 5*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, ts.Backend(), rec.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestMemQueryTxnDecisionEndpoints(t *testing.T) {
	cfg := memQueryConfigWithSweeper(time.Hour)
	ts := startMemQueryServerWithBackend(t, nil, cfg)
	httpClient := newHTTPClient(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("mem-rm-commit-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("mem-rm-commit-b-%d", time.Now().UnixNano())

	leaseA, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "txn-decision",
		TTLSeconds: 15,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	leaseB, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "txn-decision",
		TTLSeconds: 15,
		TxnID:      leaseA.TxnID,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire B: %v", err)
	}
	if err := leaseA.Save(ctx, map[string]any{"value": "a"}); err != nil {
		t.Fatalf("save A: %v", err)
	}
	if err := leaseB.Save(ctx, map[string]any{"value": "b"}); err != nil {
		t.Fatalf("save B: %v", err)
	}

	decReq := api.TxnDecisionRequest{
		TxnID: leaseA.TxnID,
		State: "commit",
		Participants: []api.TxnParticipant{
			{Namespace: namespaces.Default, Key: keyA},
			{Namespace: namespaces.Default, Key: keyB},
		},
		ExpiresAtUnix: time.Now().Add(30 * time.Second).Unix(),
	}
	body, err := json.Marshal(decReq)
	if err != nil {
		t.Fatalf("marshal decision: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL()+"/v1/txn/decide", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new decision request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("decision request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("decision status=%d", resp.StatusCode)
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer waitCancel()
	assertCommitted := func(key, expected string) {
		for {
			state, err := ts.Client.Get(waitCtx, key)
			if err == nil && state.HasState {
				doc, derr := state.Document()
				if derr != nil {
					t.Fatalf("decode %s: %v", key, derr)
				}
				if doc.Body["value"] == expected {
					return
				}
			}
			if waitCtx.Err() != nil {
				t.Fatalf("timeout waiting for %s commit: %v", key, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	assertCommitted(keyA, "a")
	assertCommitted(keyB, "b")

	// Rollback a new staged state to exercise rollback endpoint.
	keyC := fmt.Sprintf("mem-rm-rb-c-%d", time.Now().UnixNano())
	leaseC, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyC,
		Owner:      "txn-decision",
		TTLSeconds: 15,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire C: %v", err)
	}
	if err := leaseC.Save(ctx, map[string]any{"value": "rollback"}); err != nil {
		t.Fatalf("save C: %v", err)
	}
	rbReq := api.TxnDecisionRequest{
		TxnID: leaseC.TxnID,
		State: "rollback",
		Participants: []api.TxnParticipant{
			{Namespace: namespaces.Default, Key: keyC},
		},
		ExpiresAtUnix: time.Now().Add(30 * time.Second).Unix(),
	}
	rbBody, err := json.Marshal(rbReq)
	if err != nil {
		t.Fatalf("marshal rb: %v", err)
	}
	rbHTTP, err := http.NewRequest(http.MethodPost, ts.URL()+"/v1/txn/decide", bytes.NewReader(rbBody))
	if err != nil {
		t.Fatalf("new rb request: %v", err)
	}
	rbHTTP.Header.Set("Content-Type", "application/json")
	rbResp, err := httpClient.Do(rbHTTP)
	if err != nil {
		t.Fatalf("rollback request failed: %v", err)
	}
	rbResp.Body.Close()
	if rbResp.StatusCode != http.StatusOK {
		t.Fatalf("rollback status=%d", rbResp.StatusCode)
	}

	waitRollbackCtx, cancelRB := context.WithTimeout(ctx, 8*time.Second)
	defer cancelRB()
	for {
		state, err := ts.Client.Get(waitRollbackCtx, keyC)
		if err == nil && !state.HasState {
			break
		}
		if waitRollbackCtx.Err() != nil {
			t.Fatalf("timeout waiting for rollback on %s: %v", keyC, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestMemQueryTxnFanoutAcrossNodes(t *testing.T) {
	backend := memory.New()
	cfg := memQueryConfigWithSweeper(2 * time.Second)
	bundlePath := cryptotest.SharedTCClientBundlePath(t)
	if bundlePath == "" {
		cfg.DisableMTLS = true
	}

	tsA := startMemQueryServerWithBackend(t, backend, cfg)
	cfgB := cfg
	if bundlePath != "" {
		cfgB.TCClientBundlePath = bundlePath
	}
	tsB := startMemQueryServerWithBackend(t, backend, cfgB)

	stop := func(ts *lockd.TestServer) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}
	t.Cleanup(func() { stop(tsB) })
	t.Cleanup(func() { stop(tsA) })

	cryptotest.RegisterRM(t, tsB, tsA)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("mem-multinode-commit-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("mem-multinode-commit-b-%d", time.Now().UnixNano())

	leaseA, err := tsA.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "multinode-commit",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	leaseB, err := tsB.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "multinode-commit",
		TTLSeconds: 30,
		TxnID:      leaseA.TxnID,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire B: %v", err)
	}
	if err := leaseA.Save(ctx, map[string]any{"value": "a"}); err != nil {
		t.Fatalf("save A: %v", err)
	}
	if err := leaseB.Save(ctx, map[string]any{"value": "b"}); err != nil {
		t.Fatalf("save B: %v", err)
	}

	httpClient := newHTTPClient(t, tsB)
	decReq := api.TxnDecisionRequest{
		TxnID: leaseA.TxnID,
		State: "commit",
		Participants: []api.TxnParticipant{
			{Namespace: namespaces.Default, Key: keyA},
			{Namespace: namespaces.Default, Key: keyB},
		},
		ExpiresAtUnix: time.Now().Add(30 * time.Second).Unix(),
	}
	body, err := json.Marshal(decReq)
	if err != nil {
		t.Fatalf("marshal decision: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, tsB.URL()+"/v1/txn/decide", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new decision request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("decision request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("decision status=%d", resp.StatusCode)
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
	defer waitCancel()
	assertCommitted := func(cli *lockdclient.Client, key, expected string) {
		for {
			state, err := cli.Get(waitCtx, key, lockdclient.WithGetNamespace(namespaces.Default))
			if err == nil && state.HasState {
				doc, derr := state.Document()
				if derr != nil {
					t.Fatalf("decode %s: %v", key, derr)
				}
				if doc.Body["value"] == expected {
					return
				}
			}
			if waitCtx.Err() != nil {
				t.Fatalf("timeout waiting for %s commit: %v", key, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	assertCommitted(tsA.Client, keyA, "a")
	assertCommitted(tsB.Client, keyB, "b")

	recordCtx, recordCancel := context.WithTimeout(ctx, 5*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, tsA.Backend(), leaseA.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestMemQueryTxnFanoutRollbackAcrossNodes(t *testing.T) {
	backend := memory.New()
	cfg := memQueryConfigWithSweeper(2 * time.Second)
	bundlePath := cryptotest.SharedTCClientBundlePath(t)
	if bundlePath == "" {
		cfg.DisableMTLS = true
	}

	tsA := startMemQueryServerWithBackend(t, backend, cfg)
	cfgB := cfg
	if bundlePath != "" {
		cfgB.TCClientBundlePath = bundlePath
	}
	tsB := startMemQueryServerWithBackend(t, backend, cfgB)

	stop := func(ts *lockd.TestServer) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}
	t.Cleanup(func() { stop(tsB) })
	t.Cleanup(func() { stop(tsA) })

	cryptotest.RegisterRM(t, tsB, tsA)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("mem-multinode-rollback-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("mem-multinode-rollback-b-%d", time.Now().UnixNano())

	leaseA, err := tsA.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "multinode-rollback",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	leaseB, err := tsB.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "multinode-rollback",
		TTLSeconds: 30,
		TxnID:      leaseA.TxnID,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire B: %v", err)
	}
	if err := leaseA.Save(ctx, map[string]any{"value": "rollback-a"}); err != nil {
		t.Fatalf("save A: %v", err)
	}
	if err := leaseB.Save(ctx, map[string]any{"value": "rollback-b"}); err != nil {
		t.Fatalf("save B: %v", err)
	}

	httpClient := newHTTPClient(t, tsB)
	rbReq := api.TxnDecisionRequest{
		TxnID: leaseA.TxnID,
		State: "rollback",
		Participants: []api.TxnParticipant{
			{Namespace: namespaces.Default, Key: keyA},
			{Namespace: namespaces.Default, Key: keyB},
		},
	}
	rbBody, err := json.Marshal(rbReq)
	if err != nil {
		t.Fatalf("marshal rollback: %v", err)
	}
	rbHTTP, err := http.NewRequest(http.MethodPost, tsB.URL()+"/v1/txn/decide", bytes.NewReader(rbBody))
	if err != nil {
		t.Fatalf("new rollback request: %v", err)
	}
	rbHTTP.Header.Set("Content-Type", "application/json")
	rbResp, err := httpClient.Do(rbHTTP)
	if err != nil {
		t.Fatalf("rollback request failed: %v", err)
	}
	rbResp.Body.Close()
	if rbResp.StatusCode != http.StatusOK {
		t.Fatalf("rollback status=%d", rbResp.StatusCode)
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
	defer waitCancel()
	assertRolledBack := func(cli *lockdclient.Client, key string) {
		for {
			state, err := cli.Get(waitCtx, key, lockdclient.WithGetNamespace(namespaces.Default))
			if err == nil && !state.HasState {
				return
			}
			if waitCtx.Err() != nil {
				t.Fatalf("timeout waiting for %s rollback: %v", key, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	assertRolledBack(tsA.Client, keyA)
	assertRolledBack(tsB.Client, keyB)

	recordCtx, recordCancel := context.WithTimeout(ctx, 5*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, tsA.Backend(), leaseA.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestMemQueryTxnReplayAfterRestartAcrossNodes(t *testing.T) {
	backend := memory.New()
	cfg := memQueryConfigWithSweeper(time.Hour) // disable sweeper; explicit replay after restart
	tsA := startMemQueryServerWithBackend(t, backend, cfg)
	tsB := startMemQueryServerWithBackend(t, backend, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	commitKeyA := fmt.Sprintf("mem-restart-commit-a-%d", time.Now().UnixNano())
	commitKeyB := fmt.Sprintf("mem-restart-commit-b-%d", time.Now().UnixNano())
	leaseA, err := tsA.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        commitKeyA,
		Owner:      "restart-commit",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire commit A: %v", err)
	}
	leaseB, err := tsB.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        commitKeyB,
		Owner:      "restart-commit",
		TTLSeconds: 30,
		TxnID:      leaseA.TxnID,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire commit B: %v", err)
	}
	if err := leaseA.Save(ctx, map[string]any{"value": "a"}); err != nil {
		t.Fatalf("save commit A: %v", err)
	}
	if err := leaseB.Save(ctx, map[string]any{"value": "b"}); err != nil {
		t.Fatalf("save commit B: %v", err)
	}

	rollbackKeyA := fmt.Sprintf("mem-restart-rollback-a-%d", time.Now().UnixNano())
	rollbackKeyB := fmt.Sprintf("mem-restart-rollback-b-%d", time.Now().UnixNano())
	leaseC, err := tsA.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        rollbackKeyA,
		Owner:      "restart-rollback",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire rollback A: %v", err)
	}
	leaseD, err := tsB.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        rollbackKeyB,
		Owner:      "restart-rollback",
		TTLSeconds: 30,
		TxnID:      leaseC.TxnID,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire rollback B: %v", err)
	}
	if err := leaseC.Save(ctx, map[string]any{"value": "rollback-a"}); err != nil {
		t.Fatalf("save rollback A: %v", err)
	}
	if err := leaseD.Save(ctx, map[string]any{"value": "rollback-b"}); err != nil {
		t.Fatalf("save rollback B: %v", err)
	}

	persistRecord := func(txnID string, state core.TxnState, participants []core.TxnParticipant) {
		now := time.Now()
		rec := core.TxnRecord{
			TxnID:         txnID,
			State:         state,
			Participants:  participants,
			ExpiresAtUnix: now.Add(45 * time.Second).Unix(),
			UpdatedAtUnix: now.Unix(),
			CreatedAtUnix: now.Unix(),
		}
		buf := &bytes.Buffer{}
		if err := json.NewEncoder(buf).Encode(rec); err != nil {
			t.Fatalf("encode txn record: %v", err)
		}
		if _, err := tsA.Backend().PutObject(ctx, ".txns", rec.TxnID, bytes.NewReader(buf.Bytes()), storage.PutObjectOptions{
			ContentType: storage.ContentTypeJSON,
		}); err != nil {
			t.Fatalf("persist txn record: %v", err)
		}
	}
	persistRecord(leaseA.TxnID, core.TxnStateCommit, []core.TxnParticipant{
		{Namespace: namespaces.Default, Key: commitKeyA},
		{Namespace: namespaces.Default, Key: commitKeyB},
	})
	persistRecord(leaseC.TxnID, core.TxnStateRollback, []core.TxnParticipant{
		{Namespace: namespaces.Default, Key: rollbackKeyA},
		{Namespace: namespaces.Default, Key: rollbackKeyB},
	})

	stop := func(ts *lockd.TestServer) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}
	stop(tsA)
	stop(tsB)
	tsA.Client = nil
	tsB.Client = nil

	tsA = startMemQueryServerWithBackend(t, backend, cfg)
	tsB = startMemQueryServerWithBackend(t, backend, cfg)
	t.Cleanup(func() { stop(tsB) })
	t.Cleanup(func() { stop(tsA) })

	httpClient := newHTTPClient(t, tsA)
	replay := func(txnID string) {
		body, err := json.Marshal(api.TxnReplayRequest{TxnID: txnID})
		if err != nil {
			t.Fatalf("marshal replay request: %v", err)
		}
		req, err := http.NewRequest(http.MethodPost, tsA.URL()+"/v1/txn/replay", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("new replay request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("replay request failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("replay status=%d", resp.StatusCode)
		}
	}
	replay(leaseA.TxnID)
	replay(leaseC.TxnID)

	waitCommitCtx, waitCommitCancel := context.WithTimeout(ctx, 15*time.Second)
	defer waitCommitCancel()
	assertCommitted := func(cli *lockdclient.Client, key, expected string) {
		for {
			state, err := cli.Get(waitCommitCtx, key, lockdclient.WithGetNamespace(namespaces.Default))
			if err == nil && state.HasState {
				doc, derr := state.Document()
				if derr != nil {
					t.Fatalf("decode %s: %v", key, derr)
				}
				if doc.Body["value"] == expected {
					return
				}
			}
			if waitCommitCtx.Err() != nil {
				t.Fatalf("timeout waiting for %s commit: %v", key, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	assertCommitted(tsA.Client, commitKeyA, "a")
	assertCommitted(tsB.Client, commitKeyB, "b")

	waitRollbackCtx, waitRollbackCancel := context.WithTimeout(ctx, 15*time.Second)
	defer waitRollbackCancel()
	assertRolledBack := func(cli *lockdclient.Client, key string) {
		for {
			state, err := cli.Get(waitRollbackCtx, key, lockdclient.WithGetNamespace(namespaces.Default))
			if err == nil && !state.HasState {
				return
			}
			if waitRollbackCtx.Err() != nil {
				t.Fatalf("timeout waiting for %s rollback: %v", key, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	assertRolledBack(tsA.Client, rollbackKeyA)
	assertRolledBack(tsB.Client, rollbackKeyB)

	recordCommitCtx, recordCommitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer recordCommitCancel()
	if err := waitForTxnRecordDecided(recordCommitCtx, tsA.Backend(), leaseA.TxnID); err != nil {
		t.Fatalf("waiting for commit txn record cleanup: %v", err)
	}

	recordRollbackCtx, recordRollbackCancel := context.WithTimeout(ctx, 5*time.Second)
	defer recordRollbackCancel()
	if err := waitForTxnRecordDecided(recordRollbackCtx, tsA.Backend(), leaseC.TxnID); err != nil {
		t.Fatalf("waiting for rollback txn record cleanup: %v", err)
	}
}

func TestMemNamespaceQueryConfig(t *testing.T) {
	ts := startMemQueryServer(t)
	httpClient := newHTTPClient(t, ts)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	configRes, err := ts.Client.GetNamespaceConfig(ctx, namespaces.Default)
	if err != nil {
		t.Fatalf("get namespace config: %v", err)
	}
	cfg := configRes.Config
	etag := configRes.ETag
	if cfg.Query.PreferredEngine != "scan" || cfg.Query.FallbackEngine != "none" {
		t.Fatalf("unexpected default config: %+v", cfg)
	}

	updateReq := api.NamespaceConfigRequest{
		Namespace: namespaces.Default,
		Query: &api.NamespaceQueryConfig{
			PreferredEngine: "index",
			FallbackEngine:  "scan",
		},
	}
	updatedRes, err := ts.Client.UpdateNamespaceConfig(ctx, updateReq, lockdclient.NamespaceConfigOptions{IfMatch: etag})
	if err != nil {
		t.Fatalf("update namespace config: %v", err)
	}
	if updatedRes.Config.Query.PreferredEngine != "index" || updatedRes.Config.Query.FallbackEngine != "scan" {
		t.Fatalf("unexpected updated config: %+v", updatedRes.Config)
	}
	newETag := updatedRes.ETag

	querydata.SeedState(ctx, t, ts.Client, "", "namespace-config-check", map[string]any{"status": "open"})
	reqBody, err := json.Marshal(api.QueryRequest{
		Namespace: namespaces.Default,
		Selector:  api.Selector{},
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("marshal query request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL()+"/v1/query", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("new query request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("query request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query status = %d", resp.StatusCode)
	}

	resetReq := api.NamespaceConfigRequest{
		Namespace: namespaces.Default,
		Query: &api.NamespaceQueryConfig{
			PreferredEngine: "scan",
			FallbackEngine:  "none",
		},
	}
	if _, err := ts.Client.UpdateNamespaceConfig(ctx, resetReq, lockdclient.NamespaceConfigOptions{IfMatch: newETag}); err != nil {
		t.Fatalf("reset namespace config: %v", err)
	}
}

func TestMemQueryHiddenKeys(t *testing.T) {
	ts := startMemQueryServer(t)
	httpClient := newHTTPClient(t, ts)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	visible := "hidden-visible"
	hidden := "hidden-secret"
	querydata.SeedState(ctx, t, ts.Client, "", visible, map[string]any{"status": "open"})
	querydata.SeedState(ctx, t, ts.Client, "", hidden, map[string]any{"status": "open"})
	markKeyHidden(ctx, t, ts.Client, namespaces.Default, hidden)

	body, err := json.Marshal(api.QueryRequest{
		Namespace: namespaces.Default,
		Selector:  api.Selector{},
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL()+"/v1/query", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("query request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status %d", resp.StatusCode)
	}
	var qr api.QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&qr); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	querydata.ExpectKeySet(t, qr.Keys, []string{visible})
}

func TestMemQueryRestartRollsBackStaged(t *testing.T) {
	backend := memory.New()
	cfg := memQueryConfigWithSweeper(2 * time.Second)
	ts := startMemQueryServerWithBackend(t, backend, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	key := fmt.Sprintf("mem-restart-staged-%d", time.Now().UnixNano())
	lease, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        key,
		Owner:      "restart-test",
		TTLSeconds: 2,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := lease.Save(ctx, map[string]any{"status": "staged-only"}); err != nil {
		t.Fatalf("save staged: %v", err)
	}

	// Simulate crash by stopping without releasing.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := ts.Stop(stopCtx); err != nil {
		t.Fatalf("stop server: %v", err)
	}

	time.Sleep(3 * time.Second) // allow TTL to expire

	ts = startMemQueryServerWithBackend(t, backend, cfg)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	})

	lease2, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        key,
		Owner:      "restart-test",
		TTLSeconds: 5,
		BlockSecs:  1,
	})
	if err != nil {
		t.Fatalf("re-acquire after restart: %v", err)
	}
	defer lease2.Release(ctx) //nolint:errcheck

	snap, err := lease2.Get(ctx)
	if err != nil {
		t.Fatalf("get after restart: %v", err)
	}
	if snap != nil && snap.HasState {
		t.Fatalf("expected no committed state after rollback, got %+v", snap)
	}

	list, err := backend.ListObjects(ctx, namespaces.Default, storage.ListOptions{Prefix: ".staging/"})
	if err != nil {
		t.Fatalf("list staged: %v", err)
	}
	if len(list.Objects) != 0 {
		t.Fatalf("expected staged cleanup, found %d objects", len(list.Objects))
	}
}

func startMemQueryServer(t testing.TB) *lockd.TestServer {
	t.Helper()
	cfg := memQueryConfigWithSweeper(2 * time.Second)
	ts := startMemQueryServerWithBackend(t, nil, cfg)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	})
	return ts
}

func memQueryConfigWithSweeper(interval time.Duration) lockd.Config {
	return lockd.Config{
		Store:           "mem://",
		Listen:          "127.0.0.1:0",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          time.Minute,
		AcquireBlock:    10 * time.Second,
		SweeperInterval: interval,
	}
}

func startMemQueryServerWithBackend(t testing.TB, backend storage.Backend, cfg lockd.Config) *lockd.TestServer {
	t.Helper()
	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	cryptotest.ConfigureTCAuth(t, &cfg)
	opts := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestLoggerFromTB(t, pslog.DebugLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(30*time.Second),
			lockdclient.WithCloseTimeout(30*time.Second),
			lockdclient.WithKeepAliveTimeout(30*time.Second),
			lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.DebugLevel)),
		),
	}
	if backend != nil {
		opts = append(opts, lockd.WithTestBackend(backend))
	}
	opts = append(opts, cryptotest.SharedMTLSOptions(t)...)
	return lockd.StartTestServer(t, opts...)
}

func waitForTxnRecordDecided(ctx context.Context, backend storage.Backend, txnID string) error {
	for {
		obj, err := backend.GetObject(ctx, ".txns", txnID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				if ctx.Err() != nil {
					return fmt.Errorf("txn record %s not found before timeout", txnID)
				}
				time.Sleep(25 * time.Millisecond)
				continue
			}
			if errors.Is(err, context.DeadlineExceeded) {
				if ctx.Err() != nil {
					return fmt.Errorf("txn record %s still pending after timeout", txnID)
				}
				continue
			}
			return err
		}
		var rec core.TxnRecord
		decodeErr := json.NewDecoder(obj.Reader).Decode(&rec)
		_ = obj.Reader.Close()
		if decodeErr != nil {
			return decodeErr
		}
		if rec.State != "" && rec.State != core.TxnStatePending {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("txn record %s still pending after timeout", txnID)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func newHTTPClient(t testing.TB, ts *lockd.TestServer) *http.Client {
	t.Helper()
	return cryptotest.RequireTCHTTPClient(t, ts)
}

func markKeyHidden(ctx context.Context, t testing.TB, cli *lockdclient.Client, namespace, key string) {
	t.Helper()
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespace,
		Key:        key,
		Owner:      "mem-query-hidden",
		TTLSeconds: 30,
	})
	if err != nil {
		t.Fatalf("acquire %s/%s: %v", namespace, key, err)
	}
	defer lease.Release(ctx)
	_, err = lease.UpdateMetadata(ctx, lockdclient.MetadataOptions{
		QueryHidden: lockdclient.Bool(true),
	})
	if err != nil {
		t.Fatalf("update metadata: %v", err)
	}
}
