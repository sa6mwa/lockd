//go:build integration && disk && query

package diskquery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queriesuite "pkt.systems/lockd/integration/query/suite"
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

func TestDiskQuerySelectors(t *testing.T) {
	queriesuite.RunSelectors(t, startDiskQueryServer)
}

func TestDiskQueryPagination(t *testing.T) {
	queriesuite.RunPagination(t, startDiskQueryServer)
}

func TestDiskQueryNamespaceIsolation(t *testing.T) {
	queriesuite.RunNamespaceIsolation(t, startDiskQueryServer)
}

func TestDiskQueryResultsSupportPublicRead(t *testing.T) {
	queriesuite.RunPublicRead(t, startDiskQueryServer)
}

func TestDiskQueryDocumentStreaming(t *testing.T) {
	queriesuite.RunDocumentStreaming(t, startDiskQueryServer)
}

func TestDiskQueryDomainDatasets(t *testing.T) {
	queriesuite.RunDomainDatasets(t, startDiskQueryServer)
}

func TestDiskQueryTxSmoke(t *testing.T) {
	queriesuite.RunTxSmoke(t, startDiskQueryServer)
}

func TestDiskQueryRawTxnSmoke(t *testing.T) {
	queriesuite.RunRawTxnSmoke(t, startDiskQueryServer)
}

func TestDiskQueryTxnMultiKey(t *testing.T) {
	queriesuite.RunTxnMultiKey(t, startDiskQueryServer)
}

func TestDiskQueryTxnMultiKeyNamespaces(t *testing.T) {
	queriesuite.RunTxnMultiKeyNamespaces(t, startDiskQueryServer)
}

func TestDiskQueryTxnSoak(t *testing.T) {
	queriesuite.RunTxnSoak(t, startDiskQueryServer)
}

func TestDiskQueryTxnKeepAliveParity(t *testing.T) {
	queriesuite.RunTxnKeepAliveParity(t, startDiskQueryServer)
}

func TestDiskQueryTxnRecoveryCommit(t *testing.T) {
	root := prepareDiskQueryRoot(t)
	cfg := diskQueryConfigWithSweeper(root, time.Hour)
	ts := startDiskQueryServerWithRootAndConfig(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("disk-recover-commit-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("disk-recover-commit-b-%d", time.Now().UnixNano())

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
	if _, err := ts.Backend().PutObject(ctx, ".txns", rec.TxnID, bytes.NewReader(buf.Bytes()), storage.PutObjectOptions{ContentType: storage.ContentTypeJSON}); err != nil {
		t.Fatalf("persist txn record: %v", err)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := ts.Stop(stopCtx); err != nil {
		t.Fatalf("stop initial server: %v", err)
	}

	cfg.SweeperInterval = 200 * time.Millisecond
	ts = startDiskQueryServerWithRootAndConfig(t, cfg)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}()

	waitCtx, waitCancel := context.WithTimeout(ctx, 12*time.Second)
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
			time.Sleep(75 * time.Millisecond)
		}
	}
	assertCommitted(keyA, "a")
	assertCommitted(keyB, "b")

	recordCtx, recordCancel := context.WithTimeout(ctx, 8*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, ts.Backend(), rec.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestDiskQueryTxnRecoveryRollbackPending(t *testing.T) {
	root := prepareDiskQueryRoot(t)
	cfg := diskQueryConfigWithSweeper(root, time.Hour)
	ts := startDiskQueryServerWithRootAndConfig(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("disk-recover-rb-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("disk-recover-rb-b-%d", time.Now().UnixNano())

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
	if _, err := ts.Backend().PutObject(ctx, ".txns", rec.TxnID, bytes.NewReader(buf.Bytes()), storage.PutObjectOptions{ContentType: storage.ContentTypeJSON}); err != nil {
		t.Fatalf("persist txn record: %v", err)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := ts.Stop(stopCtx); err != nil {
		t.Fatalf("stop initial server: %v", err)
	}

	cfg.SweeperInterval = 200 * time.Millisecond
	ts = startDiskQueryServerWithRootAndConfig(t, cfg)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}()

	waitCtx, waitCancel := context.WithTimeout(ctx, 12*time.Second)
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
			time.Sleep(75 * time.Millisecond)
		}
	}
	assertRolledBack(keyA)
	assertRolledBack(keyB)

	recordCtx, recordCancel := context.WithTimeout(ctx, 8*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, ts.Backend(), rec.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestDiskQueryTxnRecoveryRollbackPendingNamespaces(t *testing.T) {
	root := prepareDiskQueryRoot(t)
	cfg := diskQueryConfigWithSweeper(root, time.Hour)
	ts := startDiskQueryServerWithRootAndConfig(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	nsA := "alpha"
	nsB := "beta"
	keyA := fmt.Sprintf("disk-recover-rb-ns-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("disk-recover-rb-ns-b-%d", time.Now().UnixNano())

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
	if _, err := ts.Backend().PutObject(ctx, ".txns", rec.TxnID, bytes.NewReader(buf.Bytes()), storage.PutObjectOptions{ContentType: storage.ContentTypeJSON}); err != nil {
		t.Fatalf("persist txn record: %v", err)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := ts.Stop(stopCtx); err != nil {
		t.Fatalf("stop initial server: %v", err)
	}

	cfg.SweeperInterval = 200 * time.Millisecond
	ts = startDiskQueryServerWithRootAndConfig(t, cfg)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}()

	waitCtx, waitCancel := context.WithTimeout(ctx, 12*time.Second)
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
			time.Sleep(75 * time.Millisecond)
		}
	}
	assertRolledBack(nsA, keyA)
	assertRolledBack(nsB, keyB)

	recordCtx, recordCancel := context.WithTimeout(ctx, 8*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, ts.Backend(), rec.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestDiskQueryTxnReplayEndpointCommit(t *testing.T) {
	root := prepareDiskQueryRoot(t)
	cfg := diskQueryConfigWithSweeper(root, time.Hour) // disable sweeper; explicit replay
	ts := startDiskQueryServerWithRootAndConfig(t, cfg)
	httpClient := newHTTPClient(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("disk-replay-commit-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("disk-replay-commit-b-%d", time.Now().UnixNano())

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

	body, err := json.Marshal(api.TxnReplayRequest{TxnID: rec.TxnID})
	if err != nil {
		t.Fatalf("marshal replay request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL()+"/v1/txn/replay", bytes.NewReader(body))
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

	waitCtx, waitCancel := context.WithTimeout(ctx, 12*time.Second)
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

	recordCtx, recordCancel := context.WithTimeout(ctx, 8*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, ts.Backend(), rec.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestDiskQueryTxnDecisionEndpoints(t *testing.T) {
	root := prepareDiskQueryRoot(t)
	cfg := diskQueryConfigWithSweeper(root, time.Hour)
	ts := startDiskQueryServerWithRootAndConfig(t, cfg)
	httpClient := newHTTPClient(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("disk-rm-commit-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("disk-rm-commit-b-%d", time.Now().UnixNano())

	leaseA, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "txn-decision",
		TTLSeconds: 20,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	leaseB, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "txn-decision",
		TTLSeconds: 20,
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
		ExpiresAtUnix: time.Now().Add(45 * time.Second).Unix(),
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

	waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
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
			time.Sleep(75 * time.Millisecond)
		}
	}
	assertCommitted(keyA, "a")
	assertCommitted(keyB, "b")

	// Rollback path
	keyC := fmt.Sprintf("disk-rm-rb-c-%d", time.Now().UnixNano())
	leaseC, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyC,
		Owner:      "txn-decision",
		TTLSeconds: 20,
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
		ExpiresAtUnix: time.Now().Add(45 * time.Second).Unix(),
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

	waitRollbackCtx, cancelRB := context.WithTimeout(ctx, 10*time.Second)
	defer cancelRB()
	for {
		state, err := ts.Client.Get(waitRollbackCtx, keyC)
		if err == nil && !state.HasState {
			break
		}
		if waitRollbackCtx.Err() != nil {
			t.Fatalf("timeout waiting for rollback on %s: %v", keyC, err)
		}
		time.Sleep(75 * time.Millisecond)
	}
}

func TestDiskQueryTxnFanoutAcrossNodes(t *testing.T) {
	t.Run("FSNotify", func(t *testing.T) {
		runDiskQueryTxnFanoutAcrossNodes(t, true)
	})
	t.Run("Polling", func(t *testing.T) {
		runDiskQueryTxnFanoutAcrossNodes(t, false)
	})
}

func runDiskQueryTxnFanoutAcrossNodes(t *testing.T, notify bool) {
	root := prepareDiskQueryRoot(t)
	cfg := diskQueryConfigWithSweeper(root, 2*time.Second)
	cfg.DiskQueueWatch = notify
	bundlePath := cryptotest.SharedTCClientBundlePath(t)
	if bundlePath == "" {
		cfg.DisableMTLS = true
	}

	tsA := startDiskQueryServerWithRootAndConfig(t, cfg)
	cfgB := cfg
	if bundlePath != "" {
		cfgB.TCClientBundlePath = bundlePath
	}
	tsB := startDiskQueryServerWithRootAndConfig(t, cfgB)

	stop := func(ts *lockd.TestServer) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}
	t.Cleanup(func() { stop(tsB) })
	t.Cleanup(func() { stop(tsA) })

	cryptotest.RegisterRM(t, tsB, tsA)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("disk-multinode-commit-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("disk-multinode-commit-b-%d", time.Now().UnixNano())

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
		ExpiresAtUnix: time.Now().Add(45 * time.Second).Unix(),
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

func TestDiskQueryTxnFanoutRollbackAcrossNodes(t *testing.T) {
	t.Run("FSNotify", func(t *testing.T) {
		runDiskQueryTxnFanoutRollbackAcrossNodes(t, true)
	})
	t.Run("Polling", func(t *testing.T) {
		runDiskQueryTxnFanoutRollbackAcrossNodes(t, false)
	})
}

func runDiskQueryTxnFanoutRollbackAcrossNodes(t *testing.T, notify bool) {
	root := prepareDiskQueryRoot(t)
	cfg := diskQueryConfigWithSweeper(root, 2*time.Second)
	cfg.DiskQueueWatch = notify
	bundlePath := cryptotest.SharedTCClientBundlePath(t)
	if bundlePath == "" {
		cfg.DisableMTLS = true
	}

	tsA := startDiskQueryServerWithRootAndConfig(t, cfg)
	cfgB := cfg
	if bundlePath != "" {
		cfgB.TCClientBundlePath = bundlePath
	}
	tsB := startDiskQueryServerWithRootAndConfig(t, cfgB)
	stop := func(ts *lockd.TestServer) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}
	t.Cleanup(func() { stop(tsB) })
	t.Cleanup(func() { stop(tsA) })

	cryptotest.RegisterRM(t, tsB, tsA)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("disk-multinode-rollback-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("disk-multinode-rollback-b-%d", time.Now().UnixNano())

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
				t.Fatalf("timeout waiting for rollback on %s: %v", key, err)
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

func TestDiskQueryTxnReplayAfterRestartAcrossNodes(t *testing.T) {
	t.Run("FSNotify", func(t *testing.T) {
		runDiskQueryTxnReplayAfterRestartAcrossNodes(t, true)
	})
	t.Run("Polling", func(t *testing.T) {
		runDiskQueryTxnReplayAfterRestartAcrossNodes(t, false)
	})
}

func runDiskQueryTxnReplayAfterRestartAcrossNodes(t *testing.T, notify bool) {
	root := prepareDiskQueryRoot(t)
	cfg := diskQueryConfigWithSweeper(root, time.Hour) // disable sweeper; explicit replay after restart
	cfg.DiskQueueWatch = notify

	tsA := startDiskQueryServerWithRootAndConfig(t, cfg)
	tsB := startDiskQueryServerWithRootAndConfig(t, cfg)

	stop := func(ts *lockd.TestServer) {
		if ts == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}
	t.Cleanup(func() { stop(tsA) })
	t.Cleanup(func() { stop(tsB) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	commitKeyA := fmt.Sprintf("disk-restart-commit-a-%d", time.Now().UnixNano())
	commitKeyB := fmt.Sprintf("disk-restart-commit-b-%d", time.Now().UnixNano())
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

	rollbackKeyA := fmt.Sprintf("disk-restart-rollback-a-%d", time.Now().UnixNano())
	rollbackKeyB := fmt.Sprintf("disk-restart-rollback-b-%d", time.Now().UnixNano())
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
			ExpiresAtUnix: now.Add(60 * time.Second).Unix(),
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

	stop(tsA)
	stop(tsB)

	tsA = startDiskQueryServerWithRootAndConfig(t, cfg)
	tsB = startDiskQueryServerWithRootAndConfig(t, cfg)

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

	waitCommitCtx, waitCommitCancel := context.WithTimeout(ctx, 20*time.Second)
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

	waitRollbackCtx, waitRollbackCancel := context.WithTimeout(ctx, 20*time.Second)
	defer waitRollbackCancel()
	assertRolledBack := func(cli *lockdclient.Client, key string) {
		for {
			state, err := cli.Get(waitRollbackCtx, key, lockdclient.WithGetNamespace(namespaces.Default))
			if err == nil && !state.HasState {
				return
			}
			if waitRollbackCtx.Err() != nil {
				t.Fatalf("timeout waiting for rollback on %s: %v", key, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	assertRolledBack(tsA.Client, rollbackKeyA)
	assertRolledBack(tsB.Client, rollbackKeyB)

	recordCommitCtx, recordCommitCancel := context.WithTimeout(ctx, 8*time.Second)
	defer recordCommitCancel()
	if err := waitForTxnRecordDecided(recordCommitCtx, tsA.Backend(), leaseA.TxnID); err != nil {
		t.Fatalf("waiting for commit txn record cleanup: %v", err)
	}

	recordRollbackCtx, recordRollbackCancel := context.WithTimeout(ctx, 8*time.Second)
	defer recordRollbackCancel()
	if err := waitForTxnRecordDecided(recordRollbackCtx, tsA.Backend(), leaseC.TxnID); err != nil {
		t.Fatalf("waiting for rollback txn record cleanup: %v", err)
	}
}

func newHTTPClient(t testing.TB, ts *lockd.TestServer) *http.Client {
	t.Helper()
	return cryptotest.RequireTCHTTPClient(t, ts)
}

func TestDiskQueryRestartRollsBackStaged(t *testing.T) {
	root := prepareDiskQueryRoot(t)
	ts := startDiskQueryServerWithRoot(t, root)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := fmt.Sprintf("restart-staged-%d", time.Now().UnixNano())
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

	// Stop the server without releasing to simulate crash.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := ts.Stop(stopCtx); err != nil {
		t.Fatalf("stop server: %v", err)
	}

	time.Sleep(3 * time.Second) // allow lease to expire

	// Restart against the same root to simulate recovery.
	ts = startDiskQueryServerWithRoot(t, root)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}()

	lease2, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        key,
		Owner:      "restart-test",
		TTLSeconds: 10,
		BlockSecs:  1,
	})
	if err != nil {
		t.Fatalf("re-acquire after restart: %v", err)
	}
	// State should have been rolled back (never committed).
	snap, err := lease2.Get(ctx)
	if err != nil {
		t.Fatalf("get after restart: %v", err)
	}
	if snap != nil && snap.HasState {
		t.Fatalf("expected no committed state after rollback, got %+v", snap)
	}

	// No staged objects should remain in storage.
	backend := ts.Backend()
	list, err := backend.ListObjects(ctx, namespaces.Default, storage.ListOptions{Prefix: ".staging/"})
	if err != nil {
		t.Fatalf("list staged: %v", err)
	}
	if len(list.Objects) != 0 {
		t.Fatalf("expected staged cleanup, found %d objects", len(list.Objects))
	}
	_ = lease2.Release(ctx)
}

func startDiskQueryServer(t testing.TB) *lockd.TestServer {
	t.Helper()
	root := prepareDiskQueryRoot(t)
	return startDiskQueryServerWithRoot(t, root)
}

func startDiskQueryServerWithRoot(t testing.TB, root string) *lockd.TestServer {
	t.Helper()
	cfg := diskQueryConfigWithSweeper(root, 2*time.Second)
	ts := startDiskQueryServerWithRootAndConfig(t, cfg)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	})
	return ts
}

func diskQueryConfigWithSweeper(root string, interval time.Duration) lockd.Config {
	return lockd.Config{
		Store:           diskStoreURL(root),
		Listen:          "127.0.0.1:0",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          time.Minute,
		AcquireBlock:    10 * time.Second,
		SweeperInterval: interval,
	}
}

func startDiskQueryServerWithRootAndConfig(t testing.TB, cfg lockd.Config, extra ...lockd.TestServerOption) *lockd.TestServer {
	t.Helper()
	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	cryptotest.ConfigureTCAuth(t, &cfg)
	opts := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestLoggerFromTB(t, pslog.DebugLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(45*time.Second),
			lockdclient.WithCloseTimeout(45*time.Second),
			lockdclient.WithKeepAliveTimeout(45*time.Second),
			lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.DebugLevel)),
		),
	}
	opts = append(opts, extra...)
	opts = append(opts, cryptotest.SharedMTLSOptions(t)...)
	return lockd.StartTestServer(t, opts...)
}

func waitForTxnRecordDecided(ctx context.Context, backend storage.Backend, txnID string) error {
	for {
		obj, err := backend.GetObject(ctx, ".txns", txnID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil
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
		time.Sleep(50 * time.Millisecond)
	}
}

func prepareDiskQueryRoot(t testing.TB) string {
	t.Helper()
	base := os.Getenv("LOCKD_DISK_ROOT")
	if base == "" {
		t.Fatalf("LOCKD_DISK_ROOT must be set (source .env.disk before running disk query tests)")
	}
	root := filepath.Join(base, fmt.Sprintf("lockd-query-%d", time.Now().UnixNano()))
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir disk root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func diskStoreURL(root string) string {
	if root == "" {
		root = "/tmp/lockd-disk"
	}
	if !strings.HasPrefix(root, "/") {
		root = "/" + root
	}
	return (&url.URL{Scheme: "disk", Path: root}).String()
}
