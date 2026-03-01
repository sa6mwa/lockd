//go:build integration && nfs && query

package nfsquery

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
	"pkt.systems/lockd/integration/internal/hatest"
	queriesuite "pkt.systems/lockd/integration/query/suite"
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

func TestNFSQuerySelectors(t *testing.T) {
	queriesuite.RunSelectors(t, startNFSQueryServer)
}

func TestNFSQueryPagination(t *testing.T) {
	queriesuite.RunPagination(t, startNFSQueryServer)
}

func TestNFSQueryNamespaceIsolation(t *testing.T) {
	queriesuite.RunNamespaceIsolation(t, startNFSQueryServer)
}

func TestNFSQueryResultsSupportPublicRead(t *testing.T) {
	queriesuite.RunPublicRead(t, startNFSQueryServer)
}

func TestNFSQueryDocumentStreaming(t *testing.T) {
	queriesuite.RunDocumentStreaming(t, startNFSQueryServer)
}

func TestNFSQueryDocumentStreamingFlowControl(t *testing.T) {
	queriesuite.RunDocumentStreamingFlowControl(t, startNFSQueryServer)
}

func TestNFSQueryIndexRebuildUpgrade(t *testing.T) {
	queriesuite.RunIndexRebuildUpgrade(t, startNFSQueryServer)
}

func TestNFSQueryLargeNamespaceLowMatchKeys(t *testing.T) {
	queriesuite.RunLargeNamespaceLowMatchKeys(t, startNFSQueryServer)
}

func TestNFSQueryLargeNamespaceLowMatchDocuments(t *testing.T) {
	queriesuite.RunLargeNamespaceLowMatchDocuments(t, startNFSQueryServer)
}

func TestNFSQueryDomainDatasets(t *testing.T) {
	queriesuite.RunDomainDatasets(t, startNFSQueryServer)
}

func TestNFSQueryTxSmoke(t *testing.T) {
	queriesuite.RunTxSmoke(t, startNFSQueryServer)
}

func TestNFSQueryTxnMultiKey(t *testing.T) {
	queriesuite.RunTxnMultiKey(t, startNFSQueryServer)
}

func TestNFSQueryTxnMultiKeyNamespaces(t *testing.T) {
	queriesuite.RunTxnMultiKeyNamespaces(t, startNFSQueryServer)
}

func TestNFSQueryTxnRecoveryCommit(t *testing.T) {
	root := prepareNFSQueryRoot(t)
	cfg := nfsQueryConfigWithSweeper(root, time.Hour)
	ts := startNFSQueryServerWithRootAndConfig(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("nfs-recover-commit-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("nfs-recover-commit-b-%d", time.Now().UnixNano())

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

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer stopCancel()
	if err := ts.Stop(stopCtx); err != nil {
		t.Fatalf("stop initial server: %v", err)
	}

	cfg.SweeperInterval = 200 * time.Millisecond
	ts = startNFSQueryServerWithRootAndConfig(t, cfg)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}()

	waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
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

	recordCtx, recordCancel := context.WithTimeout(ctx, 10*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, ts.Backend(), rec.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestNFSQueryTxnRecoveryRollbackPending(t *testing.T) {
	root := prepareNFSQueryRoot(t)
	cfg := nfsQueryConfigWithSweeper(root, time.Hour)
	ts := startNFSQueryServerWithRootAndConfig(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("nfs-recover-rb-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("nfs-recover-rb-b-%d", time.Now().UnixNano())

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

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer stopCancel()
	if err := ts.Stop(stopCtx); err != nil {
		t.Fatalf("stop initial server: %v", err)
	}

	cfg.SweeperInterval = 200 * time.Millisecond
	ts = startNFSQueryServerWithRootAndConfig(t, cfg)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}()

	waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
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

	recordCtx, recordCancel := context.WithTimeout(ctx, 10*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, ts.Backend(), rec.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestNFSQueryTxnRecoveryRollbackPendingNamespaces(t *testing.T) {
	root := prepareNFSQueryRoot(t)
	cfg := nfsQueryConfigWithSweeper(root, time.Hour)
	ts := startNFSQueryServerWithRootAndConfig(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	nsA := "alpha"
	nsB := "beta"
	keyA := fmt.Sprintf("nfs-recover-rb-ns-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("nfs-recover-rb-ns-b-%d", time.Now().UnixNano())

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

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer stopCancel()
	if err := ts.Stop(stopCtx); err != nil {
		t.Fatalf("stop initial server: %v", err)
	}

	cfg.SweeperInterval = 200 * time.Millisecond
	ts = startNFSQueryServerWithRootAndConfig(t, cfg)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}()

	waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
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

	recordCtx, recordCancel := context.WithTimeout(ctx, 10*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, ts.Backend(), rec.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestNFSQueryTxnDecisionEndpoints(t *testing.T) {
	root := prepareNFSQueryRoot(t)
	cfg := nfsQueryConfigWithSweeper(root, time.Hour)
	ts := startNFSQueryServerWithRootAndConfig(t, cfg)
	httpClient := newHTTPClient(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("nfs-rm-commit-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("nfs-rm-commit-b-%d", time.Now().UnixNano())

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

	keyC := fmt.Sprintf("nfs-rm-rb-c-%d", time.Now().UnixNano())
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

func TestNFSQueryTxnFanoutAcrossNodes(t *testing.T) {
	root := prepareNFSQueryRoot(t)
	cfg := nfsQueryConfigWithSweeper(root, 2*time.Second)
	cfg.DisableMTLS = true
	tsA := startNFSQueryServerWithRootAndConfig(t, cfg, lockd.WithoutTestMTLS())
	cfgB := cfg
	tsB := startNFSQueryServerWithRootAndConfig(t, cfgB, lockd.WithoutTestMTLS())

	t.Run("Commit", func(t *testing.T) {
		runNFSQueryTxnFanoutAcrossNodes(t, tsA, tsB, true)
	})
	t.Run("Rollback", func(t *testing.T) {
		runNFSQueryTxnFanoutAcrossNodes(t, tsA, tsB, false)
	})
}

func runNFSQueryTxnFanoutAcrossNodes(t *testing.T, tsA, tsB *lockd.TestServer, commit bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	activeServer, activeClient, err := hatest.FindActiveServer(ctx, tsA, tsB)
	if err != nil {
		t.Fatalf("find active server: %v", err)
	}
	passiveServer := tsB
	if activeServer == tsB {
		passiveServer = tsA
	}
	passiveClient := passiveServer.Client
	if passiveClient == nil {
		t.Fatalf("passive server missing client")
	}

	keyA := fmt.Sprintf("nfs-multinode-%t-a-%d", commit, time.Now().UnixNano())
	keyB := fmt.Sprintf("nfs-multinode-%t-b-%d", commit, time.Now().UnixNano())

	if _, err := passiveClient.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        "nfs-passive-probe-" + fmt.Sprint(time.Now().UnixNano()),
		Owner:      "passive-probe",
		TTLSeconds: 5,
		BlockSecs:  lockdclient.BlockNoWait,
	}); err == nil || !hatest.IsNodePassive(err) {
		t.Fatalf("expected ha passive on acquire, got %v", err)
	}

	leaseA, err := activeClient.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "multinode",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	leaseB, err := activeClient.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "multinode",
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

	decReq := api.TxnDecisionRequest{
		TxnID: leaseA.TxnID,
		State: "commit",
		Participants: []api.TxnParticipant{
			{Namespace: namespaces.Default, Key: keyA},
			{Namespace: namespaces.Default, Key: keyB},
		},
	}
	if !commit {
		decReq.State = "rollback"
	}
	if commit {
		if _, err := passiveClient.TxnCommit(ctx, decReq); err == nil || !hatest.IsNodePassive(err) {
			t.Fatalf("expected ha passive on commit, got %v", err)
		}
	} else {
		if _, err := passiveClient.TxnRollback(ctx, decReq); err == nil || !hatest.IsNodePassive(err) {
			t.Fatalf("expected ha passive on rollback, got %v", err)
		}
	}
	if commit {
		if _, err := activeClient.TxnCommit(ctx, decReq); err != nil {
			t.Fatalf("decision commit failed: %v", err)
		}
	} else {
		if _, err := activeClient.TxnRollback(ctx, decReq); err != nil {
			t.Fatalf("decision rollback failed: %v", err)
		}
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
			time.Sleep(75 * time.Millisecond)
		}
	}
	assertRolledBack := func(cli *lockdclient.Client, key string) {
		for {
			state, err := cli.Get(waitCtx, key, lockdclient.WithGetNamespace(namespaces.Default))
			if err == nil && !state.HasState {
				return
			}
			if waitCtx.Err() != nil {
				t.Fatalf("timeout waiting for rollback on %s: %v", key, err)
			}
			time.Sleep(75 * time.Millisecond)
		}
	}
	if commit {
		assertCommitted(activeClient, keyA, "a")
		assertCommitted(activeClient, keyB, "b")
	} else {
		assertRolledBack(activeClient, keyA)
		assertRolledBack(activeClient, keyB)
	}

	recordCtx, recordCancel := context.WithTimeout(ctx, 8*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, activeServer.Backend(), leaseA.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestNFSQueryRestartRollsBackStaged(t *testing.T) {
	root := prepareNFSQueryRoot(t)
	cfg := nfsQueryConfigWithSweeper(root, 2*time.Second)
	ts := startNFSQueryServerWithRootAndConfig(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	key := fmt.Sprintf("nfs-restart-staged-%d", time.Now().UnixNano())
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

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := ts.Stop(stopCtx); err != nil {
		t.Fatalf("stop server: %v", err)
	}

	time.Sleep(3 * time.Second) // allow TTL to expire

	cfg.SweeperInterval = 200 * time.Millisecond
	ts = startNFSQueryServerWithRootAndConfig(t, cfg)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}()

	waitCtx, waitCancel := context.WithTimeout(ctx, 20*time.Second)
	defer waitCancel()
	lease2, err := ts.Client.Acquire(waitCtx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        key,
		Owner:      "restart-test",
		TTLSeconds: 10,
		BlockSecs:  1,
	})
	if err != nil {
		t.Fatalf("re-acquire after restart: %v", err)
	}
	defer lease2.Release(waitCtx) //nolint:errcheck

	for {
		snap, gerr := lease2.Get(waitCtx)
		if gerr == nil && (snap == nil || !snap.HasState) {
			break
		}
		if waitCtx.Err() != nil {
			t.Fatalf("state still present after restart: %v", gerr)
		}
		time.Sleep(100 * time.Millisecond)
	}

	backend := ts.Backend()
	if backend == nil {
		t.Fatalf("backend unavailable after restart")
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(waitCtx, 10*time.Second)
	defer cleanupCancel()
	for {
		stagedCount, lerr := countStagingObjects(cleanupCtx, backend, namespaces.Default)
		if lerr == nil && stagedCount == 0 {
			break
		}
		if cleanupCtx.Err() != nil {
			t.Fatalf("staged objects not cleaned: %v", lerr)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

func countStagingObjects(ctx context.Context, backend storage.Backend, namespace string) (int, error) {
	count := 0
	startAfter := ""
	for {
		list, err := backend.ListObjects(ctx, namespace, storage.ListOptions{StartAfter: startAfter, Limit: 256})
		if err != nil {
			return 0, err
		}
		for _, obj := range list.Objects {
			if storage.IsStagingObjectKey(obj.Key) {
				count++
			}
		}
		if !list.Truncated || list.NextStartAfter == "" {
			return count, nil
		}
		startAfter = list.NextStartAfter
	}
}
func TestNFSQueryTxnReplayEndpointCommit(t *testing.T) {
	root := prepareNFSQueryRoot(t)
	cfg := nfsQueryConfigWithSweeper(root, time.Hour) // disable sweeper; explicit replay
	ts := startNFSQueryServerWithRootAndConfig(t, cfg)
	httpClient := newHTTPClient(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("nfs-replay-commit-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("nfs-replay-commit-b-%d", time.Now().UnixNano())

	leaseA, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "txn-replay",
		TTLSeconds: 20,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	leaseB, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "txn-replay",
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

	rec := core.TxnRecord{
		TxnID: leaseA.TxnID,
		State: core.TxnStateCommit,
		Participants: []core.TxnParticipant{
			{Namespace: namespaces.Default, Key: keyA},
			{Namespace: namespaces.Default, Key: keyB},
		},
		ExpiresAtUnix: time.Now().Add(45 * time.Second).Unix(),
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

	waitCtx, waitCancel := context.WithTimeout(ctx, 20*time.Second)
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

	recordCtx, recordCancel := context.WithTimeout(ctx, 8*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, ts.Backend(), rec.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func newHTTPClient(t testing.TB, ts *lockd.TestServer) *http.Client {
	t.Helper()
	return cryptotest.RequireTCHTTPClient(t, ts)
}

func startNFSQueryServer(t testing.TB) *lockd.TestServer {
	t.Helper()
	root := prepareNFSQueryRoot(t)
	cfg := nfsQueryConfigWithSweeper(root, 2*time.Second)
	return startNFSQueryServerWithRootAndConfig(t, cfg)
}

func prepareNFSQueryRoot(t testing.TB) string {
	t.Helper()
	base := strings.TrimSpace(os.Getenv("LOCKD_NFS_ROOT"))
	if base == "" {
		t.Fatalf("LOCKD_NFS_ROOT must be set (source .env.nfs before running nfs query tests)")
	}
	root := filepath.Join(base, fmt.Sprintf("lockd-query-%d", time.Now().UnixNano()))
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir nfs root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func diskStoreURL(root string) string {
	if root == "" {
		root = "/tmp/lockd-nfs"
	}
	if !strings.HasPrefix(root, "/") {
		root = "/" + root
	}
	return (&url.URL{Scheme: "disk", Path: root}).String()
}

func nfsQueryConfigWithSweeper(root string, interval time.Duration) lockd.Config {
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

func startNFSQueryServerWithRootAndConfig(t testing.TB, cfg lockd.Config, extra ...lockd.TestServerOption) *lockd.TestServer {
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
		lockd.WithTestCloseDefaults(
			lockd.WithDrainLeases(-1),
			lockd.WithShutdownTimeout(10*time.Second),
		),
	}
	opts = append(opts, extra...)
	opts = append(opts, cryptotest.SharedMTLSOptions(t)...)
	ts := lockd.StartTestServer(t, opts...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	})
	return ts
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
