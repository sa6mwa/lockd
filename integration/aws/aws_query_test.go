//go:build integration && aws && query

package awsintegration

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
	"pkt.systems/lockd/namespaces"
)

func TestAWSQuerySelectors(t *testing.T) {
	queriesuite.RunSelectors(t, startAWSQueryServer)
}

func TestAWSQueryFlushWaitReadabilityContract(t *testing.T) {
	queriesuite.RunFlushWaitReadabilityContract(t, startAWSQueryServer)
}

func TestAWSQueryRefreshWaitForContract(t *testing.T) {
	queriesuite.RunQueryRefreshWaitForContract(t, startAWSQueryServer)
}

func TestAWSQueryPagination(t *testing.T) {
	queriesuite.RunPagination(t, startAWSQueryServer)
}

func TestAWSQueryNamespaceIsolation(t *testing.T) {
	queriesuite.RunNamespaceIsolation(t, startAWSQueryServer)
}

func TestAWSQueryResultsSupportPublicRead(t *testing.T) {
	queriesuite.RunPublicRead(t, startAWSQueryServer)
}

func TestAWSQueryDocumentStreaming(t *testing.T) {
	queriesuite.RunDocumentStreaming(t, startAWSQueryServer)
}

func TestAWSQueryDocumentStreamingFlowControl(t *testing.T) {
	queriesuite.RunDocumentStreamingFlowControl(t, startAWSQueryServer)
}

func TestAWSQueryIndexRebuildUpgrade(t *testing.T) {
	queriesuite.RunIndexRebuildUpgrade(t, startAWSQueryServer)
}

func TestAWSQueryLargeNamespaceLowMatchKeys(t *testing.T) {
	queriesuite.RunLargeNamespaceLowMatchKeys(t, startAWSQueryServer)
}

func TestAWSQueryLargeNamespaceLowMatchDocuments(t *testing.T) {
	queriesuite.RunLargeNamespaceLowMatchDocuments(t, startAWSQueryServer)
}

func TestAWSQueryDomainDatasets(t *testing.T) {
	queriesuite.RunDomainDatasets(t, startAWSQueryServer, queriesuite.WithReducedDataset())
}

func TestAWSQueryTxSmoke(t *testing.T) {
	queriesuite.RunTxSmoke(t, startAWSQueryServer)
}

func TestAWSQueryTxnMultiKey(t *testing.T) {
	queriesuite.RunTxnMultiKey(t, startAWSQueryServer)
}

func TestAWSQueryTxnMultiKeyNamespaces(t *testing.T) {
	queriesuite.RunTxnMultiKeyNamespaces(t, startAWSQueryServer)
}

func TestAWSQueryTxnRecoveryCommit(t *testing.T) {
	cfg := awsQueryConfigWithSweeper(t, time.Hour)
	ts := startAWSQueryServerWithConfig(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("aws-recover-commit-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("aws-recover-commit-b-%d", time.Now().UnixNano())

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

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer stopCancel()
	if err := ts.Stop(stopCtx); err != nil {
		t.Fatalf("stop initial server: %v", err)
	}
	ts.Client = nil

	cfg.SweeperInterval = 200 * time.Millisecond
	ts = startAWSQueryServerWithConfigNoPreclean(t, cfg)

	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
	defer waitCancel()
	assertCommitted := func(key, expected string) {
		for {
			resp, err := ts.Client.Get(waitCtx, key)
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
			time.Sleep(200 * time.Millisecond)
		}
	}
	assertCommitted(keyA, "a")
	assertCommitted(keyB, "b")

	recordCtx, recordCancel := context.WithTimeout(ctx, 20*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, ts.Backend(), rec.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestAWSQueryTxnRecoveryRollbackPending(t *testing.T) {
	cfg := awsQueryConfigWithSweeper(t, time.Hour)
	ts := startAWSQueryServerWithConfig(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("aws-recover-rb-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("aws-recover-rb-b-%d", time.Now().UnixNano())

	leaseA, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "txn-recover",
		TTLSeconds: 15,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	leaseB, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "txn-recover",
		TTLSeconds: 15,
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

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer stopCancel()
	if err := ts.Stop(stopCtx); err != nil {
		t.Fatalf("stop initial server: %v", err)
	}
	ts.Client = nil

	cfg.SweeperInterval = 200 * time.Millisecond
	ts = startAWSQueryServerWithConfigNoPreclean(t, cfg)

	waitCtx, waitCancel := context.WithTimeout(ctx, 25*time.Second)
	defer waitCancel()
	assertRolledBack := func(key string) {
		for {
			resp, err := ts.Client.Get(waitCtx, key)
			if err == nil && !resp.HasState {
				return
			}
			if waitCtx.Err() != nil {
				t.Fatalf("timeout waiting for rollback on %s: %v", key, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	assertRolledBack(keyA)
	assertRolledBack(keyB)

	recordCtx, recordCancel := context.WithTimeout(ctx, 20*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, ts.Backend(), rec.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestAWSQueryTxnRecoveryRollbackPendingNamespaces(t *testing.T) {
	cfg := awsQueryConfigWithSweeper(t, time.Hour)
	ts := startAWSQueryServerWithConfig(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	nsA := "alpha"
	nsB := "beta"
	keyA := fmt.Sprintf("aws-recover-rb-ns-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("aws-recover-rb-ns-b-%d", time.Now().UnixNano())

	leaseA, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  nsA,
		Key:        keyA,
		Owner:      "txn-recover",
		TTLSeconds: 15,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	leaseB, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  nsB,
		Key:        keyB,
		Owner:      "txn-recover",
		TTLSeconds: 15,
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

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer stopCancel()
	if err := ts.Stop(stopCtx); err != nil {
		t.Fatalf("stop initial server: %v", err)
	}
	ts.Client = nil

	cfg.SweeperInterval = 200 * time.Millisecond
	ts = startAWSQueryServerWithConfigNoPreclean(t, cfg)

	waitCtx, waitCancel := context.WithTimeout(ctx, 25*time.Second)
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
			time.Sleep(200 * time.Millisecond)
		}
	}
	assertRolledBack(nsA, keyA)
	assertRolledBack(nsB, keyB)

	recordCtx, recordCancel := context.WithTimeout(ctx, 20*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, ts.Backend(), rec.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestAWSQueryTxnReplayEndpointCommit(t *testing.T) {
	cfg := awsQueryConfigWithSweeper(t, time.Hour) // disable sweeper; explicit replay
	ts := startAWSQueryServerWithConfigNoPreclean(t, cfg)
	httpClient := newHTTPClient(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("aws-replay-commit-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("aws-replay-commit-b-%d", time.Now().UnixNano())

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
		ExpiresAtUnix: time.Now().Add(60 * time.Second).Unix(),
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

	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
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
			time.Sleep(200 * time.Millisecond)
		}
	}
	assertCommitted(keyA, "a")
	assertCommitted(keyB, "b")

	recordCtx, recordCancel := context.WithTimeout(ctx, 15*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, ts.Backend(), rec.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestAWSQueryTxnDecisionEndpoints(t *testing.T) {
	cfg := awsQueryConfigWithSweeper(t, time.Hour)
	ts := startAWSQueryServerWithConfigNoPreclean(t, cfg)
	httpClient := newHTTPClient(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("aws-rm-commit-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("aws-rm-commit-b-%d", time.Now().UnixNano())

	leaseA, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "txn-decision",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	leaseB, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "txn-decision",
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
		ExpiresAtUnix: time.Now().Add(60 * time.Second).Unix(),
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

	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
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
			time.Sleep(200 * time.Millisecond)
		}
	}
	assertCommitted(keyA, "a")
	assertCommitted(keyB, "b")

	keyC := fmt.Sprintf("aws-rm-rb-c-%d", time.Now().UnixNano())
	leaseC, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyC,
		Owner:      "txn-decision",
		TTLSeconds: 30,
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
		ExpiresAtUnix: time.Now().Add(60 * time.Second).Unix(),
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

	waitRollbackCtx, cancelRB := context.WithTimeout(ctx, 20*time.Second)
	defer cancelRB()
	for {
		state, err := ts.Client.Get(waitRollbackCtx, keyC)
		if err == nil && !state.HasState {
			break
		}
		if waitRollbackCtx.Err() != nil {
			t.Fatalf("timeout waiting for rollback on %s: %v", keyC, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestAWSQueryTxnFanoutAcrossNodes(t *testing.T) {
	cfg := awsQueryConfigWithSweeper(t, 2*time.Second)
	cfg.HAMode = "concurrent"
	bundlePath := cryptotest.SharedTCClientBundlePath(t)
	if bundlePath == "" {
		cfg.DisableMTLS = true
	}
	tsA := startAWSQueryServerWithConfig(t, cfg)
	cfgB := cfg
	if bundlePath != "" {
		cfgB.TCClientBundlePath = bundlePath
	}
	tsB := startAWSQueryServerWithConfig(t, cfgB)

	cryptotest.RegisterRM(t, tsB, tsA)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	keyA := fmt.Sprintf("aws-multinode-commit-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("aws-multinode-commit-b-%d", time.Now().UnixNano())

	leaseA, err := tsA.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "multinode-commit",
		TTLSeconds: 60,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	leaseB, err := tsB.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "multinode-commit",
		TTLSeconds: 60,
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
		ExpiresAtUnix: time.Now().Add(2 * time.Minute).Unix(),
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

	waitCtx, waitCancel := context.WithTimeout(ctx, 90*time.Second)
	defer waitCancel()
	assertCommitted := func(cli *lockdclient.Client, key, expected string) {
		for {
			state, err := cli.Get(waitCtx, key, lockdclient.WithGetNamespace(namespaces.Default))
			if err == nil && state.HasState {
				raw, derr := state.Bytes()
				if derr != nil {
					t.Fatalf("read %s: %v", key, derr)
				}
				var doc map[string]any
				if err := json.Unmarshal(raw, &doc); err != nil {
					t.Fatalf("decode %s body=%s: %v", key, string(raw), err)
				}
				val, ok := doc["value"].(string)
				if !ok {
					t.Fatalf("unexpected value type for %s: %T (body=%s)", key, doc["value"], string(raw))
				}
				if val == expected {
					return
				}
				t.Fatalf("unexpected value for %s: got %q want %q (body=%s)", key, val, expected, string(raw))
			}
			if waitCtx.Err() != nil {
				t.Fatalf("timeout waiting for %s commit: %v", key, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	assertCommitted(tsA.Client, keyA, "a")
	assertCommitted(tsB.Client, keyB, "b")

	recordCtx, recordCancel := context.WithTimeout(ctx, 30*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, tsA.Backend(), leaseA.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestAWSQueryTxnFanoutRollbackAcrossNodes(t *testing.T) {
	cfg := awsQueryConfigWithSweeper(t, 2*time.Second)
	cfg.HAMode = "concurrent"
	bundlePath := cryptotest.SharedTCClientBundlePath(t)
	if bundlePath == "" {
		cfg.DisableMTLS = true
	}
	tsB := startAWSQueryServerWithConfig(t, cfg)
	cfgA := cfg
	if bundlePath != "" {
		cfgA.TCClientBundlePath = bundlePath
	}
	tsA := startAWSQueryServerWithConfig(t, cfgA)

	cryptotest.RegisterRM(t, tsA, tsB)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	keyA := fmt.Sprintf("aws-multinode-rollback-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("aws-multinode-rollback-b-%d", time.Now().UnixNano())

	leaseA, err := tsA.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "multinode-rollback",
		TTLSeconds: 60,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	leaseB, err := tsB.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "multinode-rollback",
		TTLSeconds: 60,
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

	httpClient := newHTTPClient(t, tsA)
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
	rbHTTP, err := http.NewRequest(http.MethodPost, tsA.URL()+"/v1/txn/decide", bytes.NewReader(rbBody))
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

	waitCtx, waitCancel := context.WithTimeout(ctx, 90*time.Second)
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
			time.Sleep(200 * time.Millisecond)
		}
	}
	assertRolledBack(tsA.Client, keyA)
	assertRolledBack(tsB.Client, keyB)

	recordCtx, recordCancel := context.WithTimeout(ctx, 30*time.Second)
	defer recordCancel()
	if err := waitForTxnRecordDecided(recordCtx, tsA.Backend(), leaseA.TxnID); err != nil {
		t.Fatalf("waiting for txn record cleanup: %v", err)
	}
}

func TestAWSQueryTxnReplayAfterRestartAcrossNodes(t *testing.T) {
	cfg := awsQueryConfigWithSweeper(t, time.Hour) // disable sweeper; explicit replay after restart
	cfg.HAMode = "concurrent"
	tsA := startAWSQueryServerWithConfig(t, cfg)
	tsB := startAWSQueryServerWithConfig(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	commitKeyA := fmt.Sprintf("aws-restart-commit-a-%d", time.Now().UnixNano())
	commitKeyB := fmt.Sprintf("aws-restart-commit-b-%d", time.Now().UnixNano())
	leaseA, err := tsA.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        commitKeyA,
		Owner:      "restart-commit",
		TTLSeconds: 60,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire commit A: %v", err)
	}
	leaseB, err := tsB.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        commitKeyB,
		Owner:      "restart-commit",
		TTLSeconds: 60,
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

	rollbackKeyA := fmt.Sprintf("aws-restart-rollback-a-%d", time.Now().UnixNano())
	rollbackKeyB := fmt.Sprintf("aws-restart-rollback-b-%d", time.Now().UnixNano())
	leaseC, err := tsA.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        rollbackKeyA,
		Owner:      "restart-rollback",
		TTLSeconds: 60,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire rollback A: %v", err)
	}
	leaseD, err := tsB.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        rollbackKeyB,
		Owner:      "restart-rollback",
		TTLSeconds: 60,
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
			ExpiresAtUnix: now.Add(3 * time.Minute).Unix(),
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

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopCancel()
	if err := tsA.Stop(stopCtx); err != nil {
		t.Fatalf("stop server A: %v", err)
	}
	if err := tsB.Stop(stopCtx); err != nil {
		t.Fatalf("stop server B: %v", err)
	}
	tsA.Client = nil
	tsB.Client = nil

	tsA = startAWSQueryServerWithConfigNoPreclean(t, cfg)
	tsB = startAWSQueryServerWithConfigNoPreclean(t, cfg)

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

	waitCommitCtx, waitCommitCancel := context.WithTimeout(ctx, 90*time.Second)
	defer waitCommitCancel()
	assertCommitted := func(cli *lockdclient.Client, key, expected string) {
		for {
			state, err := cli.Get(waitCommitCtx, key, lockdclient.WithGetNamespace(namespaces.Default))
			if err == nil && state.HasState {
				raw, derr := state.Bytes()
				if derr != nil {
					t.Fatalf("read %s: %v", key, derr)
				}
				var doc map[string]any
				if err := json.Unmarshal(raw, &doc); err != nil {
					t.Fatalf("decode %s body=%s: %v", key, string(raw), err)
				}
				val, ok := doc["value"].(string)
				if !ok {
					t.Fatalf("unexpected value type for %s: %T (body=%s)", key, doc["value"], string(raw))
				}
				if val == expected {
					return
				}
				t.Fatalf("unexpected value for %s: got %q want %q (body=%s)", key, val, expected, string(raw))
			}
			if waitCommitCtx.Err() != nil {
				t.Fatalf("timeout waiting for %s commit: %v", key, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	assertCommitted(tsA.Client, commitKeyA, "a")
	assertCommitted(tsB.Client, commitKeyB, "b")

	waitRollbackCtx, waitRollbackCancel := context.WithTimeout(ctx, 90*time.Second)
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
			time.Sleep(200 * time.Millisecond)
		}
	}
	assertRolledBack(tsA.Client, rollbackKeyA)
	assertRolledBack(tsB.Client, rollbackKeyB)

	recordCommitCtx, recordCommitCancel := context.WithTimeout(ctx, 30*time.Second)
	defer recordCommitCancel()
	if err := waitForTxnRecordDecided(recordCommitCtx, tsA.Backend(), leaseA.TxnID); err != nil {
		t.Fatalf("waiting for commit txn record cleanup: %v", err)
	}

	recordRollbackCtx, recordRollbackCancel := context.WithTimeout(ctx, 30*time.Second)
	defer recordRollbackCancel()
	if err := waitForTxnRecordDecided(recordRollbackCtx, tsA.Backend(), leaseC.TxnID); err != nil {
		t.Fatalf("waiting for rollback txn record cleanup: %v", err)
	}
}

func TestAWSQueryRestartRollsBackStaged(t *testing.T) {
	cfg := awsQueryConfigWithSweeper(t, 2*time.Second)
	ts := startAWSQueryServerWithConfig(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	key := fmt.Sprintf("aws-restart-staged-%d", time.Now().UnixNano())
	lease, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        key,
		Owner:      "restart-test",
		TTLSeconds: 4,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := lease.Save(ctx, map[string]any{"status": "staged-only"}); err != nil {
		t.Fatalf("save staged: %v", err)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer stopCancel()
	if err := ts.Stop(stopCtx); err != nil {
		t.Fatalf("stop server: %v", err)
	}
	ts.Client = nil

	time.Sleep(5 * time.Second) // allow TTL to expire

	cfg.SweeperInterval = 200 * time.Millisecond
	ts = startAWSQueryServerWithConfigNoPreclean(t, cfg)

	waitCtx, waitCancel := context.WithTimeout(ctx, 45*time.Second)
	defer waitCancel()
	lease2, err := ts.Client.Acquire(waitCtx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        key,
		Owner:      "restart-test",
		TTLSeconds: 20,
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
		time.Sleep(200 * time.Millisecond)
	}

	backend := ts.Backend()
	if backend == nil {
		t.Fatalf("backend unavailable after restart")
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(waitCtx, 25*time.Second)
	defer cleanupCancel()
	for {
		stagedCount, lerr := countStagingObjects(cleanupCtx, backend, namespaces.Default)
		if lerr == nil && stagedCount == 0 {
			break
		}
		if cleanupCtx.Err() != nil {
			t.Fatalf("staged objects not cleaned: %v", lerr)
		}
		time.Sleep(200 * time.Millisecond)
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

func newHTTPClient(t testing.TB, ts *lockd.TestServer) *http.Client {
	t.Helper()
	return cryptotest.RequireTCHTTPClient(t, ts)
}

func startAWSQueryServer(t testing.TB) *lockd.TestServer {
	t.Helper()
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)
	CleanupQueryNamespaces(t, cfg)
	ts := startAWSTestServer(t, cfg)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		querydata.FlushQueryNamespaces(ctx, t, ts.Client)
		_ = ts.Stop(ctx)
		CleanupQueryNamespaces(t, cfg)
	})
	return ts
}

func startAWSQueryServerWithConfig(t testing.TB, cfg lockd.Config, opts ...lockd.TestServerOption) *lockd.TestServer {
	t.Helper()
	ensureStoreReady(t, context.Background(), cfg)
	CleanupQueryNamespaces(t, cfg)
	ts := startAWSTestServer(t, cfg, opts...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		querydata.FlushQueryNamespaces(ctx, t, ts.Client)
		_ = ts.Stop(ctx)
		CleanupQueryNamespaces(t, cfg)
	})
	return ts
}

// startAWSQueryServerWithConfigNoPreclean starts a server without wiping namespaces first,
// so replay tests can see the staged state left behind by the prior process.
func startAWSQueryServerWithConfigNoPreclean(t testing.TB, cfg lockd.Config, opts ...lockd.TestServerOption) *lockd.TestServer {
	t.Helper()
	ts := startAWSTestServer(t, cfg, opts...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		querydata.FlushQueryNamespaces(ctx, t, ts.Client)
		_ = ts.Stop(ctx)
		CleanupQueryNamespaces(t, cfg)
	})
	return ts
}

func awsQueryConfigWithSweeper(t testing.TB, interval time.Duration) lockd.Config {
	cfg := loadAWSConfig(t)
	cfg.SweeperInterval = interval
	return cfg
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
		time.Sleep(200 * time.Millisecond)
	}
}
