package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/xid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	"pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

func TestCLIClientStateLifecycle(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	clk := clock.NewManual(time.Now().UTC())
	ts := startCLITestServerWithClock(t, clk)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := "cli-state-" + uuidv7.NewString()
	statePath := filepath.Join(t.TempDir(), "state.json")
	initial := []byte(`{"status":{"counter":0,"phase":"draft"}}`)
	if err := os.WriteFile(statePath, initial, 0o600); err != nil {
		t.Fatalf("write seed file: %v", err)
	}

	runCLICommand(t,
		"client",
		"edit",
		"--file", statePath,
		`/status/phase="staged"`,
		"/status/counter=+2",
	)

	var edited map[string]any
	data, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read edited file: %v", err)
	}
	if err := json.Unmarshal(data, &edited); err != nil {
		t.Fatalf("unmarshal edited state: %v", err)
	}
	status := edited["status"].(map[string]any)
	if status["phase"] != "staged" || status["counter"].(float64) != 2 {
		t.Fatalf("unexpected local edit: %#v", status)
	}

	acquireOut, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"acquire",
		"--key", key,
		"--owner", "cli-tester",
		"--ttl", "45s",
		"--namespace", namespaces.Default,
		"--output", "json",
	)
	var acquireResp api.AcquireResponse
	if err := json.Unmarshal([]byte(acquireOut), &acquireResp); err != nil {
		t.Fatalf("decode acquire response: %v", err)
	}
	t.Setenv(envTxnID, acquireResp.TxnID)

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"update",
		"--key", key,
		"--namespace", namespaces.Default,
		"--lease", acquireResp.LeaseID,
		"--fencing-token", strconv.FormatInt(acquireResp.FencingToken, 10),
		statePath,
	)

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"keepalive",
		"--key", key,
		"--namespace", namespaces.Default,
		"--lease", acquireResp.LeaseID,
		"--fencing-token", strconv.FormatInt(acquireResp.FencingToken, 10),
		"--ttl", "60s",
	)

	t.Setenv(envLeaseID, acquireResp.LeaseID)
	t.Setenv(envFencingToken, strconv.FormatInt(acquireResp.FencingToken, 10))
	t.Setenv(envKey, key)

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"set",
		"--key", key,
		"--namespace", namespaces.Default,
		"/status/counter++",
		`time:/status/updated=NOW`,
	)

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"release",
		"--key", key,
		"--namespace", namespaces.Default,
		"--lease", acquireResp.LeaseID,
		"--fencing-token", strconv.FormatInt(acquireResp.FencingToken, 10),
	)

	verifyLease, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        key,
		Owner:      "cli-verifier",
		TTLSeconds: 30,
		BlockSecs:  client.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("verify acquire: %v", err)
	}
	defer verifyLease.Release(ctx)
	var serverState map[string]any
	if err := verifyLease.Load(ctx, &serverState); err != nil {
		t.Fatalf("load final state: %v", err)
	}
	srvStatus := serverState["status"].(map[string]any)
	if srvStatus["phase"] != "staged" {
		t.Fatalf("expected phase staged, got %#v", srvStatus["phase"])
	}
	if count := srvStatus["counter"].(float64); count != 3 {
		t.Fatalf("expected counter 3, got %v", count)
	}
	if _, ok := srvStatus["updated"]; !ok {
		t.Fatalf("expected updated timestamp in %+v", srvStatus)
	}
}

func TestCLIClientQueueCommands(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	clk := clock.NewManual(time.Now().UTC())
	ts := startCLITestServerWithClock(t, clk)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")
	queueName := "cli-queue-" + uuidv7.NewString()
	payload := `{"op":"test","value":42}`

	enqueueOut, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"queue", "enqueue",
		"--queue", queueName,
		"--namespace", namespaces.Default,
		"--data", payload,
		"--content-type", "application/json",
		"--output", "json",
	)
	var enqueueResp api.EnqueueResponse
	if err := json.Unmarshal([]byte(enqueueOut), &enqueueResp); err != nil {
		t.Fatalf("decode enqueue response: %v", err)
	}
	if enqueueResp.Queue != queueName {
		t.Fatalf("unexpected queue name %s", enqueueResp.Queue)
	}

	payloadPath := filepath.Join(t.TempDir(), "payload.json")
	dequeueOut, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"queue", "dequeue",
		"--queue", queueName,
		"--namespace", namespaces.Default,
		"--owner", "cli-worker",
		"--block", "nowait",
		"--payload-out", payloadPath,
		"--output", "json",
	)
	var dq struct {
		Namespace    string `json:"namespace"`
		Queue        string `json:"queue"`
		MessageID    string `json:"message_id"`
		LeaseID      string `json:"lease_id"`
		MetaETag     string `json:"meta_etag"`
		FencingToken int64  `json:"fencing_token"`
		PayloadPath  string `json:"payload_path"`
		PayloadBytes int64  `json:"payload_bytes"`
	}
	if err := json.Unmarshal([]byte(dequeueOut), &dq); err != nil {
		t.Fatalf("decode dequeue summary: %v", err)
	}
	if dq.MessageID != enqueueResp.MessageID {
		t.Fatalf("message mismatch dequeue=%s enqueue=%s", dq.MessageID, enqueueResp.MessageID)
	}
	payloadData, err := os.ReadFile(payloadPath)
	if err != nil {
		t.Fatalf("read payload file: %v", err)
	}
	if strings.TrimSpace(string(payloadData)) != payload {
		t.Fatalf("unexpected payload %q", payloadData)
	}

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"queue", "ack",
		"--queue", queueName,
		"--namespace", namespaces.Default,
		"--message", dq.MessageID,
		"--lease", dq.LeaseID,
		"--meta-etag", dq.MetaETag,
		"--fencing-token", strconv.FormatInt(dq.FencingToken, 10),
	)

	_, stderr := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"queue", "dequeue",
		"--queue", queueName,
		"--namespace", namespaces.Default,
		"--owner", "cli-worker",
		"--block", "nowait",
		"--output", "json",
	)
	if !strings.Contains(stderr, "no message available") && !strings.Contains(stderr, "no messages available") {
		t.Fatalf("expected empty queue notice, stderr=%q", stderr)
	}
}

func TestCLIClientServerFlagOverridesEnv(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	t.Setenv("LOCKD_CLIENT_SERVER", "http://[::1")
	err := runCLICommandExpectError(t,
		"client",
		"--server", "http://[::2",
		"query", "",
	)
	if !strings.Contains(err.Error(), `parse endpoint "http://[::2"`) {
		t.Fatalf("expected flag endpoint parse error, got %v", err)
	}
	if strings.Contains(err.Error(), "http://[::1") {
		t.Fatalf("expected --server to override env endpoint, got %v", err)
	}
}

func TestCLIClientServerShorthandOverridesEnv(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	t.Setenv("LOCKD_CLIENT_SERVER", "http://[::1")
	err := runCLICommandExpectError(t,
		"client",
		"-s", "http://[::2",
		"query", "",
	)
	if !strings.Contains(err.Error(), `parse endpoint "http://[::2"`) {
		t.Fatalf("expected shorthand endpoint parse error, got %v", err)
	}
	if strings.Contains(err.Error(), "http://[::1") {
		t.Fatalf("expected -s to override env endpoint, got %v", err)
	}
}

func TestCLIGlobalClientFlagsBeforeSubcommand(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	err := runCLICommandExpectError(t,
		"-b", filepath.Join(t.TempDir(), "client.pem"),
		"-s", "http://[::2",
		"client", "query", "",
	)
	if !strings.Contains(err.Error(), `parse endpoint "http://[::2"`) {
		t.Fatalf("expected global -s endpoint parse error, got %v", err)
	}
	if strings.Contains(err.Error(), "unknown shorthand flag: 'b' in -b") {
		t.Fatalf("expected global -b to parse before subcommand, got %v", err)
	}
}

func TestCLIGlobalClientFlagsApplyToNamespace(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	err := runCLICommandExpectError(t,
		"-s", "http://[::2",
		"namespace", "get",
	)
	if !strings.Contains(err.Error(), `parse endpoint "http://[::2"`) {
		t.Fatalf("expected namespace command to use global --server/-s, got %v", err)
	}
}

func TestCLIClientQueueNackRejectsReasonForDefer(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	clk := clock.NewManual(time.Now().UTC())
	ts := startCLITestServerWithClock(t, clk)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")
	queueName := "cli-queue-defer-reason-" + uuidv7.NewString()
	payload := `{"op":"test","value":99}`

	_, _ = runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"queue", "enqueue",
		"--queue", queueName,
		"--namespace", namespaces.Default,
		"--data", payload,
		"--content-type", "application/json",
	)

	dequeueOut, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"queue", "dequeue",
		"--queue", queueName,
		"--namespace", namespaces.Default,
		"--owner", "cli-worker",
		"--block", "nowait",
		"--output", "json",
	)
	var dq struct {
		MessageID    string `json:"message_id"`
		LeaseID      string `json:"lease_id"`
		MetaETag     string `json:"meta_etag"`
		FencingToken int64  `json:"fencing_token"`
	}
	if err := json.Unmarshal([]byte(dequeueOut), &dq); err != nil {
		t.Fatalf("decode dequeue summary: %v", err)
	}

	err := runCLICommandExpectError(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"queue", "nack",
		"--queue", queueName,
		"--namespace", namespaces.Default,
		"--message", dq.MessageID,
		"--lease", dq.LeaseID,
		"--meta-etag", dq.MetaETag,
		"--fencing-token", strconv.FormatInt(dq.FencingToken, 10),
		"--intent", "defer",
		"--reason", "waiting on dependency",
	)
	if err == nil || !strings.Contains(err.Error(), "--reason is only supported when --intent=failure") {
		t.Fatalf("expected defer reason validation error, got %v", err)
	}
}

func TestCLIClientAttachmentsLifecycle(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	ts := startCLITestServer(t)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")

	key := "cli-attach-" + uuidv7.NewString()
	acquireOut, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"acquire",
		"--key", key,
		"--owner", "cli-attach",
		"--ttl", "30s",
		"--output", "json",
	)
	var acquireResp api.AcquireResponse
	if err := json.Unmarshal([]byte(acquireOut), &acquireResp); err != nil {
		t.Fatalf("decode acquire: %v", err)
	}
	fencing := strconv.FormatInt(acquireResp.FencingToken, 10)

	dir := t.TempDir()
	alphaPath := filepath.Join(dir, "alpha.txt")
	bravoPath := filepath.Join(dir, "bravo.txt")
	alphaPayload := []byte("alpha attachment")
	bravoPayload := []byte("bravo attachment")
	if err := os.WriteFile(alphaPath, alphaPayload, 0o600); err != nil {
		t.Fatalf("write alpha: %v", err)
	}
	if err := os.WriteFile(bravoPath, bravoPayload, 0o600); err != nil {
		t.Fatalf("write bravo: %v", err)
	}

	listOut, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"attachments", "list",
		"--key", key,
		"--lease", acquireResp.LeaseID,
		"--txn-id", acquireResp.TxnID,
		"--fencing-token", fencing,
		"--output", "json",
	)
	var list client.AttachmentList
	if err := json.Unmarshal([]byte(strings.TrimSpace(listOut)), &list); err != nil {
		t.Fatalf("decode empty list: %v", err)
	}
	if len(list.Attachments) != 0 {
		t.Fatalf("expected no attachments, got %+v", list.Attachments)
	}

	attachOut, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"attachments", "put",
		"--key", key,
		"--lease", acquireResp.LeaseID,
		"--txn-id", acquireResp.TxnID,
		"--fencing-token", fencing,
		"--name", "alpha.txt",
		"--file", alphaPath,
		"--content-type", "text/plain",
	)
	var attachRes client.AttachResult
	if err := json.Unmarshal([]byte(strings.TrimSpace(attachOut)), &attachRes); err != nil {
		t.Fatalf("decode attach: %v", err)
	}
	if attachRes.Attachment.Name != "alpha.txt" || attachRes.Attachment.ID == "" {
		t.Fatalf("unexpected attach metadata: %+v", attachRes.Attachment)
	}

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"attachments", "put",
		"--key", key,
		"--lease", acquireResp.LeaseID,
		"--txn-id", acquireResp.TxnID,
		"--fencing-token", fencing,
		"--name", "bravo.txt",
		"--file", bravoPath,
		"--content-type", "text/plain",
	)

	listOut2, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"attachments", "list",
		"--key", key,
		"--lease", acquireResp.LeaseID,
		"--txn-id", acquireResp.TxnID,
		"--fencing-token", fencing,
		"--output", "json",
	)
	if err := json.Unmarshal([]byte(strings.TrimSpace(listOut2)), &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(list.Attachments) != 2 {
		t.Fatalf("expected 2 attachments, got %+v", list.Attachments)
	}
	names := map[string]bool{}
	for _, att := range list.Attachments {
		names[att.Name] = true
	}
	if !names["alpha.txt"] || !names["bravo.txt"] {
		t.Fatalf("unexpected attachment names: %+v", names)
	}

	alphaOut := filepath.Join(dir, "alpha.out")
	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"attachments", "get",
		"--key", key,
		"--lease", acquireResp.LeaseID,
		"--txn-id", acquireResp.TxnID,
		"--fencing-token", fencing,
		"--name", "alpha.txt",
		"--output", alphaOut,
	)
	alphaRead, err := os.ReadFile(alphaOut)
	if err != nil {
		t.Fatalf("read alpha out: %v", err)
	}
	if string(alphaRead) != string(alphaPayload) {
		t.Fatalf("unexpected alpha payload: %q", alphaRead)
	}

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"release",
		"--key", key,
		"--lease", acquireResp.LeaseID,
		"--txn-id", acquireResp.TxnID,
		"--fencing-token", fencing,
	)

	publicListOut, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"attachments", "list",
		"--key", key,
		"--public",
		"--output", "json",
	)
	if err := json.Unmarshal([]byte(strings.TrimSpace(publicListOut)), &list); err != nil {
		t.Fatalf("decode public list: %v", err)
	}
	if len(list.Attachments) != 2 {
		t.Fatalf("expected 2 public attachments, got %+v", list.Attachments)
	}

	bravoOut := filepath.Join(dir, "bravo.out")
	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"attachments", "get",
		"--key", key,
		"--public",
		"--name", "bravo.txt",
		"--output", bravoOut,
	)
	bravoRead, err := os.ReadFile(bravoOut)
	if err != nil {
		t.Fatalf("read bravo out: %v", err)
	}
	if string(bravoRead) != string(bravoPayload) {
		t.Fatalf("unexpected bravo payload: %q", bravoRead)
	}

	acquireOut2, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"acquire",
		"--key", key,
		"--owner", "cli-attach-delete",
		"--ttl", "30s",
		"--output", "json",
	)
	var acquireResp2 api.AcquireResponse
	if err := json.Unmarshal([]byte(acquireOut2), &acquireResp2); err != nil {
		t.Fatalf("decode acquire 2: %v", err)
	}
	fencing2 := strconv.FormatInt(acquireResp2.FencingToken, 10)

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"attachments", "delete",
		"--key", key,
		"--lease", acquireResp2.LeaseID,
		"--txn-id", acquireResp2.TxnID,
		"--fencing-token", fencing2,
		"--name", "alpha.txt",
	)

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"release",
		"--key", key,
		"--lease", acquireResp2.LeaseID,
		"--txn-id", acquireResp2.TxnID,
		"--fencing-token", fencing2,
	)

	publicListOut2, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"attachments", "list",
		"--key", key,
		"--public",
		"--output", "json",
	)
	if err := json.Unmarshal([]byte(strings.TrimSpace(publicListOut2)), &list); err != nil {
		t.Fatalf("decode public list 2: %v", err)
	}
	if len(list.Attachments) != 1 || list.Attachments[0].Name != "bravo.txt" {
		t.Fatalf("expected bravo only, got %+v", list.Attachments)
	}

	acquireOut3, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"acquire",
		"--key", key,
		"--owner", "cli-attach-delete-all",
		"--ttl", "30s",
		"--output", "json",
	)
	var acquireResp3 api.AcquireResponse
	if err := json.Unmarshal([]byte(acquireOut3), &acquireResp3); err != nil {
		t.Fatalf("decode acquire 3: %v", err)
	}
	fencing3 := strconv.FormatInt(acquireResp3.FencingToken, 10)

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"attachments", "delete-all",
		"--key", key,
		"--lease", acquireResp3.LeaseID,
		"--txn-id", acquireResp3.TxnID,
		"--fencing-token", fencing3,
	)

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"release",
		"--key", key,
		"--lease", acquireResp3.LeaseID,
		"--txn-id", acquireResp3.TxnID,
		"--fencing-token", fencing3,
	)

	publicListOut3, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"attachments", "list",
		"--key", key,
		"--public",
		"--output", "json",
	)
	if err := json.Unmarshal([]byte(strings.TrimSpace(publicListOut3)), &list); err != nil {
		t.Fatalf("decode public list 3: %v", err)
	}
	if len(list.Attachments) != 0 {
		t.Fatalf("expected no attachments, got %+v", list.Attachments)
	}
}

func TestCLIClientAttachmentsValidation(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	ts := startCLITestServer(t)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")

	err := runCLICommandExpectError(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"attachments", "get",
		"--key", "missing-name",
		"--public",
	)
	if err == nil || !strings.Contains(err.Error(), "--name or --id required") {
		t.Fatalf("expected missing name error, got %v", err)
	}

	err = runCLICommandExpectError(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"attachments", "put",
		"--key", "missing-file",
		"--name", "alpha.txt",
	)
	if err == nil || !strings.Contains(err.Error(), "--file required") {
		t.Fatalf("expected missing file error, got %v", err)
	}

	err = runCLICommandExpectError(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"attachments", "put",
		"--key", "missing-name",
		"--file", "-",
	)
	if err == nil || !strings.Contains(err.Error(), "--name required") {
		t.Fatalf("expected missing name error, got %v", err)
	}
}

func TestCLIClientTxCommands(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	clk := clock.NewManual(time.Now().UTC())
	ts := startCLITestServerWithClock(t, clk)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")

	now := time.Now()

	// prepare + commit flow
	txn1 := xid.New().String()
	outPrepare, _ := runCLICommandOutput(t,
		"txn", "prepare",
		"--server", serverURL, "--disable-mtls",
		"--txn-id", txn1,
		"--participant", "orders",
		"--expires-unix", strconv.FormatInt(now.Add(30*time.Second).Unix(), 10),
	)
	if !strings.Contains(outPrepare, "state=pending") {
		t.Fatalf("expected pending prepare, got %q", outPrepare)
	}

	outCommit, _ := runCLICommandOutput(t,
		"txn", "commit",
		"--server", serverURL, "--disable-mtls",
		"--txn-id", txn1,
		"--participant", "orders",
	)
	if !strings.Contains(outCommit, "state=commit") {
		t.Fatalf("expected commit state, got %q", outCommit)
	}

	// expired pending replay should rollback
	txn2 := xid.New().String()
	outPrepare2, _ := runCLICommandOutput(t,
		"txn", "prepare",
		"--server", serverURL, "--disable-mtls",
		"--txn-id", txn2,
		"--participant", "orders",
		"--expires-unix", strconv.FormatInt(now.Add(-5*time.Second).Unix(), 10),
	)
	if !strings.Contains(outPrepare2, "state=pending") {
		t.Fatalf("expected pending prepare (expired), got %q", outPrepare2)
	}

	outReplay, _ := runCLICommandOutput(t,
		"txn", "replay",
		"--server", serverURL, "--disable-mtls",
		"--txn-id", txn2,
	)
	if !strings.Contains(outReplay, "state=rollback") {
		t.Fatalf("expected rollback replay, got %q", outReplay)
	}

	// direct rollback
	txn3 := xid.New().String()
	outRollback, _ := runCLICommandOutput(t,
		"txn", "rollback",
		"--server", serverURL, "--disable-mtls",
		"--txn-id", txn3,
		"--participant", "orders",
	)
	if !strings.Contains(outRollback, "state=rollback") {
		t.Fatalf("expected rollback state, got %q", outRollback)
	}
}

func TestCLIClientTxMultiKeyCommitApplies(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	clk := clock.NewManual(time.Now().UTC())
	ts := startCLITestServerWithClock(t, clk)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")

	txn := xid.New().String()
	key1 := "tx-multi-1"
	key2 := "tx-multi-2"

	stageKeyWithTxn(t, ts, key1, txn, map[string]any{"counter": 1})
	stageKeyWithTxn(t, ts, key2, txn, map[string]any{"counter": 2})

	out, _ := runCLICommandOutput(t,
		"txn", "commit",
		"--server", serverURL, "--disable-mtls",
		"--txn-id", txn,
		"--participant", key1,
		"--participant", key2,
	)
	if !strings.Contains(out, "state=commit") {
		t.Fatalf("expected commit state, got %q", out)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state1 := loadKeyState(ctx, t, ts, key1)
	state2 := loadKeyState(ctx, t, ts, key2)
	if state1["counter"] != float64(1) || state2["counter"] != float64(2) {
		t.Fatalf("unexpected states: k1=%v k2=%v", state1, state2)
	}
}

func TestCLIClientTxReplayRollsBackExpiredPending(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	clk := clock.NewManual(time.Now().UTC())
	ts := startCLITestServerWithClock(t, clk)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")

	txn := xid.New().String()
	key := "tx-replay-1"

	// Use a short TTL so the txn record expires quickly.
	stageKeyWithTxnTTL(t, ts, key, txn, map[string]any{"counter": 9}, 1)

	outPrepare, _ := runCLICommandOutput(t,
		"txn", "prepare",
		"--server", serverURL, "--disable-mtls",
		"--txn-id", txn,
		"--participant", key,
	)
	if !strings.Contains(outPrepare, "state=pending") {
		t.Fatalf("expected pending prepare, got %q", outPrepare)
	}

	// Advance the manual clock past the txn TTL so replay rolls it back.
	clk.Advance(2 * time.Second)

	outReplay, _ := runCLICommandOutput(t,
		"txn", "replay",
		"--server", serverURL, "--disable-mtls",
		"--txn-id", txn,
	)
	if !strings.Contains(outReplay, "state=rollback") {
		t.Fatalf("expected rollback from replay, got %q", outReplay)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state := loadKeyState(ctx, t, ts, key)
	if len(state) != 0 {
		t.Fatalf("expected empty state after rollback, got %v", state)
	}
}

func TestCLIClientAcquireJoinsTxnAndCommitApplies(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	clk := clock.NewManual(time.Now().UTC())
	ts := startCLITestServerWithClock(t, clk)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")

	txn := xid.New().String()
	key := "cli-acquire-join-" + uuidv7.NewString()

	acquireOut, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"acquire",
		"--key", key,
		"--owner", "cli-join",
		"--ttl", "20s",
		"--txn-id", txn,
		"--output", "json",
	)
	var acquireResp api.AcquireResponse
	if err := json.Unmarshal([]byte(acquireOut), &acquireResp); err != nil {
		t.Fatalf("decode acquire: %v", err)
	}

	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(statePath, []byte(`{"stage":"pending"}`), 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"update",
		"--key", key,
		"--lease", acquireResp.LeaseID,
		"--fencing-token", strconv.FormatInt(acquireResp.FencingToken, 10),
		"--txn-id", txn,
		statePath,
	)

	outCommit, _ := runCLICommandOutput(t,
		"txn", "commit",
		"--server", serverURL, "--disable-mtls",
		"--txn-id", txn,
		"--participant", key,
	)
	if !strings.Contains(outCommit, "state=commit") {
		t.Fatalf("expected commit state, got %q", outCommit)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state := loadKeyState(ctx, t, ts, key)
	if got := state["stage"]; got != "pending" {
		t.Fatalf("expected committed stage, got %v", got)
	}
}

func TestCLIClientAcquireReleaseRollbackClearsState(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	clk := clock.NewManual(time.Now().UTC())
	ts := startCLITestServerWithClock(t, clk)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")

	key := "cli-acquire-rollback-" + uuidv7.NewString()
	acquireOut, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"acquire",
		"--key", key,
		"--owner", "cli-rollback",
		"--ttl", "20s",
		"--output", "json",
	)
	var acquireResp api.AcquireResponse
	if err := json.Unmarshal([]byte(acquireOut), &acquireResp); err != nil {
		t.Fatalf("decode acquire: %v", err)
	}

	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(statePath, []byte(`{"value":123}`), 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"update",
		"--key", key,
		"--lease", acquireResp.LeaseID,
		"--fencing-token", strconv.FormatInt(acquireResp.FencingToken, 10),
		"--txn-id", acquireResp.TxnID,
		statePath,
	)

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"release",
		"--key", key,
		"--lease", acquireResp.LeaseID,
		"--fencing-token", strconv.FormatInt(acquireResp.FencingToken, 10),
		"--txn-id", acquireResp.TxnID,
		"--rollback",
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state := loadKeyState(ctx, t, ts, key)
	if len(state) != 0 {
		t.Fatalf("expected empty state after rollback release, got %v", state)
	}
}

func TestCLIClientAcquireGeneratesKeyAndCommits(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	ts := startCLITestServer(t)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")

	acquireOut, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"acquire",
		"--owner", "cli-generated",
		"--ttl", "15s",
		"--output", "json",
	)
	var acquireResp api.AcquireResponse
	if err := json.Unmarshal([]byte(acquireOut), &acquireResp); err != nil {
		t.Fatalf("decode acquire: %v", err)
	}
	if acquireResp.Key == "" {
		t.Fatalf("expected generated key, got empty")
	}

	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(statePath, []byte(`{"hello":"world"}`), 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"update",
		"--key", acquireResp.Key,
		"--lease", acquireResp.LeaseID,
		"--fencing-token", strconv.FormatInt(acquireResp.FencingToken, 10),
		"--txn-id", acquireResp.TxnID,
		statePath,
	)

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"release",
		"--key", acquireResp.Key,
		"--lease", acquireResp.LeaseID,
		"--fencing-token", strconv.FormatInt(acquireResp.FencingToken, 10),
		"--txn-id", acquireResp.TxnID,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state := loadKeyState(ctx, t, ts, acquireResp.Key)
	if state["hello"] != "world" {
		t.Fatalf("expected committed state, got %v", state)
	}
}

func TestCLIClientAcquireNowaitFailsWhenHeld(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	ts := startCLITestServer(t)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")

	key := "cli-nowait-" + uuidv7.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	holder, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        key,
		Owner:      "holder",
		TTLSeconds: 30,
		BlockSecs:  client.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	defer holder.Release(ctx)

	err = runCLICommandExpectError(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"acquire",
		"--key", key,
		"--owner", "contender",
		"--ttl", "5s",
		"--block", "nowait",
	)
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) || apiErr.Response.ErrorCode != "waiting" {
		t.Fatalf("expected waiting API error, got %v", err)
	}
}

func TestCLIClientAcquireIfNotExistsFailsWhenKeyExists(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	ts := startCLITestServer(t)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")

	key := "cli-if-not-exists-" + uuidv7.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	holder, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        key,
		Owner:      "holder",
		TTLSeconds: 30,
		BlockSecs:  client.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	if err := holder.Release(ctx); err != nil {
		t.Fatalf("seed release: %v", err)
	}

	err = runCLICommandExpectError(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"acquire",
		"--key", key,
		"--owner", "contender",
		"--ttl", "5s",
		"--if-not-exists",
	)
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) || apiErr.Response.ErrorCode != "already_exists" {
		t.Fatalf("expected already_exists API error, got %v", err)
	}
}

func TestCLIClientAcquireWithNamespacePersists(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	ts := startCLITestServer(t)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")

	ns := "sales"
	key := "cli-ns-" + uuidv7.NewString()

	acquireOut, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"acquire",
		"--key", key,
		"--owner", "ns-owner",
		"--ttl", "15s",
		"--namespace", ns,
		"--output", "json",
	)
	var acquireResp api.AcquireResponse
	if err := json.Unmarshal([]byte(acquireOut), &acquireResp); err != nil {
		t.Fatalf("decode acquire: %v", err)
	}

	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(statePath, []byte(`{"ns":"ok"}`), 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"update",
		"--key", key,
		"--lease", acquireResp.LeaseID,
		"--fencing-token", strconv.FormatInt(acquireResp.FencingToken, 10),
		"--txn-id", acquireResp.TxnID,
		"--namespace", ns,
		statePath,
	)

	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"release",
		"--key", key,
		"--lease", acquireResp.LeaseID,
		"--fencing-token", strconv.FormatInt(acquireResp.FencingToken, 10),
		"--txn-id", acquireResp.TxnID,
		"--namespace", ns,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state := loadKeyStateNamespace(ctx, t, ts, ns, key)
	if state["ns"] != "ok" {
		t.Fatalf("expected state in namespace, got %v", state)
	}
}

func TestCLIClientKeepAliveRequiresTxnWhenEnlisted(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	ts := startCLITestServer(t)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")

	key := "cli-keepalive-" + uuidv7.NewString()
	acquireOut, _ := runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"acquire",
		"--key", key,
		"--owner", "keepalive",
		"--ttl", "5s",
		"--output", "json",
	)
	var acquireResp api.AcquireResponse
	if err := json.Unmarshal([]byte(acquireOut), &acquireResp); err != nil {
		t.Fatalf("decode acquire: %v", err)
	}

	err := runCLICommandExpectError(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"keepalive",
		"--key", key,
		"--lease", acquireResp.LeaseID,
		"--fencing-token", strconv.FormatInt(acquireResp.FencingToken, 10),
		"--ttl", "5s",
	)
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) || apiErr.Response.ErrorCode != "missing_txn" {
		t.Fatalf("expected missing_txn error, got %v", err)
	}

	_, _ = runCLICommandOutput(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"keepalive",
		"--key", key,
		"--lease", acquireResp.LeaseID,
		"--ttl", "5s",
		"--fencing-token", strconv.FormatInt(acquireResp.FencingToken, 10),
		"--txn-id", acquireResp.TxnID,
		"--output", "json",
	)
}

func TestCLIClientAcquireBlockSucceedsAfterRelease(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	clk := clock.NewManual(time.Now().UTC())
	ts := startCLITestServerWithClock(t, clk)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")

	key := "cli-block-" + uuidv7.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	holder, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        key,
		Owner:      "holder",
		TTLSeconds: 1,
		BlockSecs:  client.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}

	// Kick off blocking acquire that should wait for holder expiration.
	acquireDone := make(chan struct{})
	go func() {
		defer close(acquireDone)
		runCLICommand(t,
			"client",
			"--server", serverURL,
			"--disable-mtls",
			"acquire",
			"--key", key,
			"--owner", "blocker",
			"--ttl", "5s",
			"--block", "2s",
		)
	}()

	clk.Advance(2 * time.Second)
	runtime.Gosched()
	_ = holder.Release(ctx)
	<-acquireDone
}

func TestCLIClientAcquireRetriesFlag(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	ts := startCLITestServer(t)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")

	key := "cli-retries-" + uuidv7.NewString()
	// Just ensure the flag is accepted; failure is unlikely in-memory, so we only assert success.
	runCLICommand(t,
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"acquire",
		"--key", key,
		"--owner", "retry",
		"--ttl", "5s",
		"--retries", "2",
	)
}

type queryDoc struct {
	key    string
	status string
	region string
	urgent bool
}

func TestCLIClientQueryCommand(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	ts := startCLITestServer(t)
	serverURL := ts.URL()
	t.Setenv("LOCKD_CLIENT_SERVER", serverURL)
	t.Setenv("LOCKD_CLIENT_DISABLE_MTLS", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	docs := []queryDoc{
		{status: "staged", region: "emea", urgent: false},
		{status: "draft", region: "amer", urgent: true},
		{status: "staged", region: "emea", urgent: true},
	}
	for i := range docs {
		docs[i].key = "cli-query-" + uuidv7.NewString()
		body := map[string]any{
			"report": map[string]any{
				"key":    docs[i].key,
				"status": docs[i].status,
				"region": docs[i].region,
				"urgent": docs[i].urgent,
			},
		}
		lease, err := ts.Client.Acquire(ctx, api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        docs[i].key,
			Owner:      "cli-query",
			TTLSeconds: 60,
			BlockSecs:  client.BlockNoWait,
		})
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		if err := lease.Save(ctx, body); err != nil {
			lease.Release(ctx)
			t.Fatalf("save %d: %v", i, err)
		}
		if err := lease.Release(ctx); err != nil {
			t.Fatalf("release %d: %v", i, err)
		}
	}
	if _, err := ts.Client.FlushIndex(ctx, namespaces.Default); err != nil {
		t.Fatalf("flush index: %v", err)
	}

	baseArgs := []string{
		"client",
		"--server", serverURL,
		"--disable-mtls",
		"query",
		"--namespace", namespaces.Default,
	}
	stagedParts := []string{`and.eq{field=/report/status,value=staged}`, `and.eq{field=/report/region,value=emea}`}
	outJSON, _ := runCLICommandOutput(t, append(baseArgs, stagedParts...)...)
	var keyList []string
	if err := json.Unmarshal([]byte(strings.TrimSpace(outJSON)), &keyList); err != nil {
		t.Fatalf("decode key list: %v", err)
	}
	expectStaged := stagedKeys(docs, "staged")
	if !stringSetEqual(keyList, expectStaged) {
		t.Fatalf("expected staged keys %v, got %v", expectStaged, keyList)
	}

	textPath := filepath.Join(t.TempDir(), "keys.txt")
	argsText := append([]string{}, baseArgs...)
	argsText = append(argsText,
		"--output", "text",
		"--file", textPath,
	)
	argsText = append(argsText, stagedParts...)
	runCLICommand(t, argsText...)
	textData, err := os.ReadFile(textPath)
	if err != nil {
		t.Fatalf("read text output: %v", err)
	}
	lines := strings.FieldsFunc(strings.TrimSpace(string(textData)), func(r rune) bool { return r == '\n' || r == '\r' })
	if !stringSetEqual(lines, expectStaged) {
		t.Fatalf("expected text keys %v, got %v", expectStaged, lines)
	}

	urgentSelector := `/report/urgent=true`
	ndjsonPath := filepath.Join(t.TempDir(), "docs.ndjson")
	argsDocs := append([]string{}, baseArgs...)
	argsDocs = append(argsDocs,
		"--documents",
		"--file", ndjsonPath,
		urgentSelector,
	)
	runCLICommand(t, argsDocs...)
	ndjsonData, err := os.ReadFile(ndjsonPath)
	if err != nil {
		t.Fatalf("read ndjson: %v", err)
	}
	ndjsonLines := strings.Split(strings.TrimSpace(string(ndjsonData)), "\n")
	if len(ndjsonLines) != urgentCount(docs) {
		t.Fatalf("expected %d urgent docs, got %d", urgentCount(docs), len(ndjsonLines))
	}
	for i, line := range ndjsonLines {
		var doc map[string]any
		if err := json.Unmarshal([]byte(line), &doc); err != nil {
			t.Fatalf("decode ndjson line %d: %v", i, err)
		}
		report := doc["report"].(map[string]any)
		if report["urgent"] != true {
			t.Fatalf("line %d missing urgent flag", i)
		}
	}

	dir := filepath.Join(t.TempDir(), "docsdir")
	argsDir := append([]string{}, baseArgs...)
	argsDir = append(argsDir,
		"--documents",
		"--directory", dir,
		`/report/region="emea"`,
	)
	runCLICommand(t, argsDir...)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("list dir: %v", err)
	}
	files := entriesBySuffix(entries, ".json")
	if len(files) != emeaCount(docs) {
		t.Fatalf("expected %d files, got %d", emeaCount(docs), len(files))
	}
	for _, name := range files {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		var doc map[string]any
		if err := json.Unmarshal(data, &doc); err != nil {
			t.Fatalf("decode %s: %v", name, err)
		}
		report := doc["report"].(map[string]any)
		if report["region"] != "emea" {
			t.Fatalf("file %s expected region emea, got %v", name, report["region"])
		}
	}
}

func TestCLIClientQueryRequiresSelector(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	err := runCLICommandExpectError(t, "client", "query")
	if err == nil {
		t.Fatalf("expected error when selector missing")
	}
}

func stagedKeys(docs []queryDoc, status string) []string {
	var out []string
	for _, doc := range docs {
		if doc.status == status {
			out = append(out, doc.key)
		}
	}
	return out
}

func urgentCount(docs []queryDoc) int {
	count := 0
	for _, doc := range docs {
		if doc.urgent {
			count++
		}
	}
	return count
}

func emeaCount(docs []queryDoc) int {
	count := 0
	for _, doc := range docs {
		if doc.region == "emea" {
			count++
		}
	}
	return count
}

func stringSetEqual(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	gs := append([]string(nil), got...)
	ws := append([]string(nil), want...)
	sort.Strings(gs)
	sort.Strings(ws)
	for i := range gs {
		if gs[i] != ws[i] {
			return false
		}
	}
	return true
}

func entriesBySuffix(entries []os.DirEntry, suffix string) []string {
	var out []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, suffix) {
			out = append(out, name)
		}
	}
	return out
}

func startCLITestServer(t *testing.T) *lockd.TestServer {
	t.Helper()
	return startCLITestServerWithClock(t, nil)
}

func startCLITestServerWithClock(t *testing.T, clk clock.Clock) *lockd.TestServer {
	t.Helper()
	opts := []lockd.TestServerOption{
		lockd.WithTestConfigFunc(func(cfg *lockd.Config) {
			cfg.Store = "mem://"
			cfg.Listen = "127.0.0.1:0"
			cfg.DisableMTLS = true
			cfg.DefaultNamespace = namespaces.Default
		}),
		lockd.WithoutTestMTLS(),
		lockd.WithTestCloseDefaults(lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(200*time.Millisecond)),
		lockd.WithTestLoggerFromTB(t, pslog.WarnLevel),
	}
	if clk != nil {
		opts = append(opts, lockd.WithTestClock(clk))
	}
	ts := lockd.StartTestServer(t, opts...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	})
	return ts
}

func stageKeyWithTxn(t *testing.T, ts *lockd.TestServer, key, txn string, state map[string]any) {
	stageKeyWithTxnTTL(t, ts, key, txn, state, 30)
}

func stageKeyWithTxnTTL(t *testing.T, ts *lockd.TestServer, key, txn string, state map[string]any, ttlSeconds int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lease, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        key,
		Owner:      "stager",
		TTLSeconds: ttlSeconds,
		BlockSecs:  client.BlockWaitForever,
		TxnID:      txn,
	})
	if err != nil {
		t.Fatalf("acquire %s: %v", key, err)
	}
	if err := lease.Save(ctx, state); err != nil {
		lease.Release(ctx)
		t.Fatalf("save %s: %v", key, err)
	}
	// Intentionally keep the staged state under the transaction without
	// releasing the lease here. Transaction commands (prepare/commit/rollback/
	// replay) are responsible for deciding and clearing the lease.
}

func loadKeyState(ctx context.Context, t *testing.T, ts *lockd.TestServer, key string) map[string]any {
	t.Helper()
	return loadKeyStateNamespace(ctx, t, ts, namespaces.Default, key)
}

func loadKeyStateNamespace(ctx context.Context, t *testing.T, ts *lockd.TestServer, ns, key string) map[string]any {
	t.Helper()
	lease, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  ns,
		Key:        key,
		Owner:      "verifier",
		TTLSeconds: 15,
		BlockSecs:  client.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("verify acquire %s: %v", key, err)
	}
	defer lease.Release(ctx)
	var state map[string]any
	if err := lease.Load(ctx, &state); err != nil {
		t.Fatalf("load %s: %v", key, err)
	}
	return state
}

func TestClientCLIConfigDefaultLoggerDisabled(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	var buf bytes.Buffer
	baseLogger := pslog.NewStructured(context.Background(), &buf).With("base", "yes")
	cmd := &cobra.Command{Use: "client-test"}
	cfg := addClientConnectionFlags(cmd, true, baseLogger)
	if err := cmd.PersistentFlags().Parse([]string{}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	if err := cfg.load(); err != nil {
		t.Fatalf("load config: %v", err)
	}
	defer cfg.cleanup()

	if cfg.logger != nil {
		t.Fatalf("expected no client logger by default")
	}
}

func TestClientCLIConfigVerboseInheritsBaseLogger(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	var buf bytes.Buffer
	baseLogger := pslog.NewStructured(context.Background(), &buf).With("base", "yes")
	cmd := &cobra.Command{Use: "client-test"}
	cfg := addClientConnectionFlags(cmd, true, baseLogger)
	if err := cmd.PersistentFlags().Parse([]string{"-v"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	if err := cfg.load(); err != nil {
		t.Fatalf("load config: %v", err)
	}
	defer cfg.cleanup()

	logger, ok := cfg.logger.(pslog.Logger)
	if !ok {
		t.Fatalf("expected pslog.Logger, got %T", cfg.logger)
	}
	logger.Info("verbose-probe")
	output := buf.String()
	if !strings.Contains(output, `"msg":"verbose-probe"`) {
		t.Fatalf("expected probe log, got %q", output)
	}
	if !strings.Contains(output, `"base":"yes"`) {
		t.Fatalf("expected inherited base logger fields, got %q", output)
	}
}

func TestClientCLIConfigFlagLogLevelOverridesEnv(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("LOCKD_CLIENT_LOG_LEVEL", "error")

	var buf bytes.Buffer
	baseLogger := pslog.NewStructured(context.Background(), &buf).With("base", "yes")
	cmd := &cobra.Command{Use: "client-test"}
	cfg := addClientConnectionFlags(cmd, true, baseLogger)
	if err := cmd.PersistentFlags().Parse([]string{"--log-level", "trace"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	if err := cfg.load(); err != nil {
		t.Fatalf("load config: %v", err)
	}
	defer cfg.cleanup()

	logger, ok := cfg.logger.(pslog.Logger)
	if !ok {
		t.Fatalf("expected pslog.Logger, got %T", cfg.logger)
	}
	logger.Info("flag-overrides-env")
	output := buf.String()
	if !strings.Contains(output, `"msg":"flag-overrides-env"`) {
		t.Fatalf("expected trace-enabled info log when flag overrides env, got %q", output)
	}
}

func runCLICommand(t *testing.T, args ...string) {
	t.Helper()
	_, _ = runCLICommandOutput(t, args...)
}

func runCLICommandOutput(t *testing.T, args ...string) (string, string) {
	t.Helper()
	cmd := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("command failed (%v): stdout=%q stderr=%q", err, stdout.String(), stderr.String())
	}
	return stdout.String(), stderr.String()
}

func runCLICommandExpectError(t *testing.T, args ...string) error {
	t.Helper()
	cmd := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	cmd.SetArgs(args)
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	if err := cmd.Execute(); err != nil {
		return err
	}
	t.Fatalf("command unexpectedly succeeded: stdout=%q stderr=%q", stdout.String(), stderr.String())
	return nil
}
