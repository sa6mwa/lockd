package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	"pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

func TestCLIClientStateLifecycle(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")
	viper.Reset()
	t.Cleanup(viper.Reset)

	ts := startCLITestServer(t)
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

	ts := startCLITestServer(t)
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
	ts := lockd.StartTestServer(t,
		lockd.WithTestConfigFunc(func(cfg *lockd.Config) {
			cfg.Store = "mem://"
			cfg.Listen = "127.0.0.1:0"
			cfg.DisableMTLS = true
			cfg.DefaultNamespace = namespaces.Default
		}),
		lockd.WithoutTestMTLS(),
		lockd.WithTestCloseDefaults(lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(200*time.Millisecond)),
		lockd.WithTestLoggerFromTB(t, pslog.WarnLevel),
	)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	})
	return ts
}

func runCLICommand(t *testing.T, args ...string) {
	t.Helper()
	_, _ = runCLICommandOutput(t, args...)
}

func runCLICommandOutput(t *testing.T, args ...string) (string, string) {
	t.Helper()
	cmd := newRootCommand(pslog.NewStructured(io.Discard))
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
	cmd := newRootCommand(pslog.NewStructured(io.Discard))
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
