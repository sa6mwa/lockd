//go:build integration

package queuetestutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/xid"
	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

// InstallWatchdog fails the test if the timeout elapses, dumping all goroutines.
func InstallWatchdog(t testing.TB, name string, timeout time.Duration) {
	t.Helper()
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	timer := time.AfterFunc(timeout, func() {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("watchdog %s triggered after %s\n%s", name, timeout, string(buf[:n]))
	})
	t.Cleanup(func() {
		timer.Stop()
	})
}

// QRFChecklist tracks whether clients observed QRF throttling states during a test run.
type QRFChecklist struct {
	throttled atomic.Bool
	softArm   atomic.Bool
	engaged   atomic.Bool

	mu     sync.Mutex
	states map[string]int
}

// RecordError inspects err and records QRF state information when the error represents a throttling response.
func (c *QRFChecklist) RecordError(err error) {
	if err == nil {
		return
	}
	var apiErr *lockdclient.APIError
	if !errors.As(err, &apiErr) {
		return
	}
	if apiErr.Response.ErrorCode != "throttled" && apiErr.Status != http.StatusTooManyRequests {
		return
	}
	c.throttled.Store(true)
	state := strings.TrimSpace(apiErr.QRFState)
	if state == "" {
		state = "unknown"
	}
	c.mu.Lock()
	if c.states == nil {
		c.states = make(map[string]int)
	}
	c.states[state]++
	c.mu.Unlock()
	switch state {
	case "soft_arm":
		c.softArm.Store(true)
	case "engaged":
		c.engaged.Store(true)
	}
}

// Throttled reports whether the client observed any throttling responses.
func (c *QRFChecklist) Throttled() bool {
	return c.throttled.Load()
}

// SoftArmSeen reports whether a soft-arm posture was observed via client responses.
func (c *QRFChecklist) SoftArmSeen() bool {
	return c.softArm.Load()
}

// EngagedSeen reports whether an engaged posture was observed via client responses.
func (c *QRFChecklist) EngagedSeen() bool {
	return c.engaged.Load()
}

// StateCounts returns a copy of the recorded QRF state counts.
func (c *QRFChecklist) StateCounts() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.states))
	for k, v := range c.states {
		out[k] = v
	}
	return out
}

// QueueName returns a unique queue name with the provided prefix.
func QueueName(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "queue"
	}
	return fmt.Sprintf("%s-%s", prefix, uniqueSuffix())
}

// QueueOwner returns a unique owner identity with the provided prefix.
func QueueOwner(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "owner"
	}
	return fmt.Sprintf("%s-%s", prefix, uniqueSuffix())
}

func replayDeadlineForStore(store string) time.Duration {
	store = strings.ToLower(strings.TrimSpace(store))
	switch {
	case strings.HasPrefix(store, "aws://"),
		strings.HasPrefix(store, "s3://"),
		strings.HasPrefix(store, "azure://"):
		return 60 * time.Second
	default:
		return 20 * time.Second
	}
}

// ReplayDeadlineForStore returns a reasonable deadline for replay operations by backend type.
func ReplayDeadlineForStore(store string) time.Duration {
	return replayDeadlineForStore(store)
}

// StartQueueTestServer launches a lockd test server with helpful defaults.
func StartQueueTestServer(t testing.TB, cfg lockd.Config, extraClientOpts ...lockdclient.Option) *lockd.TestServer {
	t.Helper()
	serverLogger := lockd.NewTestingLogger(t, pslog.TraceLevel)
	return StartQueueTestServerWithOptions(t, cfg, []lockd.TestServerOption{lockd.WithTestLogger(serverLogger)}, extraClientOpts...)
}

// StartQueueTestServerWithLogger launches a test server with a custom logger.
func StartQueueTestServerWithLogger(t testing.TB, cfg lockd.Config, logger pslog.Logger, extraClientOpts ...lockdclient.Option) *lockd.TestServer {
	t.Helper()
	return StartQueueTestServerWithOptions(t, cfg, []lockd.TestServerOption{lockd.WithTestLogger(logger)}, extraClientOpts...)
}

// StartQueueTestServerWithOptions allows callers to supply additional server options alongside client options.
func StartQueueTestServerWithOptions(t testing.TB, cfg lockd.Config, extraServerOpts []lockd.TestServerOption, extraClientOpts ...lockdclient.Option) *lockd.TestServer {
	t.Helper()

	clientLogger := lockd.NewTestingLogger(t, pslog.TraceLevel)
	cryptotest.ConfigureTCAuth(t, &cfg)
	baseClientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(60 * time.Second),
		lockdclient.WithKeepAliveTimeout(60 * time.Second),
		lockdclient.WithCloseTimeout(60 * time.Second),
		lockdclient.WithLogger(clientLogger),
	}
	if len(extraClientOpts) > 0 {
		baseClientOpts = append(baseClientOpts, extraClientOpts...)
	}

	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestClientOptions(baseClientOpts...),
	}
	options = append(options, extraServerOpts...)
	options = append(options, cryptotest.SharedMTLSOptions(t)...)

	return lockd.StartTestServer(t, options...)
}

func stopTestServer(ts *lockd.TestServer) {
	if ts == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = ts.Stop(ctx)
}

func stopTestServerNoDrain(ts *lockd.TestServer) {
	if ts == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = ts.Stop(ctx, lockd.WithDrainLeases(-1))
}

func recordTxnDecision(t testing.TB, backend storage.Backend, txnID string, state core.TxnState, participants []core.TxnParticipant) {
	t.Helper()
	if backend == nil {
		t.Fatalf("txn record backend required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var rec core.TxnRecord
	var etag string
	obj, err := backend.GetObject(ctx, ".txns", txnID)
	if err == nil {
		func() {
			defer obj.Reader.Close()
			if decodeErr := json.NewDecoder(obj.Reader).Decode(&rec); decodeErr != nil {
				t.Fatalf("decode txn record: %v", decodeErr)
			}
		}()
		if obj.Info != nil {
			etag = obj.Info.ETag
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("load txn record: %v", err)
	}

	now := time.Now().Unix()
	if rec.TxnID == "" {
		rec.TxnID = txnID
	}
	if rec.CreatedAtUnix == 0 {
		rec.CreatedAtUnix = now
	}
	rec.State = state
	rec.UpdatedAtUnix = now
	if len(participants) > 0 {
		if len(rec.Participants) == 0 {
			rec.Participants = append([]core.TxnParticipant(nil), participants...)
		} else {
			existing := make(map[core.TxnParticipant]struct{}, len(rec.Participants))
			for _, p := range rec.Participants {
				existing[p] = struct{}{}
			}
			for _, p := range participants {
				if _, ok := existing[p]; ok {
					continue
				}
				rec.Participants = append(rec.Participants, p)
				existing[p] = struct{}{}
			}
		}
	}

	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(&rec); err != nil {
		t.Fatalf("encode txn record: %v", err)
	}
	if _, err := backend.PutObject(ctx, ".txns", txnID, bytes.NewReader(buf.Bytes()), storage.PutObjectOptions{
		ExpectedETag: etag,
		ContentType:  storage.ContentTypeJSON,
	}); err != nil {
		t.Fatalf("store txn record: %v", err)
	}
}

// MustEnqueueBytes enqueues payload to queue, failing the test on error.
func MustEnqueueBytes(t testing.TB, cli *lockdclient.Client, queue string, payload []byte) *api.EnqueueResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := cli.Enqueue(ctx, queue, bytes.NewReader(payload), lockdclient.EnqueueOptions{
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("enqueue %s: %v", queue, err)
	}
	return res
}

// MustDequeueMessage pulls one message from queue, retrying until timeout.
func MustDequeueMessage(t testing.TB, cli *lockdclient.Client, queue, owner string, waitSeconds int64, timeout time.Duration) *lockdclient.QueueMessage {
	t.Helper()
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		ctxTimeout := remaining
		if ctxTimeout > 5*time.Second {
			ctxTimeout = 5 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
		msg, err := cli.Dequeue(ctx, queue, lockdclient.DequeueOptions{
			Owner:        owner,
			BlockSeconds: waitSeconds,
		})
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				time.Sleep(25 * time.Millisecond)
				continue
			}
			var apiErr *lockdclient.APIError
			if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
				time.Sleep(25 * time.Millisecond)
				continue
			}
			t.Fatalf("dequeue %s: %v", queue, err)
		}
		if msg != nil && msg.MessageID() != "" {
			return msg
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("dequeue %s: no message available within %s", queue, timeout)
	return nil
}

// MustDequeueMessageTxn dequeues one message enlisted in the provided txn id.
func MustDequeueMessageTxn(t testing.TB, cli *lockdclient.Client, queue, owner, txnID string, waitSeconds int64, timeout time.Duration) *lockdclient.QueueMessage {
	t.Helper()
	if txnID == "" {
		t.Fatalf("txn id required for MustDequeueMessageTxn")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		ctxTimeout := remaining
		if ctxTimeout > 5*time.Second {
			ctxTimeout = 5 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
		msg, err := cli.Dequeue(ctx, queue, lockdclient.DequeueOptions{
			Owner:        owner,
			BlockSeconds: waitSeconds,
			TxnID:        txnID,
		})
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				time.Sleep(25 * time.Millisecond)
				continue
			}
			var apiErr *lockdclient.APIError
			if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
				time.Sleep(25 * time.Millisecond)
				continue
			}
			t.Fatalf("dequeue %s (txn): %v", queue, err)
		}
		if msg != nil && msg.MessageID() != "" {
			return msg
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("dequeue %s (txn=%s): no message available within %s", queue, txnID, timeout)
	return nil
}

// MustDequeueMessages dequeues up to batchSize messages, failing the test on error.
func MustDequeueMessages(t testing.TB, cli *lockdclient.Client, queue, owner string, waitSeconds int64, timeout time.Duration, batchSize int) []*lockdclient.QueueMessage {
	t.Helper()
	if batchSize <= 0 {
		batchSize = 1
	}
	collected := make([]*lockdclient.QueueMessage, 0, batchSize)
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		ctxTimeout := remaining
		if ctxTimeout > 5*time.Second {
			ctxTimeout = 5 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
		msgs, err := cli.DequeueBatch(ctx, queue, lockdclient.DequeueOptions{
			Owner:        owner,
			BlockSeconds: waitSeconds,
			PageSize:     batchSize,
		})
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				time.Sleep(25 * time.Millisecond)
				continue
			}
			var apiErr *lockdclient.APIError
			if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
				time.Sleep(25 * time.Millisecond)
				continue
			}
			t.Fatalf("dequeue batch %s: %v", queue, err)
		}
		if len(msgs) == 0 {
			t.Logf("MustDequeueMessages: received 0 messages, retrying")
			time.Sleep(25 * time.Millisecond)
			continue
		}
		t.Logf("MustDequeueMessages: received %d messages (total %d/%d)", len(msgs), len(collected)+len(msgs), batchSize)
		collected = append(collected, msgs...)
		if len(collected) >= batchSize {
			return collected[:batchSize]
		}
	}
	t.Fatalf("dequeue batch %s: gathered %d/%d messages within %s", queue, len(collected), batchSize, timeout)
	return nil
}

// RunQueueTxnDecisionScenario exercises queue enlistment in a transaction and
// verifies commit->ACK and rollback->NACK via the TC decision endpoint.
func RunQueueTxnDecisionScenario(t testing.TB, ts *lockd.TestServer, commit bool) {
	t.Helper()
	queueName := QueueName("txn-queue")
	owner := QueueOwner("txn-owner")
	cli := ts.Client

	txnID := xid.New().String()

	MustEnqueueBytes(t, cli, queueName, []byte("txn-payload"))
	msg := MustDequeueMessageTxn(t, cli, queueName, owner, txnID, 1, 5*time.Second)
	if got := msg.TxnID(); got != txnID {
		t.Fatalf("expected txn_id %s on dequeue, got %s", txnID, got)
	}

	ns := msg.Namespace()
	if ns == "" {
		ns = "default"
	}
	namespacedKey, err := queue.MessageLeaseKey(ns, queueName, msg.MessageID())
	if err != nil {
		t.Fatalf("message lease key: %v", err)
	}
	participantKey := strings.TrimPrefix(namespacedKey, ns+"/")

	httpClient := cryptotest.RequireTCHTTPClient(t, ts)
	state := "commit"
	if !commit {
		state = "rollback"
	}
	payload := api.TxnDecisionRequest{
		TxnID: txnID,
		State: state,
		Participants: []api.TxnParticipant{{
			Namespace: ns,
			Key:       participantKey,
		}},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal txn payload: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.BaseURL+"/v1/txn/decide", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build txn %s request: %v", state, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("txn %s request failed: %v", state, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("txn %s unexpected status %d: %s", state, resp.StatusCode, data)
	}

	time.Sleep(150 * time.Millisecond)

	if commit {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			m, err := cli.Dequeue(ctx, queueName, lockdclient.DequeueOptions{
				Owner:        owner + "-probe",
				BlockSeconds: api.BlockNoWait,
			})
			cancel()
			if err == nil && m != nil {
				t.Fatalf("expected queue empty after commit, got message %s", m.MessageID())
			}
			var apiErr *lockdclient.APIError
			if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("message still visible after commit")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rbMsg, err := cli.Dequeue(ctx, queueName, lockdclient.DequeueOptions{
		Owner:        owner + "-rb",
		BlockSeconds: 1,
	})
	if err != nil {
		t.Fatalf("rollback dequeue: %v", err)
	}
	if rbMsg == nil || rbMsg.MessageID() != msg.MessageID() {
		t.Fatalf("expected same message after rollback, got %+v", rbMsg)
	}
	_ = rbMsg.Ack(context.Background())
}

func postTxnDecision(t testing.TB, ts *lockd.TestServer, payload api.TxnDecisionRequest) {
	t.Helper()
	if ts == nil {
		t.Fatalf("txn decision requires test server")
	}
	httpClient := cryptotest.RequireTCHTTPClient(t, ts)
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal txn payload: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.BaseURL+"/v1/txn/decide", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build txn request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("txn request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("txn %s unexpected status %d: %s", payload.State, resp.StatusCode, data)
	}
}

// RunQueueTxnDecisionFanoutScenario exercises queue enlistment via a TC that fans
// out to an RM endpoint on another node.
func RunQueueTxnDecisionFanoutScenario(t testing.TB, tc, rm *lockd.TestServer, commit bool) {
	t.Helper()
	if tc == nil || rm == nil {
		t.Fatalf("tc/rm servers required")
	}
	cli := rm.Client
	if cli == nil {
		t.Fatalf("rm server missing client")
	}

	queueName := QueueName("txn-fanout-queue")
	owner := QueueOwner("txn-fanout-owner")
	txnID := xid.New().String()

	MustEnqueueBytes(t, cli, queueName, []byte("txn-fanout"))
	msg := MustDequeueMessageTxn(t, cli, queueName, owner, txnID, 1, 5*time.Second)
	if got := msg.TxnID(); got != txnID {
		t.Fatalf("expected txn_id %s on dequeue, got %s", txnID, got)
	}

	ns := msg.Namespace()
	if ns == "" {
		ns = namespaces.Default
	}
	namespacedKey, err := queue.MessageLeaseKey(ns, queueName, msg.MessageID())
	if err != nil {
		t.Fatalf("message lease key: %v", err)
	}
	participantKey := strings.TrimPrefix(namespacedKey, ns+"/")

	state := "commit"
	if !commit {
		state = "rollback"
	}
	payload := api.TxnDecisionRequest{
		TxnID: txnID,
		State: state,
		Participants: []api.TxnParticipant{{
			Namespace: ns,
			Key:       participantKey,
		}},
	}
	postTxnDecision(t, tc, payload)

	assertQueueEmpty := func(client *lockdclient.Client, label string) {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			ctxProbe, cancelProbe := context.WithTimeout(context.Background(), 250*time.Millisecond)
			m, err := client.Dequeue(ctxProbe, queueName, lockdclient.DequeueOptions{
				Owner:        QueueOwner("txn-probe-" + label),
				BlockSeconds: api.BlockNoWait,
			})
			cancelProbe()
			if err == nil && m != nil {
				t.Fatalf("expected queue empty after commit (%s), got message %s", label, m.MessageID())
			}
			var apiErr *lockdclient.APIError
			if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("message still visible after commit (%s)", label)
	}

	if commit {
		assertQueueEmpty(cli, "rm")
		if tc.Client != nil {
			assertQueueEmpty(tc.Client, "tc")
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	probe := tc.Client
	if probe == nil {
		probe = cli
	}
	rbMsg, err := probe.Dequeue(ctx, queueName, lockdclient.DequeueOptions{
		Owner:        owner + "-rb",
		BlockSeconds: 1,
	})
	if err != nil {
		t.Fatalf("rollback dequeue: %v", err)
	}
	if rbMsg == nil || rbMsg.MessageID() != msg.MessageID() {
		t.Fatalf("expected same message after rollback, got %+v", rbMsg)
	}
	_ = rbMsg.Ack(context.Background())
}

// RunQueueTxnDecisionRestartScenario exercises queue enlistment when the RM is
// restarted after a TC decision is recorded.
func RunQueueTxnDecisionRestartScenario(t testing.TB, tc, rm *lockd.TestServer, restartRM func(testing.TB) *lockd.TestServer, commit bool, replayDeadline time.Duration) {
	t.Helper()
	if tc == nil || rm == nil || restartRM == nil {
		t.Fatalf("tc/rm servers required")
	}
	cli := rm.Client
	if cli == nil {
		t.Fatalf("rm server missing client")
	}

	queueName := QueueName("txn-restart-queue")
	owner := QueueOwner("txn-restart-owner")
	txnID := xid.New().String()

	MustEnqueueBytes(t, cli, queueName, []byte("txn-restart"))
	msg := MustDequeueMessageTxn(t, cli, queueName, owner, txnID, 1, 5*time.Second)
	if got := msg.TxnID(); got != txnID {
		t.Fatalf("expected txn_id %s on dequeue, got %s", txnID, got)
	}

	ns := msg.Namespace()
	if ns == "" {
		ns = namespaces.Default
	}
	namespacedKey, err := queue.MessageLeaseKey(ns, queueName, msg.MessageID())
	if err != nil {
		t.Fatalf("message lease key: %v", err)
	}
	participantKey := strings.TrimPrefix(namespacedKey, ns+"/")

	stopTestServerNoDrain(rm)

	state := "commit"
	if !commit {
		state = "rollback"
	}
	payload := api.TxnDecisionRequest{
		TxnID: txnID,
		State: state,
		Participants: []api.TxnParticipant{{
			Namespace: ns,
			Key:       participantKey,
		}},
	}
	postTxnDecision(t, tc, payload)
	stopTestServerNoDrain(tc)

	rm2 := restartRM(t)
	if rm2 == nil || rm2.Client == nil {
		t.Fatalf("restart RM missing client")
	}
	cli2 := rm2.Client

	if replayDeadline <= 0 {
		replayDeadline = 20 * time.Second
	}

	if commit {
		deadline := time.Now().Add(replayDeadline)
		for time.Now().Before(deadline) {
			ctxProbe, cancelProbe := context.WithTimeout(context.Background(), 500*time.Millisecond)
			m, err := cli2.Dequeue(ctxProbe, queueName, lockdclient.DequeueOptions{
				Owner:        QueueOwner("txn-probe"),
				BlockSeconds: api.BlockNoWait,
			})
			cancelProbe()
			if err == nil && m != nil {
				t.Fatalf("expected queue empty after commit, got message %s", m.MessageID())
			}
			var apiErr *lockdclient.APIError
			if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
				return
			}
			time.Sleep(75 * time.Millisecond)
		}
		t.Fatalf("message still visible after commit")
		return
	}

	deadline := time.Now().Add(replayDeadline)
	for time.Now().Before(deadline) {
		ctxTimeout := 5 * time.Second
		if remaining := time.Until(deadline); remaining < ctxTimeout {
			ctxTimeout = remaining
		}
		ctxRb, cancelRb := context.WithTimeout(context.Background(), ctxTimeout)
		msgRb, err := cli2.Dequeue(ctxRb, queueName, lockdclient.DequeueOptions{
			Owner:        owner + "-rb",
			BlockSeconds: 1,
		})
		cancelRb()
		if err == nil && msgRb != nil {
			if msgRb.MessageID() != msg.MessageID() {
				t.Fatalf("expected same message after rollback; want %s got %s", msg.MessageID(), msgRb.MessageID())
			}
			if err := msgRb.Ack(context.Background()); err != nil {
				t.Fatalf("ack rollback message: %v", err)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("message not re-delivered after rollback")
}

// RunQueueTxnDecisionReplayWakeScenario ensures replayed rollbacks wake waiting consumers promptly.
func RunQueueTxnDecisionReplayWakeScenario(t testing.TB, tc, rm *lockd.TestServer, restartRM func(testing.TB) *lockd.TestServer, replayDeadline, wakeDeadline time.Duration) {
	t.Helper()
	if tc == nil || rm == nil || restartRM == nil {
		t.Fatalf("tc/rm servers required")
	}
	cli := rm.Client
	if cli == nil {
		t.Fatalf("rm server missing client")
	}

	queueName := QueueName("txn-replay-wake")
	owner := QueueOwner("txn-replay-wake-owner")
	txnID := xid.New().String()

	MustEnqueueBytes(t, cli, queueName, []byte("txn-replay-wake"))
	msg := MustDequeueMessageTxn(t, cli, queueName, owner, txnID, 1, 5*time.Second)
	if got := msg.TxnID(); got != txnID {
		t.Fatalf("expected txn_id %s on dequeue, got %s", txnID, got)
	}

	ns := msg.Namespace()
	if ns == "" {
		ns = namespaces.Default
	}
	namespacedKey, err := queue.MessageLeaseKey(ns, queueName, msg.MessageID())
	if err != nil {
		t.Fatalf("message lease key: %v", err)
	}
	participantKey := strings.TrimPrefix(namespacedKey, ns+"/")

	backend := rm.Backend()
	stopTestServerNoDrain(rm)
	recordTxnDecision(t, backend, txnID, core.TxnStateRollback, []core.TxnParticipant{
		{Namespace: ns, Key: participantKey},
	})
	stopTestServerNoDrain(tc)

	rm2 := restartRM(t)
	if rm2 == nil || rm2.Client == nil {
		t.Fatalf("restart RM missing client")
	}
	cli2 := rm2.Client
	tcClient := cryptotest.RequireTCClient(t, rm2)

	if replayDeadline <= 0 {
		replayDeadline = 20 * time.Second
	}
	if wakeDeadline <= 0 {
		wakeDeadline = 2 * time.Second
	}

	// Ensure the message is still hidden before replay.
	{
		probeCtx, cancelProbe := context.WithTimeout(context.Background(), 1*time.Second)
		probe, err := cli2.Dequeue(probeCtx, queueName, lockdclient.DequeueOptions{
			Owner:        QueueOwner("txn-replay-probe"),
			BlockSeconds: api.BlockNoWait,
		})
		cancelProbe()
		if err == nil && probe != nil {
			t.Fatalf("message visible before replay")
		}
		var apiErr *lockdclient.APIError
		if err != nil && (!errors.As(err, &apiErr) || apiErr.Response.ErrorCode != "waiting") {
			t.Fatalf("expected waiting before replay, got %v", err)
		}
	}

	blockSeconds := int64(replayDeadline.Seconds())
	if blockSeconds < 5 {
		blockSeconds = 5
	}

	waitCtx, cancelWait := context.WithTimeout(context.Background(), replayDeadline+wakeDeadline)
	defer cancelWait()
	ready := make(chan struct{})
	resultCh := make(chan *lockdclient.QueueMessage, 1)
	errCh := make(chan error, 1)
	go func() {
		close(ready)
		waitMsg, err := cli2.Dequeue(waitCtx, queueName, lockdclient.DequeueOptions{
			Owner:        owner + "-wait",
			BlockSeconds: blockSeconds,
		})
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- waitMsg
	}()
	<-ready
	time.Sleep(150 * time.Millisecond)

	replayCtx, cancelReplay := context.WithTimeout(context.Background(), replayDeadline)
	_, err = tcClient.TxnReplay(replayCtx, txnID)
	cancelReplay()
	if err != nil {
		t.Fatalf("txn replay: %v", err)
	}
	replayDone := time.Now()

	select {
	case waitMsg := <-resultCh:
		if waitMsg == nil {
			t.Fatalf("rollback dequeue returned nil message")
		}
		if waitMsg.MessageID() != msg.MessageID() {
			t.Fatalf("expected same message after replay rollback; want %s got %s", msg.MessageID(), waitMsg.MessageID())
		}
		elapsed := time.Since(replayDone)
		if elapsed > wakeDeadline {
			t.Fatalf("replay wake took %s (limit %s)", elapsed, wakeDeadline)
		}
		if err := waitMsg.Ack(context.Background()); err != nil {
			t.Fatalf("ack replay rollback message: %v", err)
		}
	case err := <-errCh:
		t.Fatalf("waiting dequeue failed: %v", err)
	case <-time.After(wakeDeadline):
		t.Fatalf("message not re-delivered within %s after replay", wakeDeadline)
	}
}

// RunQueueTxnStatefulDecisionScenario exercises stateful dequeue enlistment in a
// transaction and verifies commit deletes message+state while rollback keeps both.
func RunQueueTxnStatefulDecisionScenario(t testing.TB, ts *lockd.TestServer, commit bool) {
	t.Helper()
	queueName := QueueName("txn-stateful")
	owner := QueueOwner("txn-stateful-owner")
	cli := ts.Client

	txnID := xid.New().String()

	MustEnqueueBytes(t, cli, queueName, []byte("txn-stateful"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	msg, err := cli.DequeueWithState(ctx, queueName, lockdclient.DequeueOptions{
		Owner:        owner,
		TxnID:        txnID,
		BlockSeconds: 1,
	})
	cancel()
	if err != nil {
		t.Fatalf("stateful dequeue: %v", err)
	}
	if msg == nil {
		t.Fatalf("stateful dequeue returned nil message")
	}
	if got := msg.TxnID(); got != txnID {
		t.Fatalf("expected txn_id %s on dequeue, got %s", txnID, got)
	}
	if msg.StateHandle() == nil {
		t.Fatalf("expected state handle on stateful dequeue")
	}

	ns := msg.Namespace()
	if ns == "" {
		ns = namespaces.Default
	}
	messageKey, err := queue.MessageLeaseKey(ns, queueName, msg.MessageID())
	if err != nil {
		t.Fatalf("message lease key: %v", err)
	}
	stateKey, err := queue.StateLeaseKey(ns, queueName, msg.MessageID())
	if err != nil {
		t.Fatalf("state lease key: %v", err)
	}
	participantMessage := strings.TrimPrefix(messageKey, ns+"/")
	participantState := strings.TrimPrefix(stateKey, ns+"/")

	httpClient := cryptotest.RequireTCHTTPClient(t, ts)
	state := "commit"
	if !commit {
		state = "rollback"
	}
	payload := api.TxnDecisionRequest{
		TxnID: txnID,
		State: state,
		Participants: []api.TxnParticipant{
			{Namespace: ns, Key: participantMessage},
			{Namespace: ns, Key: participantState},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal txn payload: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.BaseURL+"/v1/txn/decide", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build txn %s request: %v", state, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("txn %s request failed: %v", state, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("txn %s unexpected status %d: %s", state, resp.StatusCode, data)
	}

	backend := ts.Backend()
	if backend == nil {
		t.Fatalf("test server backend not available")
	}

	stateObjKey := queueStateObjectKey(queueName, msg.MessageID())
	stateMetaKey := participantState

	if commit {
		gotWaiting := false
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			ctxProbe, cancelProbe := context.WithTimeout(context.Background(), 250*time.Millisecond)
			m, err := cli.Dequeue(ctxProbe, queueName, lockdclient.DequeueOptions{
				Owner:        owner + "-probe",
				BlockSeconds: api.BlockNoWait,
			})
			cancelProbe()
			if err == nil && m != nil {
				t.Fatalf("expected queue empty after commit, got message %s", m.MessageID())
			}
			var apiErr *lockdclient.APIError
			if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
				gotWaiting = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !gotWaiting {
			t.Fatalf("message still visible after commit")
		}
		checkCtx, cancelCheck := context.WithTimeout(context.Background(), 2*time.Second)
		if err := waitForQueueStateObject(checkCtx, backend, ns, stateObjKey, false); err != nil {
			cancelCheck()
			t.Fatalf("state object still present after commit: %v", err)
		}
		cancelCheck()
		metaRes, err := backend.LoadMeta(context.Background(), ns, stateMetaKey)
		if err != nil {
			t.Fatalf("load state meta after commit: %v", err)
		}
		meta := metaRes.Meta
		if meta.Lease != nil {
			t.Fatalf("expected state lease cleared after commit")
		}
		return
	}

	checkCtx, cancelCheck := context.WithTimeout(context.Background(), 2*time.Second)
	if err := waitForQueueStateObject(checkCtx, backend, ns, stateObjKey, true); err != nil {
		cancelCheck()
		t.Fatalf("state object missing after rollback: %v", err)
	}
	cancelCheck()
	metaRes, err := backend.LoadMeta(context.Background(), ns, stateMetaKey)
	if err != nil {
		t.Fatalf("load state meta after rollback: %v", err)
	}
	meta := metaRes.Meta
	if meta.Lease != nil {
		t.Fatalf("expected state lease cleared after rollback")
	}

	ctxRb, cancelRb := context.WithTimeout(context.Background(), 3*time.Second)
	rbMsg, err := cli.Dequeue(ctxRb, queueName, lockdclient.DequeueOptions{
		Owner:        owner + "-rb",
		BlockSeconds: 1,
	})
	cancelRb()
	if err != nil {
		t.Fatalf("rollback dequeue: %v", err)
	}
	if rbMsg == nil || rbMsg.MessageID() != msg.MessageID() {
		t.Fatalf("expected same message after rollback, got %+v", rbMsg)
	}
	if err := rbMsg.Ack(context.Background()); err != nil {
		t.Fatalf("ack rollback message: %v", err)
	}
}

// RunQueueTxnStatefulDecisionFanoutScenario exercises stateful dequeue enlistment
// via a TC that fans out to an RM endpoint on another node.
func RunQueueTxnStatefulDecisionFanoutScenario(t testing.TB, tc, rm *lockd.TestServer, commit bool) {
	t.Helper()
	if tc == nil || rm == nil {
		t.Fatalf("tc/rm servers required")
	}
	cli := rm.Client
	if cli == nil {
		t.Fatalf("rm server missing client")
	}

	queueName := QueueName("txn-stateful-fanout")
	owner := QueueOwner("txn-stateful-fanout-owner")
	txnID := xid.New().String()

	MustEnqueueBytes(t, cli, queueName, []byte("txn-stateful-fanout"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	msg, err := cli.DequeueWithState(ctx, queueName, lockdclient.DequeueOptions{
		Owner:        owner,
		TxnID:        txnID,
		BlockSeconds: 1,
	})
	cancel()
	if err != nil {
		t.Fatalf("stateful dequeue: %v", err)
	}
	if msg == nil {
		t.Fatalf("stateful dequeue returned nil message")
	}
	if got := msg.TxnID(); got != txnID {
		t.Fatalf("expected txn_id %s on dequeue, got %s", txnID, got)
	}
	if msg.StateHandle() == nil {
		t.Fatalf("expected state handle on stateful dequeue")
	}

	ns := msg.Namespace()
	if ns == "" {
		ns = namespaces.Default
	}
	messageKey, err := queue.MessageLeaseKey(ns, queueName, msg.MessageID())
	if err != nil {
		t.Fatalf("message lease key: %v", err)
	}
	stateKey, err := queue.StateLeaseKey(ns, queueName, msg.MessageID())
	if err != nil {
		t.Fatalf("state lease key: %v", err)
	}
	participantMessage := strings.TrimPrefix(messageKey, ns+"/")
	participantState := strings.TrimPrefix(stateKey, ns+"/")

	state := "commit"
	if !commit {
		state = "rollback"
	}
	payload := api.TxnDecisionRequest{
		TxnID: txnID,
		State: state,
		Participants: []api.TxnParticipant{
			{Namespace: ns, Key: participantMessage},
			{Namespace: ns, Key: participantState},
		},
	}
	postTxnDecision(t, tc, payload)

	backend := rm.Backend()
	if backend == nil {
		t.Fatalf("test server backend not available")
	}

	stateObjKey := queueStateObjectKey(queueName, msg.MessageID())
	stateMetaKey := participantState

	assertQueueEmpty := func(client *lockdclient.Client, label string) {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			ctxProbe, cancelProbe := context.WithTimeout(context.Background(), 250*time.Millisecond)
			m, err := client.Dequeue(ctxProbe, queueName, lockdclient.DequeueOptions{
				Owner:        QueueOwner("txn-probe-" + label),
				BlockSeconds: api.BlockNoWait,
			})
			cancelProbe()
			if err == nil && m != nil {
				t.Fatalf("expected queue empty after commit (%s), got message %s", label, m.MessageID())
			}
			var apiErr *lockdclient.APIError
			if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("message still visible after commit (%s)", label)
	}

	if commit {
		assertQueueEmpty(cli, "rm")
		if tc.Client != nil {
			assertQueueEmpty(tc.Client, "tc")
		}
		checkCtx, cancelCheck := context.WithTimeout(context.Background(), 2*time.Second)
		if err := waitForQueueStateObject(checkCtx, backend, ns, stateObjKey, false); err != nil {
			cancelCheck()
			t.Fatalf("state object still present after commit: %v", err)
		}
		cancelCheck()
		metaRes, err := backend.LoadMeta(context.Background(), ns, stateMetaKey)
		if err != nil {
			t.Fatalf("load state meta after commit: %v", err)
		}
		meta := metaRes.Meta
		if meta.Lease != nil {
			t.Fatalf("expected state lease cleared after commit")
		}
		return
	}

	checkCtx, cancelCheck := context.WithTimeout(context.Background(), 2*time.Second)
	if err := waitForQueueStateObject(checkCtx, backend, ns, stateObjKey, true); err != nil {
		cancelCheck()
		t.Fatalf("state object missing after rollback: %v", err)
	}
	cancelCheck()
	metaRes, err := backend.LoadMeta(context.Background(), ns, stateMetaKey)
	if err != nil {
		t.Fatalf("load state meta after rollback: %v", err)
	}
	meta := metaRes.Meta
	if meta.Lease != nil {
		t.Fatalf("expected state lease cleared after rollback")
	}

	probe := tc.Client
	if probe == nil {
		probe = cli
	}
	ctxRb, cancelRb := context.WithTimeout(context.Background(), 3*time.Second)
	rbMsg, err := probe.DequeueWithState(ctxRb, queueName, lockdclient.DequeueOptions{
		Owner:        owner + "-rb",
		BlockSeconds: 1,
	})
	cancelRb()
	if err != nil {
		t.Fatalf("rollback dequeue: %v", err)
	}
	if rbMsg == nil || rbMsg.MessageID() != msg.MessageID() {
		t.Fatalf("expected same message after rollback, got %+v", rbMsg)
	}
	if rbMsg.StateHandle() == nil {
		t.Fatalf("expected state handle on rollback dequeue")
	}
	if err := rbMsg.Ack(context.Background()); err != nil {
		t.Fatalf("ack rollback message: %v", err)
	}
}

// RunQueueTxnStatefulDecisionRestartScenario exercises stateful dequeue enlistment
// when the RM is restarted after a TC decision is recorded.
func RunQueueTxnStatefulDecisionRestartScenario(t testing.TB, tc, rm *lockd.TestServer, restartRM func(testing.TB) *lockd.TestServer, commit bool, replayDeadline time.Duration) {
	t.Helper()
	if tc == nil || rm == nil || restartRM == nil {
		t.Fatalf("tc/rm servers required")
	}
	cli := rm.Client
	if cli == nil {
		t.Fatalf("rm server missing client")
	}

	queueName := QueueName("txn-stateful-restart")
	owner := QueueOwner("txn-stateful-restart-owner")
	txnID := xid.New().String()

	MustEnqueueBytes(t, cli, queueName, []byte("txn-stateful-restart"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	msg, err := cli.DequeueWithState(ctx, queueName, lockdclient.DequeueOptions{
		Owner:        owner,
		TxnID:        txnID,
		BlockSeconds: 1,
	})
	cancel()
	if err != nil {
		t.Fatalf("stateful dequeue: %v", err)
	}
	if msg == nil {
		t.Fatalf("stateful dequeue returned nil message")
	}
	if got := msg.TxnID(); got != txnID {
		t.Fatalf("expected txn_id %s on dequeue, got %s", txnID, got)
	}
	if msg.StateHandle() == nil {
		t.Fatalf("expected state handle on stateful dequeue")
	}

	ns := msg.Namespace()
	if ns == "" {
		ns = namespaces.Default
	}
	messageKey, err := queue.MessageLeaseKey(ns, queueName, msg.MessageID())
	if err != nil {
		t.Fatalf("message lease key: %v", err)
	}
	stateKey, err := queue.StateLeaseKey(ns, queueName, msg.MessageID())
	if err != nil {
		t.Fatalf("state lease key: %v", err)
	}
	participantMessage := strings.TrimPrefix(messageKey, ns+"/")
	participantState := strings.TrimPrefix(stateKey, ns+"/")

	stopTestServer(rm)

	state := "commit"
	if !commit {
		state = "rollback"
	}
	payload := api.TxnDecisionRequest{
		TxnID: txnID,
		State: state,
		Participants: []api.TxnParticipant{
			{Namespace: ns, Key: participantMessage},
			{Namespace: ns, Key: participantState},
		},
	}
	postTxnDecision(t, tc, payload)
	stopTestServer(tc)

	rm2 := restartRM(t)
	if rm2 == nil || rm2.Client == nil {
		t.Fatalf("restart RM missing client")
	}
	cli2 := rm2.Client
	backend := rm2.Backend()
	if backend == nil {
		t.Fatalf("test server backend not available")
	}

	if replayDeadline <= 0 {
		replayDeadline = 20 * time.Second
	}

	stateObjKey := queueStateObjectKey(queueName, msg.MessageID())
	stateMetaKey := participantState

	assertQueueEmpty := func(client *lockdclient.Client, label string) {
		deadline := time.Now().Add(replayDeadline)
		for time.Now().Before(deadline) {
			ctxProbe, cancelProbe := context.WithTimeout(context.Background(), 500*time.Millisecond)
			m, err := client.Dequeue(ctxProbe, queueName, lockdclient.DequeueOptions{
				Owner:        QueueOwner("txn-probe-" + label),
				BlockSeconds: api.BlockNoWait,
			})
			cancelProbe()
			if err == nil && m != nil {
				t.Fatalf("expected queue empty after commit (%s), got message %s", label, m.MessageID())
			}
			var apiErr *lockdclient.APIError
			if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
				return
			}
			time.Sleep(75 * time.Millisecond)
		}
		t.Fatalf("message still visible after commit (%s)", label)
	}

	if commit {
		assertQueueEmpty(cli2, "rm")
		checkCtx, cancelCheck := context.WithTimeout(context.Background(), replayDeadline)
		if err := waitForQueueStateObject(checkCtx, backend, ns, stateObjKey, false); err != nil {
			cancelCheck()
			t.Fatalf("state object still present after commit: %v", err)
		}
		cancelCheck()
		metaRes, err := backend.LoadMeta(context.Background(), ns, stateMetaKey)
		if err != nil {
			t.Fatalf("load state meta after commit: %v", err)
		}
		meta := metaRes.Meta
		if meta.Lease != nil {
			t.Fatalf("expected state lease cleared after commit")
		}
		return
	}

	checkCtx, cancelCheck := context.WithTimeout(context.Background(), replayDeadline)
	if err := waitForQueueStateObject(checkCtx, backend, ns, stateObjKey, true); err != nil {
		cancelCheck()
		t.Fatalf("state object missing after rollback: %v", err)
	}
	cancelCheck()
	metaRes, err := backend.LoadMeta(context.Background(), ns, stateMetaKey)
	if err != nil {
		t.Fatalf("load state meta after rollback: %v", err)
	}
	meta := metaRes.Meta
	if meta.Lease != nil {
		t.Fatalf("expected state lease cleared after rollback")
	}

	deadline := time.Now().Add(replayDeadline)
	for time.Now().Before(deadline) {
		ctxTimeout := 5 * time.Second
		if remaining := time.Until(deadline); remaining < ctxTimeout {
			ctxTimeout = remaining
		}
		ctxRb, cancelRb := context.WithTimeout(context.Background(), ctxTimeout)
		rbMsg, err := cli2.DequeueWithState(ctxRb, queueName, lockdclient.DequeueOptions{
			Owner:        owner + "-rb",
			BlockSeconds: 1,
		})
		cancelRb()
		if err == nil && rbMsg != nil {
			if rbMsg.MessageID() != msg.MessageID() {
				t.Fatalf("expected same message after rollback, got %+v", rbMsg)
			}
			if rbMsg.StateHandle() == nil {
				t.Fatalf("expected state handle on rollback dequeue")
			}
			if err := rbMsg.Ack(context.Background()); err != nil {
				t.Fatalf("ack rollback message: %v", err)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("message not re-delivered after rollback")
}

func queueStateObjectKey(queueName, messageID string) string {
	return path.Join("q", queueName, "state", messageID+".json")
}

func waitForQueueStateObject(ctx context.Context, backend storage.Backend, namespace, stateKey string, shouldExist bool) error {
	for {
		obj, err := backend.GetObject(ctx, namespace, stateKey)
		if err == nil {
			_ = obj.Reader.Close()
			if shouldExist {
				return nil
			}
		} else if errors.Is(err, storage.ErrNotFound) {
			if !shouldExist {
				return nil
			}
		} else {
			return err
		}
		if ctx.Err() != nil {
			if shouldExist {
				return fmt.Errorf("state object still missing")
			}
			return fmt.Errorf("state object still present")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// RunQueueTxnMixedKeyScenario enlists two keys and a queue message in the same
// txn and asserts commit (writes + ACK) or rollback (no writes + NACK).
func RunQueueTxnMixedKeyScenario(t testing.TB, ts *lockd.TestServer, commit bool) {
	t.Helper()

	cli := ts.Client
	if cli == nil {
		t.Fatalf("test server missing client")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("txn-mixed-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("txn-mixed-b-%d", time.Now().UnixNano())

	leaseA, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "txn-mixed",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire keyA: %v", err)
	}
	txnID := leaseA.TxnID
	if txnID == "" {
		t.Fatalf("acquire keyA returned empty txn id")
	}
	if err := leaseA.Save(ctx, map[string]any{"value": "a"}); err != nil {
		t.Fatalf("save keyA: %v", err)
	}

	leaseB, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "txn-mixed",
		TTLSeconds: 30,
		TxnID:      txnID,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire keyB: %v", err)
	}
	if err := leaseB.Save(ctx, map[string]any{"value": "b"}); err != nil {
		t.Fatalf("save keyB: %v", err)
	}
	if backend := ts.Backend(); backend != nil {
		if metaRes, err := backend.LoadMeta(ctx, namespaces.Default, keyA); err == nil {
			metaA := metaRes.Meta
			t.Logf("meta keyA staged_txn=%q staged_etag=%q lease_txn=%v", metaA.StagedTxnID, metaA.StagedStateETag, metaA.Lease != nil && metaA.Lease.TxnID != "")
		}
		if metaRes, err := backend.LoadMeta(ctx, namespaces.Default, keyB); err == nil {
			metaB := metaRes.Meta
			t.Logf("meta keyB staged_txn=%q staged_etag=%q lease_txn=%v", metaB.StagedTxnID, metaB.StagedStateETag, metaB.Lease != nil && metaB.Lease.TxnID != "")
		}
	}

	queueName := QueueName("txn-mixed-queue")
	owner := QueueOwner("txn-mixed-owner")
	MustEnqueueBytes(t, cli, queueName, []byte("txn-mixed-payload"))
	msg := MustDequeueMessageTxn(t, cli, queueName, owner, txnID, 1, 5*time.Second)

	ns := msg.Namespace()
	if ns == "" {
		ns = namespaces.Default
	}
	namespacedKey, err := queue.MessageLeaseKey(ns, queueName, msg.MessageID())
	if err != nil {
		t.Fatalf("message lease key: %v", err)
	}
	participantQueueKey := strings.TrimPrefix(namespacedKey, ns+"/")

	payload := api.TxnDecisionRequest{
		TxnID: txnID,
		State: map[bool]string{true: "commit", false: "rollback"}[commit],
		Participants: []api.TxnParticipant{
			{Namespace: namespaces.Default, Key: keyA},
			{Namespace: namespaces.Default, Key: keyB},
			{Namespace: ns, Key: participantQueueKey},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal txn payload: %v", err)
	}
	httpClient := cryptotest.RequireTCHTTPClient(t, ts)
	req, err := http.NewRequest(http.MethodPost, ts.BaseURL+"/v1/txn/decide", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build txn request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("txn request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("txn %s unexpected status %d: %s", payload.State, resp.StatusCode, data)
	}

	if backend := ts.Backend(); backend != nil {
		if metaRes, err := backend.LoadMeta(ctx, namespaces.Default, keyA); err == nil {
			metaA := metaRes.Meta
			t.Logf("post-txn meta keyA staged_txn=%q staged_etag=%q state_etag=%q", metaA.StagedTxnID, metaA.StagedStateETag, metaA.StateETag)
		}
		if metaRes, err := backend.LoadMeta(ctx, namespaces.Default, keyB); err == nil {
			metaB := metaRes.Meta
			t.Logf("post-txn meta keyB staged_txn=%q staged_etag=%q state_etag=%q", metaB.StagedTxnID, metaB.StagedStateETag, metaB.StateETag)
		}
	}

	assertValue := func(key, leaseID, want string) {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			optsWithLease := []lockdclient.GetOption{
				lockdclient.WithGetNamespace(namespaces.Default),
				lockdclient.WithGetLeaseID(leaseID),
			}
			res, err := cli.Get(ctx, key, optsWithLease...)
			cancel()
			if err == nil && res != nil && res.HasState {
				doc, derr := res.Document()
				if derr == nil {
					if val, ok := doc.Body["value"].(string); ok && val == want {
						return
					}
					t.Logf("assertValue probe key=%s via lease got=%v want=%s", key, doc.Body["value"], want)
				} else {
					t.Logf("assertValue decode %s via lease: %v", key, derr)
				}
			}

			// Fallback to public read once lease is gone.
			ctxPub, cancelPub := context.WithTimeout(context.Background(), 500*time.Millisecond)
			resPub, errPub := cli.Get(ctxPub, key,
				lockdclient.WithGetNamespace(namespaces.Default),
				lockdclient.WithGetPublicDisabled(false),
			)
			cancelPub()
			if errPub == nil && resPub != nil && resPub.HasState {
				doc, derr := resPub.Document()
				if derr == nil {
					if val, ok := doc.Body["value"].(string); ok && val == want {
						return
					}
					t.Logf("assertValue probe key=%s via public got=%v want=%s", key, doc.Body["value"], want)
				} else {
					t.Logf("assertValue decode %s via public: %v", key, derr)
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("key %s missing expected value %s after %s", key, want, payload.State)
	}
	assertAbsent := func(key string) {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			res, err := cli.Get(ctx, key, lockdclient.WithGetNamespace(namespaces.Default), lockdclient.WithGetPublicDisabled(true))
			cancel()
			if err != nil {
				var apiErr *lockdclient.APIError
				if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound {
					return
				}
			}
			if res != nil && res.HasState {
				t.Fatalf("key %s still has state after rollback", key)
			}
			// try public read to confirm absence
			ctxPub, cancelPub := context.WithTimeout(context.Background(), 500*time.Millisecond)
			resPub, errPub := cli.Get(ctxPub, key, lockdclient.WithGetNamespace(namespaces.Default), lockdclient.WithGetPublicDisabled(false))
			cancelPub()
			if errPub != nil {
				var apiErr *lockdclient.APIError
				if errors.As(errPub, &apiErr) && apiErr.Status == http.StatusNotFound {
					return
				}
			} else if resPub == nil || !resPub.HasState {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	if commit {
		assertValue(keyA, leaseA.LeaseID, "a")
		assertValue(keyB, leaseB.LeaseID, "b")
		if err := leaseA.Release(context.Background()); err != nil {
			t.Fatalf("release keyA after commit: %v", err)
		}
		if err := leaseB.Release(context.Background()); err != nil {
			t.Fatalf("release keyB after commit: %v", err)
		}
		EnsureQueueEmpty(t, cli, queueName)
	} else {
		if err := leaseA.ReleaseWithOptions(context.Background(), lockdclient.ReleaseOptions{Rollback: true}); err != nil {
			t.Fatalf("release keyA rollback: %v", err)
		}
		if err := leaseB.ReleaseWithOptions(context.Background(), lockdclient.ReleaseOptions{Rollback: true}); err != nil {
			t.Fatalf("release keyB rollback: %v", err)
		}
		assertAbsent(keyA)
		assertAbsent(keyB)
		ctxRb, cancelRb := context.WithTimeout(context.Background(), 4*time.Second)
		msg2, err := cli.Dequeue(ctxRb, queueName, lockdclient.DequeueOptions{
			Owner:        owner + "-rb",
			BlockSeconds: 1,
		})
		cancelRb()
		if err != nil {
			t.Fatalf("rollback dequeue: %v", err)
		}
		if msg2 == nil || msg2.MessageID() != msg.MessageID() {
			t.Fatalf("expected same message after rollback, got %+v", msg2)
		}
		if err := msg2.Ack(context.Background()); err != nil {
			t.Fatalf("ack rollback message: %v", err)
		}
	}
}

// RunQueueTxnMixedKeyFanoutScenario enlists two keys and a queue message in the same
// txn, issuing the decision via a TC that fans out to an RM endpoint.
func RunQueueTxnMixedKeyFanoutScenario(t testing.TB, tc, rm *lockd.TestServer, commit bool) {
	t.Helper()
	if tc == nil || rm == nil {
		t.Fatalf("tc/rm servers required")
	}
	tcClient := tc.Client
	rmClient := rm.Client
	if tcClient == nil || rmClient == nil {
		t.Fatalf("missing tc/rm client")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("txn-mixed-fanout-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("txn-mixed-fanout-b-%d", time.Now().UnixNano())

	leaseA, err := tcClient.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "txn-mixed-fanout",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire keyA: %v", err)
	}
	txnID := leaseA.TxnID
	if txnID == "" {
		t.Fatalf("acquire keyA returned empty txn id")
	}
	if err := leaseA.Save(ctx, map[string]any{"value": "a"}); err != nil {
		t.Fatalf("save keyA: %v", err)
	}

	leaseB, err := rmClient.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "txn-mixed-fanout",
		TTLSeconds: 30,
		TxnID:      txnID,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire keyB: %v", err)
	}
	if err := leaseB.Save(ctx, map[string]any{"value": "b"}); err != nil {
		t.Fatalf("save keyB: %v", err)
	}

	queueName := QueueName("txn-mixed-fanout-queue")
	owner := QueueOwner("txn-mixed-fanout-owner")
	MustEnqueueBytes(t, rmClient, queueName, []byte("txn-mixed-fanout-payload"))
	msg := MustDequeueMessageTxn(t, rmClient, queueName, owner, txnID, 1, 5*time.Second)

	ns := msg.Namespace()
	if ns == "" {
		ns = namespaces.Default
	}
	namespacedKey, err := queue.MessageLeaseKey(ns, queueName, msg.MessageID())
	if err != nil {
		t.Fatalf("message lease key: %v", err)
	}
	participantQueueKey := strings.TrimPrefix(namespacedKey, ns+"/")

	payload := api.TxnDecisionRequest{
		TxnID: txnID,
		State: map[bool]string{true: "commit", false: "rollback"}[commit],
		Participants: []api.TxnParticipant{
			{Namespace: namespaces.Default, Key: keyA},
			{Namespace: namespaces.Default, Key: keyB},
			{Namespace: ns, Key: participantQueueKey},
		},
	}
	postTxnDecision(t, tc, payload)

	assertValue := func(client *lockdclient.Client, key, leaseID, want string) {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			optsWithLease := []lockdclient.GetOption{
				lockdclient.WithGetNamespace(namespaces.Default),
				lockdclient.WithGetLeaseID(leaseID),
			}
			res, err := client.Get(ctx, key, optsWithLease...)
			cancel()
			if err == nil && res != nil && res.HasState {
				doc, derr := res.Document()
				if derr == nil {
					if val, ok := doc.Body["value"].(string); ok && val == want {
						return
					}
					t.Logf("assertValue probe key=%s via lease got=%v want=%s", key, doc.Body["value"], want)
				} else {
					t.Logf("assertValue decode %s via lease: %v", key, derr)
				}
			}

			ctxPub, cancelPub := context.WithTimeout(context.Background(), 500*time.Millisecond)
			resPub, errPub := client.Get(ctxPub, key,
				lockdclient.WithGetNamespace(namespaces.Default),
				lockdclient.WithGetPublicDisabled(false),
			)
			cancelPub()
			if errPub == nil && resPub != nil && resPub.HasState {
				doc, derr := resPub.Document()
				if derr == nil {
					if val, ok := doc.Body["value"].(string); ok && val == want {
						return
					}
					t.Logf("assertValue probe key=%s via public got=%v want=%s", key, doc.Body["value"], want)
				} else {
					t.Logf("assertValue decode %s via public: %v", key, derr)
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("key %s missing expected value %s after %s", key, want, payload.State)
	}
	assertAbsent := func(client *lockdclient.Client, key string) {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			res, err := client.Get(ctx, key, lockdclient.WithGetNamespace(namespaces.Default), lockdclient.WithGetPublicDisabled(true))
			cancel()
			if err != nil {
				var apiErr *lockdclient.APIError
				if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound {
					return
				}
			}
			if res != nil && res.HasState {
				t.Fatalf("key %s still has state after rollback", key)
			}
			ctxPub, cancelPub := context.WithTimeout(context.Background(), 500*time.Millisecond)
			resPub, errPub := client.Get(ctxPub, key, lockdclient.WithGetNamespace(namespaces.Default), lockdclient.WithGetPublicDisabled(false))
			cancelPub()
			if errPub != nil {
				var apiErr *lockdclient.APIError
				if errors.As(errPub, &apiErr) && apiErr.Status == http.StatusNotFound {
					return
				}
			} else if resPub == nil || !resPub.HasState {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	if commit {
		assertValue(tcClient, keyA, leaseA.LeaseID, "a")
		assertValue(rmClient, keyB, leaseB.LeaseID, "b")
		if err := leaseA.Release(context.Background()); err != nil {
			t.Fatalf("release keyA after commit: %v", err)
		}
		if err := leaseB.Release(context.Background()); err != nil {
			t.Fatalf("release keyB after commit: %v", err)
		}
		EnsureQueueEmpty(t, tcClient, queueName)
		EnsureQueueEmpty(t, rmClient, queueName)
	} else {
		if err := leaseA.ReleaseWithOptions(context.Background(), lockdclient.ReleaseOptions{Rollback: true}); err != nil {
			t.Fatalf("release keyA rollback: %v", err)
		}
		if err := leaseB.ReleaseWithOptions(context.Background(), lockdclient.ReleaseOptions{Rollback: true}); err != nil {
			t.Fatalf("release keyB rollback: %v", err)
		}
		assertAbsent(tcClient, keyA)
		assertAbsent(rmClient, keyB)
		ctxRb, cancelRb := context.WithTimeout(context.Background(), 4*time.Second)
		msg2, err := tcClient.Dequeue(ctxRb, queueName, lockdclient.DequeueOptions{
			Owner:        owner + "-rb",
			BlockSeconds: 1,
		})
		cancelRb()
		if err != nil {
			t.Fatalf("rollback dequeue: %v", err)
		}
		if msg2 == nil || msg2.MessageID() != msg.MessageID() {
			t.Fatalf("expected same message after rollback, got %+v", msg2)
		}
		if err := msg2.Ack(context.Background()); err != nil {
			t.Fatalf("ack rollback message: %v", err)
		}
	}
}

// RunQueueTxnMixedKeyRestartScenario enlists two keys and a queue message in the same
// txn, then restarts the RM after the TC records the decision.
func RunQueueTxnMixedKeyRestartScenario(t testing.TB, tc, rm *lockd.TestServer, restartRM func(testing.TB) *lockd.TestServer, commit bool, replayDeadline time.Duration) {
	t.Helper()
	if tc == nil || rm == nil || restartRM == nil {
		t.Fatalf("tc/rm servers required")
	}
	tcClient := tc.Client
	rmClient := rm.Client
	if tcClient == nil || rmClient == nil {
		t.Fatalf("missing tc/rm client")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("txn-mixed-restart-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("txn-mixed-restart-b-%d", time.Now().UnixNano())

	leaseA, err := tcClient.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "txn-mixed-restart",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire keyA: %v", err)
	}
	txnID := leaseA.TxnID
	if txnID == "" {
		t.Fatalf("acquire keyA returned empty txn id")
	}
	if err := leaseA.Save(ctx, map[string]any{"value": "a"}); err != nil {
		t.Fatalf("save keyA: %v", err)
	}

	leaseB, err := rmClient.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "txn-mixed-restart",
		TTLSeconds: 30,
		TxnID:      txnID,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire keyB: %v", err)
	}
	if err := leaseB.Save(ctx, map[string]any{"value": "b"}); err != nil {
		t.Fatalf("save keyB: %v", err)
	}

	queueName := QueueName("txn-mixed-restart-queue")
	owner := QueueOwner("txn-mixed-restart-owner")
	MustEnqueueBytes(t, rmClient, queueName, []byte("txn-mixed-restart-payload"))
	msg := MustDequeueMessageTxn(t, rmClient, queueName, owner, txnID, 1, 5*time.Second)

	ns := msg.Namespace()
	if ns == "" {
		ns = namespaces.Default
	}
	namespacedKey, err := queue.MessageLeaseKey(ns, queueName, msg.MessageID())
	if err != nil {
		t.Fatalf("message lease key: %v", err)
	}
	participantQueueKey := strings.TrimPrefix(namespacedKey, ns+"/")

	stopTestServer(rm)

	payload := api.TxnDecisionRequest{
		TxnID: txnID,
		State: map[bool]string{true: "commit", false: "rollback"}[commit],
		Participants: []api.TxnParticipant{
			{Namespace: namespaces.Default, Key: keyA},
			{Namespace: namespaces.Default, Key: keyB},
			{Namespace: ns, Key: participantQueueKey},
		},
	}
	postTxnDecision(t, tc, payload)
	stopTestServer(tc)

	rm2 := restartRM(t)
	if rm2 == nil || rm2.Client == nil {
		t.Fatalf("restart RM missing client")
	}
	cli2 := rm2.Client

	if replayDeadline <= 0 {
		replayDeadline = 20 * time.Second
	}

	assertValue := func(client *lockdclient.Client, key, leaseID, want string) {
		deadline := time.Now().Add(replayDeadline)
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			optsWithLease := []lockdclient.GetOption{
				lockdclient.WithGetNamespace(namespaces.Default),
				lockdclient.WithGetLeaseID(leaseID),
			}
			res, err := client.Get(ctx, key, optsWithLease...)
			cancel()
			if err == nil && res != nil && res.HasState {
				doc, derr := res.Document()
				if derr == nil {
					if val, ok := doc.Body["value"].(string); ok && val == want {
						return
					}
					t.Logf("assertValue probe key=%s via lease got=%v want=%s", key, doc.Body["value"], want)
				} else {
					t.Logf("assertValue decode %s via lease: %v", key, derr)
				}
			}

			ctxPub, cancelPub := context.WithTimeout(context.Background(), 500*time.Millisecond)
			resPub, errPub := client.Get(ctxPub, key,
				lockdclient.WithGetNamespace(namespaces.Default),
				lockdclient.WithGetPublicDisabled(false),
			)
			cancelPub()
			if errPub == nil && resPub != nil && resPub.HasState {
				doc, derr := resPub.Document()
				if derr == nil {
					if val, ok := doc.Body["value"].(string); ok && val == want {
						return
					}
					t.Logf("assertValue probe key=%s via public got=%v want=%s", key, doc.Body["value"], want)
				} else {
					t.Logf("assertValue decode %s via public: %v", key, derr)
				}
			}
			time.Sleep(75 * time.Millisecond)
		}
		t.Fatalf("key %s missing expected value %s after %s", key, want, payload.State)
	}
	assertAbsent := func(client *lockdclient.Client, key string) {
		deadline := time.Now().Add(replayDeadline)
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			res, err := client.Get(ctx, key, lockdclient.WithGetNamespace(namespaces.Default), lockdclient.WithGetPublicDisabled(true))
			cancel()
			if err != nil {
				var apiErr *lockdclient.APIError
				if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound {
					return
				}
			}
			if res != nil && res.HasState {
				t.Fatalf("key %s still has state after rollback", key)
			}
			ctxPub, cancelPub := context.WithTimeout(context.Background(), 500*time.Millisecond)
			resPub, errPub := client.Get(ctxPub, key, lockdclient.WithGetNamespace(namespaces.Default), lockdclient.WithGetPublicDisabled(false))
			cancelPub()
			if errPub != nil {
				var apiErr *lockdclient.APIError
				if errors.As(errPub, &apiErr) && apiErr.Status == http.StatusNotFound {
					return
				}
			} else if resPub == nil || !resPub.HasState {
				return
			}
			time.Sleep(75 * time.Millisecond)
		}
	}

	if commit {
		assertValue(cli2, keyA, leaseA.LeaseID, "a")
		assertValue(cli2, keyB, leaseB.LeaseID, "b")
		deadline := time.Now().Add(replayDeadline)
		for time.Now().Before(deadline) {
			ctxProbe, cancelProbe := context.WithTimeout(context.Background(), 500*time.Millisecond)
			m, err := cli2.Dequeue(ctxProbe, queueName, lockdclient.DequeueOptions{
				Owner:        QueueOwner("txn-probe"),
				BlockSeconds: api.BlockNoWait,
			})
			cancelProbe()
			if err != nil {
				var apiErr *lockdclient.APIError
				if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
					return
				}
			}
			if m != nil {
				t.Fatalf("expected queue empty after commit, got message %s", m.MessageID())
			}
			time.Sleep(75 * time.Millisecond)
		}
		t.Fatalf("queue still non-empty after commit")
		return
	}

	assertAbsent(cli2, keyA)
	assertAbsent(cli2, keyB)
	deadline := time.Now().Add(replayDeadline)
	for time.Now().Before(deadline) {
		ctxTimeout := 5 * time.Second
		if remaining := time.Until(deadline); remaining < ctxTimeout {
			ctxTimeout = remaining
		}
		ctxRb, cancelRb := context.WithTimeout(context.Background(), ctxTimeout)
		msg2, err := cli2.Dequeue(ctxRb, queueName, lockdclient.DequeueOptions{
			Owner:        owner + "-rb",
			BlockSeconds: 1,
		})
		cancelRb()
		if err == nil && msg2 != nil {
			if msg2.MessageID() != msg.MessageID() {
				t.Fatalf("expected same message after rollback, got %+v", msg2)
			}
			if err := msg2.Ack(context.Background()); err != nil {
				t.Fatalf("ack rollback message: %v", err)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("message not re-delivered after rollback")
}

// RunQueueTxnReplayScenario seeds a txn record on the backend, restarts the
// server with the same backend, and asserts the decision is replayed to the
// queue message.
func RunQueueTxnReplayScenario(t testing.TB, cfg lockd.Config, start func(testing.TB, lockd.Config, ...lockd.TestServerOption) *lockd.TestServer, commit bool) {
	t.Helper()

	ts := start(t, cfg)
	backend := ts.Backend()
	if backend == nil {
		t.Fatalf("test server backend not available")
	}
	cli := ts.Client
	if cli == nil {
		t.Fatalf("test server missing client")
	}

	queueName := QueueName("txn-replay")
	owner := QueueOwner("txn-replay")
	txnID := xid.New().String()

	MustEnqueueBytes(t, cli, queueName, []byte("txn-replay"))
	msg := MustDequeueMessageTxn(t, cli, queueName, owner, txnID, 1, 5*time.Second)

	ns := msg.Namespace()
	if ns == "" {
		ns = namespaces.Default
	}
	namespacedKey, err := queue.MessageLeaseKey(ns, queueName, msg.MessageID())
	if err != nil {
		t.Fatalf("message lease key: %v", err)
	}
	participantKey := strings.TrimPrefix(namespacedKey, ns+"/")

	now := time.Now()
	rec := core.TxnRecord{
		TxnID: txnID,
		State: map[bool]core.TxnState{true: core.TxnStateCommit, false: core.TxnStateRollback}[commit],
		Participants: []core.TxnParticipant{{
			Namespace: ns,
			Key:       participantKey,
		}},
		ExpiresAtUnix: now.Add(time.Minute).Unix(),
		UpdatedAtUnix: now.Unix(),
		CreatedAtUnix: now.Unix(),
	}
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(rec); err != nil {
		t.Fatalf("encode txn record: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := backend.PutObject(ctx, ".txns", rec.TxnID, bytes.NewReader(buf.Bytes()), storage.PutObjectOptions{ContentType: storage.ContentTypeJSON}); err != nil {
		cancel()
		t.Fatalf("persist txn record: %v", err)
	}
	cancel()

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = ts.Stop(stopCtx)
	stopCancel()

	ts2 := start(t, cfg, lockd.WithTestBackend(backend))
	defer func() {
		ctxStop, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
		_ = ts2.Stop(ctxStop)
		cancelStop()
	}()
	cli2 := ts2.Client
	if cli2 == nil {
		t.Fatalf("restart missing client")
	}

	replayDeadline := replayDeadlineForStore(cfg.Store)
	if commit {
		deadline := time.Now().Add(replayDeadline)
		for time.Now().Before(deadline) {
			ctxProbe, cancelProbe := context.WithTimeout(context.Background(), 500*time.Millisecond)
			msgProbe, err := cli2.Dequeue(ctxProbe, queueName, lockdclient.DequeueOptions{
				Owner:        owner + "-probe",
				BlockSeconds: api.BlockNoWait,
			})
			cancelProbe()
			if err != nil {
				var apiErr *lockdclient.APIError
				if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
					return
				}
			}
			if msgProbe != nil {
				t.Fatalf("message %s still visible after replay commit", msgProbe.MessageID())
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("queue still non-empty after replay commit")
		return
	}

	deadline := time.Now().Add(replayDeadline)
	for time.Now().Before(deadline) {
		ctxTimeout := 5 * time.Second
		if remaining := time.Until(deadline); remaining < ctxTimeout {
			ctxTimeout = remaining
		}
		ctxRb, cancelRb := context.WithTimeout(context.Background(), ctxTimeout)
		msgRb, err := cli2.Dequeue(ctxRb, queueName, lockdclient.DequeueOptions{
			Owner:        owner + "-rb",
			BlockSeconds: 2,
		})
		cancelRb()
		if err == nil && msgRb != nil {
			if msgRb.MessageID() != msg.MessageID() {
				t.Fatalf("expected same message after replay rollback; want %s got %s", msg.MessageID(), msgRb.MessageID())
			}
			if err := msgRb.Ack(context.Background()); err != nil {
				t.Fatalf("ack after replay rollback: %v", err)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("message not re-delivered after replay rollback")
}

// ReadMessagePayload reads and closes the payload, failing the test on error.
func ReadMessagePayload(t testing.TB, msg *lockdclient.QueueMessage) []byte {
	t.Helper()
	if msg == nil {
		t.Fatalf("read payload: nil message")
	}
	data, err := io.ReadAll(msg)
	if err != nil {
		t.Fatalf("read payload %s: %v", msg.MessageID(), err)
	}
	if err := msg.ClosePayload(); err != nil {
		t.Fatalf("close payload %s: %v", msg.MessageID(), err)
	}
	return data
}

// EnsureQueueEmpty asserts the queue has no immediately available messages.
func EnsureQueueEmpty(t testing.TB, cli *lockdclient.Client, queue string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	msg, err := cli.Dequeue(ctx, queue, lockdclient.DequeueOptions{
		Owner:        QueueOwner("probe"),
		BlockSeconds: lockdclient.BlockNoWait,
	})
	cancel()
	if err != nil {
		var apiErr *lockdclient.APIError
		if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
			return
		}
		t.Fatalf("probe dequeue %s: %v", queue, err)
	}
	if msg == nil || msg.MessageID() == "" {
		return
	}
	_ = msg.Nack(context.Background(), 0, map[string]any{"cleanup": true})
	t.Fatalf("expected queue %s empty, found message %s", queue, msg.MessageID())
}

// RunQueueAckScenario enqueues, dequeues, validates, and acks a message.
func RunQueueAckScenario(t *testing.T, cli *lockdclient.Client, queue string, payload []byte) {
	t.Helper()
	_ = MustEnqueueBytes(t, cli, queue, payload)
	owner := QueueOwner("consumer")
	msg := MustDequeueMessage(t, cli, queue, owner, 5, 10*time.Second)
	body := ReadMessagePayload(t, msg)
	if !bytes.Equal(body, payload) {
		t.Fatalf("unexpected payload for %s: got %q want %q", msg.MessageID(), string(body), string(payload))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := msg.Ack(ctx); err != nil {
		t.Fatalf("ack %s: %v", msg.MessageID(), err)
	}
	EnsureQueueEmpty(t, cli, queue)
}

// RunQueueNackScenario verifies nack reschedules for another consumer.
func RunQueueNackScenario(t *testing.T, cli *lockdclient.Client, queue string, payload []byte) {
	t.Helper()
	_ = MustEnqueueBytes(t, cli, queue, payload)
	ownerA := QueueOwner("consumer-a")
	msg := MustDequeueMessage(t, cli, queue, ownerA, 5, 10*time.Second)
	body := ReadMessagePayload(t, msg)
	if !bytes.Equal(body, payload) {
		t.Fatalf("unexpected payload for %s: got %q want %q", msg.MessageID(), string(body), string(payload))
	}
	firstAttempts := msg.Attempts()
	if firstAttempts < 1 {
		t.Fatalf("expected attempts >=1, got %d", firstAttempts)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := msg.Nack(ctx, 0, map[string]any{"reason": "integration-nack"})
	cancel()
	if err != nil {
		t.Fatalf("nack %s: %v", msg.MessageID(), err)
	}
	ownerB := QueueOwner("consumer-b")
	msg2 := MustDequeueMessage(t, cli, queue, ownerB, 5, 12*time.Second)
	body2 := ReadMessagePayload(t, msg2)
	if !bytes.Equal(body2, payload) {
		t.Fatalf("unexpected payload after nack for %s: got %q want %q", msg2.MessageID(), string(body2), string(payload))
	}
	if attempts := msg2.Attempts(); attempts <= firstAttempts {
		t.Fatalf("expected attempts to increase after nack, was %d now %d", firstAttempts, attempts)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	if err := msg2.Ack(ctx2); err != nil {
		t.Fatalf("ack after nack %s: %v", msg2.MessageID(), err)
	}
	EnsureQueueEmpty(t, cli, queue)
}

// LogCapture collects log lines for assertions while still emitting via testing logger.
type LogCapture struct {
	t        testing.TB
	mu       sync.Mutex
	closed   bool
	lines    []string
	max      int
	prefixes []string
	level    pslog.Level
	toTest   bool
	dropped  bool
}

// NewLogCapture returns a capture sink bound to the provided test.
func NewLogCapture(t testing.TB) *LogCapture {
	return NewLogCaptureWithOptions(t, LogCaptureOptions{})
}

// LogCaptureOptions customises capture behaviour.
type LogCaptureOptions struct {
	MaxEntries   int
	Prefixes     []string
	LogLevel     pslog.Level
	LogToTesting *bool
}

// NewLogCaptureWithOptions builds a capture with custom options.
func NewLogCaptureWithOptions(t testing.TB, opts LogCaptureOptions) *LogCapture {
	t.Helper()
	cap := &LogCapture{
		t:        t,
		max:      opts.MaxEntries,
		prefixes: append([]string(nil), opts.Prefixes...),
		level:    opts.LogLevel,
		toTest:   true,
	}
	if opts.LogToTesting != nil {
		cap.toTest = *opts.LogToTesting
	}
	if cap.level == 0 {
		cap.level = pslog.TraceLevel
	}
	t.Cleanup(cap.close)
	return cap
}

// Write implements io.Writer for logging adapters.
func (c *LogCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return len(p), nil
	}
	segments := bytes.Split(p, []byte{'\n'})
	for _, seg := range segments {
		if len(seg) == 0 {
			continue
		}
		entry := string(seg)
		if len(c.prefixes) > 0 {
			match := false
			for _, pref := range c.prefixes {
				if strings.Contains(entry, pref) {
					match = true
					break
				}
			}
			if !match {
				if c.toTest {
					c.t.Log(entry)
				}
				continue
			}
		}
		if c.max > 0 && len(c.lines) >= c.max {
			if !c.dropped {
				c.dropped = true
				if c.toTest {
					c.t.Log("log capture capacity reached; suppressing additional entries")
				}
			}
			continue
		}
		c.lines = append(c.lines, entry)
		if c.toTest {
			c.t.Log(entry)
		}
	}
	return len(p), nil
}

func (c *LogCapture) close() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
}

// Len returns the count of captured log lines so far.
func (c *LogCapture) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.lines)
}

// CountSince counts captured log lines containing substr starting at index.
func (c *LogCapture) CountSince(idx int, substr string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx < 0 {
		idx = 0
	}
	if idx > len(c.lines) {
		idx = len(c.lines)
	}
	count := 0
	for _, line := range c.lines[idx:] {
		if strings.Contains(line, substr) {
			count++
		}
	}
	return count
}

// LinesSince returns a copy of captured log lines starting at index.
func (c *LogCapture) LinesSince(idx int) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx < 0 {
		idx = 0
	}
	if idx > len(c.lines) {
		idx = len(c.lines)
	}
	out := make([]string, len(c.lines[idx:]))
	copy(out, c.lines[idx:])
	return out
}

// Logger returns a pslog logger that emits through the capture.
func (c *LogCapture) Logger() pslog.Logger {
	level := c.level
	if level == 0 {
		level = pslog.TraceLevel
	}
	logger := pslog.NewStructured(c)
	return logger.LogLevel(level)
}

func uniqueSuffix() string {
	return strings.ReplaceAll(uuidv7.NewString(), "-", "")
}

// ExtractPollTimesSince returns timestamps for queue.dispatcher.poll.begin entries for the given queue at or after 'since'.
func ExtractPollTimesSince(t testing.TB, capture *LogCapture, queue string, since time.Time) []time.Time {
	t.Helper()
	lines := capture.LinesSince(0)
	if len(lines) == 0 {
		return nil
	}
	var times []time.Time
	queueKey := fmt.Sprintf("\"queue\":\"%s\"", queue)
	for _, line := range lines {
		if !strings.Contains(line, queueKey) || !strings.Contains(line, `"msg":"queue.dispatcher.poll.begin"`) {
			continue
		}
		var entry struct {
			TS    string `json:"ts"`
			Msg   string `json:"msg"`
			Queue string `json:"queue"`
		}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if entry.Queue != queue || entry.Msg != "queue.dispatcher.poll.begin" || entry.TS == "" {
			continue
		}
		ts, err := time.Parse(time.RFC3339Nano, entry.TS)
		if err != nil {
			ts, err = time.Parse(time.RFC3339, entry.TS)
			if err != nil {
				continue
			}
		}
		if ts.Before(since) {
			continue
		}
		times = append(times, ts)
	}
	return times
}

// ExtractListObjectTimesSince returns timestamps for storage.list_objects.begin entries referencing the queue prefix at or after 'since'.
func ExtractListObjectTimesSince(t testing.TB, capture *LogCapture, queue string, since time.Time) []time.Time {
	t.Helper()
	lines := capture.LinesSince(0)
	if len(lines) == 0 {
		return nil
	}
	target := "/" + queue + "/"
	var times []time.Time
	seen := make(map[string]struct{})
	for _, line := range lines {
		if !strings.Contains(line, "storage.list_objects.begin") {
			continue
		}
		var entry struct {
			TS     string `json:"ts"`
			Msg    string `json:"msg"`
			Prefix string `json:"prefix"`
			Key    string `json:"key"`
			ReqID  string `json:"req_id"`
		}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if entry.Msg != "storage.list_objects.begin" {
			continue
		}
		match := false
		if entry.Prefix != "" && strings.Contains(entry.Prefix, target) {
			match = true
		}
		if !match && entry.Key != "" && strings.Contains(entry.Key, target) {
			match = true
		}
		if !match {
			continue
		}
		if entry.ReqID != "" {
			if _, ok := seen[entry.ReqID]; ok {
				continue
			}
			seen[entry.ReqID] = struct{}{}
		}
		ts, err := time.Parse(time.RFC3339Nano, entry.TS)
		if err != nil {
			ts, err = time.Parse(time.RFC3339, entry.TS)
			if err != nil {
				continue
			}
		}
		if ts.Before(since) {
			continue
		}
		times = append(times, ts)
	}
	return times
}

// VerifyPollIntervalRespect exercises dequeue/enqueue behaviour and asserts the dispatcher never polls faster than the interval.
func VerifyPollIntervalRespect(t testing.TB, capture *LogCapture, cli *lockdclient.Client, queue, owner string, interval time.Duration) {
	t.Helper()

	startDequeue := func() (<-chan *lockdclient.QueueMessage, <-chan error) {
		msgCh := make(chan *lockdclient.QueueMessage, 1)
		errCh := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			msg, err := cli.Dequeue(ctx, queue, lockdclient.DequeueOptions{
				Owner:        owner,
				BlockSeconds: 5,
			})
			if err != nil {
				errCh <- err
				return
			}
			msgCh <- msg
		}()
		return msgCh, errCh
	}

	awaitMessage := func(msgCh <-chan *lockdclient.QueueMessage, errCh <-chan error) *lockdclient.QueueMessage {
		select {
		case msg := <-msgCh:
			return msg
		case err := <-errCh:
			t.Fatalf("dequeue error: %v", err)
		case <-time.After(12 * time.Second):
			t.Fatalf("timeout waiting for dequeue result")
		}
		return nil
	}

	msgCh1, errCh1 := startDequeue()
	time.Sleep(150 * time.Millisecond) // ensure the request is inflight before enqueue
	MustEnqueueBytes(t, cli, queue, []byte("poll-interval-first"))
	msg1 := awaitMessage(msgCh1, errCh1)
	_ = ReadMessagePayload(t, msg1)
	ackCtx1, ackCancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	if err := msg1.Ack(ackCtx1); err != nil {
		ackCancel1()
		t.Fatalf("ack first message: %v", err)
	}
	ackCancel1()

	msgCh2, errCh2 := startDequeue()
	waitBeforeSecond := interval / 3
	if waitBeforeSecond < 200*time.Millisecond {
		waitBeforeSecond = 200 * time.Millisecond
	}
	if waitBeforeSecond > interval-200*time.Millisecond {
		waitBeforeSecond = interval / 2
	}
	time.Sleep(waitBeforeSecond)
	MustEnqueueBytes(t, cli, queue, []byte("poll-interval-second"))
	msg2 := awaitMessage(msgCh2, errCh2)
	_ = ReadMessagePayload(t, msg2)
	ackCtx2, ackCancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	if err := msg2.Ack(ackCtx2); err != nil {
		ackCancel2()
		t.Fatalf("ack second message: %v", err)
	}
	ackCancel2()
	secondAckTime := time.Now()

	waitCtx, waitCancel := context.WithCancel(context.Background())
	waitErrCh := make(chan error, 1)
	go func() {
		_, err := cli.Dequeue(waitCtx, queue, lockdclient.DequeueOptions{
			Owner:        owner,
			BlockSeconds: 0,
		})
		waitErrCh <- err
	}()
	waitDuration := 3*interval + time.Second
	time.Sleep(waitDuration)
	waitCancel()
	select {
	case err := <-waitErrCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			var apiErr *lockdclient.APIError
			if !errors.As(err, &apiErr) || apiErr.Response.ErrorCode != "waiting" {
				t.Fatalf("waiting dequeue returned unexpected error: %v", err)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for idle dequeue cancellation")
	}

	EnsureQueueEmpty(t, cli, queue)

	time.Sleep(100 * time.Millisecond)

	startWindow := secondAckTime.Add(interval / 2)
	listTimes := ExtractListObjectTimesSince(t, capture, queue, startWindow)
	seriesName := "storage.list_objects.begin"
	if len(listTimes) < 2 {
		listTimes = ExtractPollTimesSince(t, capture, queue, startWindow)
		seriesName = "queue.dispatcher.poll.begin"
	}
	if len(listTimes) < 2 {
		lines := capture.LinesSince(0)
		t.Fatalf("expected at least two %s entries for queue %s, captured %d\nlogs:\n%s", seriesName, queue, len(listTimes), strings.Join(lines, "\n"))
	}
	minGap := time.Duration(1<<63 - 1)
	for i := 1; i < len(listTimes); i++ {
		gap := listTimes[i].Sub(listTimes[i-1])
		if gap < minGap {
			minGap = gap
		}
	}
	slack := 100 * time.Millisecond
	if minGap+slack < interval {
		lines := capture.LinesSince(0)
		t.Fatalf("%s events closer than %s (min gap %s, slack %s)\nlogs:\n%s", seriesName, interval, minGap, slack, strings.Join(lines, "\n"))
	}
}
