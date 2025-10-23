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
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/logport"
	"pkt.systems/logport/adapters/psl"
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

// StartQueueTestServer launches a lockd test server with helpful defaults.
func StartQueueTestServer(t testing.TB, cfg lockd.Config, extraClientOpts ...lockdclient.Option) *lockd.TestServer {
	t.Helper()
	serverLogger := lockd.NewTestingLogger(t, logport.TraceLevel)
	return StartQueueTestServerWithLogger(t, cfg, serverLogger, extraClientOpts...)
}

// StartQueueTestServerWithLogger launches a test server with a custom logger.
func StartQueueTestServerWithLogger(t testing.TB, cfg lockd.Config, logger logport.ForLogging, extraClientOpts ...lockdclient.Option) *lockd.TestServer {
	t.Helper()

	clientLogger := lockd.NewTestingLogger(t, logport.TraceLevel)
	baseClientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(60 * time.Second),
		lockdclient.WithKeepAliveTimeout(60 * time.Second),
		lockdclient.WithCloseTimeout(60 * time.Second),
		lockdclient.WithLogger(clientLogger),
	}
	if len(extraClientOpts) > 0 {
		baseClientOpts = append(baseClientOpts, extraClientOpts...)
	}

	return lockd.StartTestServer(t,
		lockd.WithTestConfig(cfg),
		lockd.WithTestLogger(logger),
		lockd.WithTestClientOptions(baseClientOpts...),
	)
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
	level    logport.Level
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
	LogLevel     logport.Level
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
		cap.level = logport.TraceLevel
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

// Logger returns a logport logger that emits through the capture.
func (c *LogCapture) Logger() logport.ForLogging {
	level := c.level
	if level == 0 {
		level = logport.TraceLevel
	}
	logger := psl.NewStructured(c).WithLogLevel().LogLevel(level)
	return logger.With("app", "testserver")
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
