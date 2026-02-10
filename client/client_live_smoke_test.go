package client_test

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	"pkt.systems/lockd/client"
)

func TestClientLiveSmoke(t *testing.T) {
	ts := lockd.StartTestServer(t)
	cli := ts.Client
	if cli == nil {
		t.Fatalf("test server did not provide client")
	}

	unique := func(prefix string) string {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}

	t.Run("AcquireAndAttachments", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		key := unique("live-smoke-lease")
		session, err := cli.Acquire(ctx, api.AcquireRequest{
			Key:        key,
			Owner:      "live-smoke-acquire",
			TTLSeconds: 30,
		})
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		defer session.Release(ctx)

		state := map[string]any{"kind": "live-smoke", "value": 1}
		if err := session.Save(ctx, state); err != nil {
			t.Fatalf("save: %v", err)
		}

		var loaded map[string]any
		if err := session.Load(ctx, &loaded); err != nil {
			t.Fatalf("load: %v", err)
		}
		if loaded["kind"] != "live-smoke" {
			t.Fatalf("unexpected loaded state: %+v", loaded)
		}

		store := session.Attachments()
		attachRes, err := store.Attach(ctx, client.AttachRequest{
			Name:        "smoke.txt",
			ContentType: "text/plain",
			Body:        strings.NewReader("smoke-attachment"),
		})
		if err != nil {
			t.Fatalf("attach: %v", err)
		}
		if attachRes == nil || attachRes.Attachment.Name != "smoke.txt" {
			t.Fatalf("unexpected attach result: %+v", attachRes)
		}

		list, err := store.List(ctx)
		if err != nil {
			t.Fatalf("list attachments: %v", err)
		}
		if list == nil || len(list.Attachments) != 1 {
			t.Fatalf("unexpected attachment list: %+v", list)
		}

		att, err := store.Retrieve(ctx, client.AttachmentSelector{Name: "smoke.txt"})
		if err != nil {
			t.Fatalf("retrieve attachment: %v", err)
		}
		data, err := io.ReadAll(att)
		if err != nil {
			_ = att.Close()
			t.Fatalf("read attachment: %v", err)
		}
		if err := att.Close(); err != nil {
			t.Fatalf("close attachment: %v", err)
		}
		if string(data) != "smoke-attachment" {
			t.Fatalf("unexpected attachment payload: %q", data)
		}

		delRes, err := store.Delete(ctx, client.AttachmentSelector{Name: "smoke.txt"})
		if err != nil {
			t.Fatalf("delete attachment: %v", err)
		}
		if delRes == nil || !delRes.Deleted {
			t.Fatalf("unexpected delete result: %+v", delRes)
		}
	})

	t.Run("AcquireForUpdate", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		key := unique("live-smoke-afu")
		seed, err := cli.Acquire(ctx, api.AcquireRequest{
			Key:        key,
			Owner:      "live-smoke-seed",
			TTLSeconds: 30,
		})
		if err != nil {
			t.Fatalf("seed acquire: %v", err)
		}
		if err := seed.Save(ctx, map[string]any{"counter": 1}); err != nil {
			_ = seed.Release(ctx)
			t.Fatalf("seed save: %v", err)
		}
		if err := seed.Release(ctx); err != nil {
			t.Fatalf("seed release: %v", err)
		}

		if err := cli.AcquireForUpdate(ctx, api.AcquireRequest{
			Key:        key,
			Owner:      "live-smoke-afu",
			TTLSeconds: 30,
		}, func(handlerCtx context.Context, af *client.AcquireForUpdateContext) error {
			if af == nil {
				return fmt.Errorf("acquire-for-update context is nil")
			}
			var current struct {
				Counter int `json:"counter"`
			}
			if af.State != nil {
				if err := af.State.Decode(&current); err != nil {
					return fmt.Errorf("decode state: %w", err)
				}
			}
			current.Counter++
			return af.Save(handlerCtx, &current)
		}); err != nil {
			t.Fatalf("acquire for update: %v", err)
		}

		verify, err := cli.Acquire(ctx, api.AcquireRequest{
			Key:        key,
			Owner:      "live-smoke-verify",
			TTLSeconds: 30,
		})
		if err != nil {
			t.Fatalf("verify acquire: %v", err)
		}
		defer verify.Release(ctx)
		var out struct {
			Counter int `json:"counter"`
		}
		if err := verify.Load(ctx, &out); err != nil {
			t.Fatalf("verify load: %v", err)
		}
		if out.Counter != 2 {
			t.Fatalf("expected counter=2, got %d", out.Counter)
		}
	})

	t.Run("DequeueWithStateFirstSave", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		queue := unique("live-smoke-state-queue")
		if _, err := cli.EnqueueBytes(ctx, queue, []byte(`{"kind":"live-smoke-state"}`), client.EnqueueOptions{}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}

		msg, err := cli.DequeueWithState(ctx, queue, client.DequeueOptions{
			Owner:        "live-smoke-state-worker",
			BlockSeconds: 5,
		})
		if err != nil {
			t.Fatalf("dequeue with state: %v", err)
		}
		if msg == nil || msg.StateHandle() == nil {
			t.Fatalf("expected stateful queue message")
		}
		defer msg.Close()
		if err := msg.ClosePayload(); err != nil {
			t.Fatalf("close payload: %v", err)
		}

		if err := msg.StateHandle().Save(ctx, map[string]any{"counter": 1}); err != nil {
			t.Fatalf("state save: %v", err)
		}
		var state struct {
			Counter int `json:"counter"`
		}
		if err := msg.StateHandle().Load(ctx, &state); err != nil {
			t.Fatalf("state load: %v", err)
		}
		if state.Counter != 1 {
			t.Fatalf("expected counter=1, got %d", state.Counter)
		}

		if err := msg.Ack(ctx); err != nil {
			t.Fatalf("ack: %v", err)
		}
	})

	t.Run("Subscribe", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		queue := unique("live-smoke-sub")
		if _, err := cli.EnqueueBytes(ctx, queue, []byte("sub-payload"), client.EnqueueOptions{}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}

		var seen atomic.Bool
		subCtx, subCancel := context.WithTimeout(ctx, 6*time.Second)
		err := cli.Subscribe(subCtx, queue, client.SubscribeOptions{
			Owner:        "live-smoke-sub-worker",
			Prefetch:     1,
			BlockSeconds: 2,
		}, func(handlerCtx context.Context, msg *client.QueueMessage) error {
			if msg == nil {
				return fmt.Errorf("nil subscribe message")
			}
			defer msg.Close()
			data, err := io.ReadAll(msg)
			if err != nil {
				return fmt.Errorf("read subscribe payload: %w", err)
			}
			if string(data) != "sub-payload" {
				return fmt.Errorf("unexpected subscribe payload: %q", data)
			}
			if err := msg.Ack(handlerCtx); err != nil {
				return fmt.Errorf("ack subscribe message: %w", err)
			}
			seen.Store(true)
			subCancel()
			return nil
		})
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			t.Fatalf("subscribe: %v", err)
		}
		if !seen.Load() {
			t.Fatalf("subscribe handler did not run")
		}
	})

	t.Run("QueryKeysAndDocuments", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		keyA := unique("live-smoke-query-a")
		keyB := unique("live-smoke-query-b")

		for _, key := range []string{keyA, keyB} {
			sess, err := cli.Acquire(ctx, api.AcquireRequest{
				Key:        key,
				Owner:      "live-smoke-query-writer",
				TTLSeconds: 30,
			})
			if err != nil {
				t.Fatalf("query acquire %s: %v", key, err)
			}
			if err := sess.Save(ctx, map[string]any{
				"kind":   "live-smoke-query",
				"status": "open",
				"key":    key,
			}); err != nil {
				_ = sess.Release(ctx)
				t.Fatalf("query save %s: %v", key, err)
			}
			if err := sess.Release(ctx); err != nil {
				t.Fatalf("query release %s: %v", key, err)
			}
		}

		respKeys, err := cli.Query(ctx,
			client.WithQuery(`eq{field=/kind,value=live-smoke-query}`),
			client.WithQueryEngineScan(),
			client.WithQueryReturnKeys(),
			client.WithQueryLimit(20),
		)
		if err != nil {
			t.Fatalf("query keys: %v", err)
		}
		keys := respKeys.Keys()
		if !slices.Contains(keys, keyA) || !slices.Contains(keys, keyB) {
			t.Fatalf("query keys missing expected entries: keys=%v keyA=%q keyB=%q", keys, keyA, keyB)
		}

		respDocs, err := cli.Query(ctx,
			client.WithQuery(`eq{field=/kind,value=live-smoke-query}`),
			client.WithQueryEngineScan(),
			client.WithQueryReturnDocuments(),
			client.WithQueryLimit(20),
		)
		if err != nil {
			t.Fatalf("query documents: %v", err)
		}
		defer respDocs.Close()
		var docCount int
		if err := respDocs.ForEach(func(row client.QueryRow) error {
			if !row.HasDocument() {
				return fmt.Errorf("expected document row")
			}
			var doc struct {
				Kind string `json:"kind"`
			}
			if err := row.DocumentInto(&doc); err != nil {
				return err
			}
			if doc.Kind != "live-smoke-query" {
				return fmt.Errorf("unexpected document kind %q", doc.Kind)
			}
			docCount++
			return nil
		}); err != nil {
			t.Fatalf("query documents foreach: %v", err)
		}
		if docCount < 2 {
			t.Fatalf("expected at least 2 query documents, got %d", docCount)
		}
	})
}
