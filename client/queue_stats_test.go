package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/client"
)

func TestClientQueueStatsSuccess(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method %s", r.Method)
		}
		if r.URL.Path != "/v1/queue/stats" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		var req api.QueueStatsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Namespace != "mcp" || req.Queue != "jobs" {
			t.Fatalf("unexpected request %+v", req)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(api.QueueStatsResponse{
			Namespace:         req.Namespace,
			Queue:             req.Queue,
			WaitingConsumers:  1,
			PendingCandidates: 2,
			TotalConsumers:    3,
			HasActiveWatcher:  true,
			Available:         true,
			HeadMessageID:     "msg-1",
			HeadAgeSeconds:    7,
		})
	}))
	defer ts.Close()

	cli, err := client.New(ts.URL, client.WithHTTPClient(ts.Client()), client.WithDisableMTLS(true))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.QueueStats(ctx, "jobs", client.QueueStatsOptions{Namespace: "mcp"})
	if err != nil {
		t.Fatalf("queue stats: %v", err)
	}
	if resp.Namespace != "mcp" || resp.Queue != "jobs" {
		t.Fatalf("unexpected response identity %+v", resp)
	}
	if !resp.Available || resp.HeadMessageID != "msg-1" {
		t.Fatalf("unexpected availability fields %+v", resp)
	}
}

func TestClientQueueStatsError(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(api.ErrorResponse{
			ErrorCode: "queue_disabled",
			Detail:    "queue service not configured",
		})
	}))
	defer ts.Close()

	cli, err := client.New(ts.URL, client.WithHTTPClient(ts.Client()), client.WithDisableMTLS(true))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = cli.QueueStats(ctx, "jobs", client.QueueStatsOptions{Namespace: "mcp"})
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError, got %T", err)
	}
	if apiErr.Status != http.StatusServiceUnavailable || apiErr.Response.ErrorCode != "queue_disabled" {
		t.Fatalf("unexpected api error %+v", apiErr)
	}
}
