package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/pslog"
)

func BenchmarkAcquireUpdate(b *testing.B) {
	h := New(Config{
		Store:        memory.New(),
		Logger:       benchmarkLogger{},
		JSONMaxBytes: 1 << 20,
		DefaultTTL:   5 * time.Second,
		MaxTTL:       time.Minute,
		AcquireBlock: time.Second,
	})

	for i := 0; b.Loop(); i++ {
		key := fmt.Sprintf("bench-%d", i)
		rec := httptest.NewRecorder()
		body, _ := json.Marshal(api.AcquireRequest{Key: key, Owner: "worker", TTLSeconds: 5})
		req := httptest.NewRequest(http.MethodPost, "/v1/acquire", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if err := h.handleAcquire(rec, req); err != nil {
			b.Fatalf("acquire error: %v", err)
		}
		var acquireResp api.AcquireResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &acquireResp); err != nil {
			b.Fatalf("decode acquire: %v", err)
		}
		fence := strconv.FormatInt(acquireResp.FencingToken, 10)

		rec = httptest.NewRecorder()
		updateReq := httptest.NewRequest(http.MethodPost, "/v1/update-state?key="+key, bytes.NewBufferString(`{"seq":1}`))
		updateReq.Header.Set("Content-Type", "application/json")
		updateReq.Header.Set("X-Lease-ID", acquireResp.LeaseID)
		updateReq.Header.Set("X-Fencing-Token", fence)
		if err := h.handleUpdateState(rec, updateReq); err != nil {
			b.Fatalf("update state: %v", err)
		}

		rec = httptest.NewRecorder()
		releaseBody, _ := json.Marshal(api.ReleaseRequest{Key: key, LeaseID: acquireResp.LeaseID})
		releaseReq := httptest.NewRequest(http.MethodPost, "/v1/release", bytes.NewReader(releaseBody))
		releaseReq.Header.Set("Content-Type", "application/json")
		releaseReq.Header.Set("X-Fencing-Token", fence)
		if err := h.handleRelease(rec, releaseReq); err != nil {
			b.Fatalf("release: %v", err)
		}
	}
}

type benchmarkLogger struct{}

func (benchmarkLogger) Trace(string, ...any)                {}
func (benchmarkLogger) Debug(string, ...any)                {}
func (benchmarkLogger) Info(string, ...any)                 {}
func (benchmarkLogger) Warn(string, ...any)                 {}
func (benchmarkLogger) Error(string, ...any)                {}
func (benchmarkLogger) Fatal(string, ...any)                {}
func (benchmarkLogger) Panic(string, ...any)                {}
func (benchmarkLogger) Log(pslog.Level, string, ...any)     {}
func (benchmarkLogger) With(...any) pslog.Logger            { return benchmarkLogger{} }
func (benchmarkLogger) WithLogLevel() pslog.Logger          { return benchmarkLogger{} }
func (benchmarkLogger) LogLevel(pslog.Level) pslog.Logger   { return benchmarkLogger{} }
func (benchmarkLogger) LogLevelFromEnv(string) pslog.Logger { return benchmarkLogger{} }
