package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/logport"
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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

func (benchmarkLogger) LogLevel(level logport.Level) logport.ForLogging                       { return benchmarkLogger{} }
func (benchmarkLogger) LogLevelFromEnv(string) logport.ForLogging                             { return benchmarkLogger{} }
func (benchmarkLogger) WithLogLevel() logport.ForLogging                                      { return benchmarkLogger{} }
func (benchmarkLogger) With(keyvals ...any) logport.ForLogging                                { return benchmarkLogger{} }
func (benchmarkLogger) WithTrace(ctx context.Context) logport.ForLogging                      { return benchmarkLogger{} }
func (benchmarkLogger) Log(ctx context.Context, level slog.Level, msg string, keyvals ...any) {}
func (benchmarkLogger) Logp(level logport.Level, msg string, keyvals ...any)                  {}
func (benchmarkLogger) Logs(level string, msg string, keyvals ...any)                         {}
func (benchmarkLogger) Logf(level logport.Level, format string, v ...any)                     {}
func (benchmarkLogger) Debug(msg string, keyvals ...any)                                      {}
func (benchmarkLogger) Debugf(format string, v ...any)                                        {}
func (benchmarkLogger) Info(msg string, keyvals ...any)                                       {}
func (benchmarkLogger) Infof(format string, v ...any)                                         {}
func (benchmarkLogger) Warn(msg string, keyvals ...any)                                       {}
func (benchmarkLogger) Warnf(format string, v ...any)                                         {}
func (benchmarkLogger) Error(msg string, keyvals ...any)                                      {}
func (benchmarkLogger) Errorf(format string, v ...any)                                        {}
func (benchmarkLogger) Fatal(msg string, keyvals ...any)                                      {}
func (benchmarkLogger) Fatalf(format string, v ...any)                                        {}
func (benchmarkLogger) Panic(msg string, keyvals ...any)                                      {}
func (benchmarkLogger) Panicf(format string, v ...any)                                        {}
func (benchmarkLogger) Trace(msg string, keyvals ...any)                                      {}
func (benchmarkLogger) Tracef(format string, v ...any)                                        {}
func (benchmarkLogger) Enabled(context.Context, slog.Level) bool                              { return false }
func (benchmarkLogger) Handle(context.Context, slog.Record) error                             { return nil }
func (benchmarkLogger) WithAttrs([]slog.Attr) slog.Handler                                    { return benchmarkLogger{} }
func (benchmarkLogger) WithGroup(string) slog.Handler                                         { return benchmarkLogger{} }
func (benchmarkLogger) Write(p []byte) (int, error)                                           { return len(p), nil }
