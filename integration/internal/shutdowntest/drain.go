package shutdowntest

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"pkt.systems/lockd/api"
)

// AcquireResult captures the shutdown-draining error response.
type AcquireResult struct {
	Response api.ErrorResponse
	Header   http.Header
}

// WaitForShutdownDrainingAcquire posts to /v1/acquire until the server returns
// HTTP 503 with error code shutdown_draining.
func WaitForShutdownDrainingAcquire(t testing.TB, url string, payload []byte) AcquireResult {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusServiceUnavailable {
			var out api.ErrorResponse
			if err := json.Unmarshal(body, &out); err != nil {
				t.Fatalf("parse error response: %v", err)
			}
			return AcquireResult{Response: out, Header: resp.Header.Clone()}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("did not observe shutdown_draining response at %s", url)
	return AcquireResult{}
}
