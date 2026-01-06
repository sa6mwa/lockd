package lockd

import (
	"testing"
	"time"
)

func startTestServerFast(t testing.TB, opts ...TestServerOption) *TestServer {
	t.Helper()
	base := []TestServerOption{
		WithTestCloseDefaults(WithDrainLeases(-1), WithShutdownTimeout(500*time.Millisecond)),
		WithTestConfigFunc(func(cfg *Config) {
			if cfg.TCFanoutTimeout <= 0 {
				cfg.TCFanoutTimeout = 250 * time.Millisecond
			}
			if cfg.TCFanoutBaseDelay <= 0 {
				cfg.TCFanoutBaseDelay = 25 * time.Millisecond
			}
			if cfg.TCFanoutMaxDelay <= 0 {
				cfg.TCFanoutMaxDelay = 100 * time.Millisecond
			}
		}),
	}
	base = append(base, opts...)
	return StartTestServer(t, base...)
}
