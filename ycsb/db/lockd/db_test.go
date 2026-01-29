package lockd

import (
	"testing"
	"time"

	"github.com/magiconair/properties"
)

func TestParseConfigPhaseMetrics(t *testing.T) {
	p := properties.NewProperties()
	p.Set(propEndpoints, "https://127.0.0.1:9341")
	p.Set(propDisableMTLS, "true")
	p.Set(propPhaseMetrics, "true")

	cfg, err := parseConfig(p)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if !cfg.phaseMetrics {
		t.Fatalf("expected phaseMetrics to be true")
	}
}

func TestRecordPhaseInvokesRecorder(t *testing.T) {
	called := false
	cfg := &driverConfig{
		phaseRecorder: func(op string, start time.Time, dur time.Duration) {
			called = true
			if op != "LOCKD_ACQUIRE" {
				t.Fatalf("unexpected op %q", op)
			}
			if dur <= 0 {
				t.Fatalf("expected positive duration")
			}
		},
	}
	db := &lockdDB{cfg: cfg}
	start := time.Now().Add(-time.Millisecond)
	db.recordPhase("LOCKD_ACQUIRE", start)
	if !called {
		t.Fatalf("expected recorder to be called")
	}
}

func TestRecordPhaseNoop(t *testing.T) {
	db := &lockdDB{cfg: &driverConfig{}}
	db.recordPhase("LOCKD_ACQUIRE", time.Now())
}
