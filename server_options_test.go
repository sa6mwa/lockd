package lockd

import (
	"testing"
	"time"
)

func TestDefaultCloseOptions(t *testing.T) {
	opts := defaultCloseOptions()
	if opts.drain.GracePeriod != 10*time.Second {
		t.Fatalf("default grace period = %v", opts.drain.GracePeriod)
	}
	if !opts.drain.NotifyClients {
		t.Fatal("default drain should notify clients")
	}
	if opts.shutdownTimeout != 10*time.Second {
		t.Fatalf("default shutdown timeout = %v", opts.shutdownTimeout)
	}
}

func TestResolveCloseOptionsAppliesOverrides(t *testing.T) {
	base := closeOptions{}
	resolved := resolveCloseOptions(base, nil)
	if resolved.drain.GracePeriod != 10*time.Second {
		t.Fatalf("expected default grace, got %v", resolved.drain.GracePeriod)
	}
	custom := resolveCloseOptions(defaultCloseOptions(), []CloseOption{
		WithDrainLeases(time.Second),
		WithShutdownTimeout(2 * time.Second),
	})
	if custom.drain.GracePeriod != time.Second {
		t.Fatalf("drain override failed: %v", custom.drain.GracePeriod)
	}
	if custom.shutdownTimeout != 2*time.Second {
		t.Fatalf("shutdown override failed: %v", custom.shutdownTimeout)
	}
	policy := DrainLeasesPolicy{GracePeriod: -5 * time.Second, ForceRelease: true}
	custom = resolveCloseOptions(defaultCloseOptions(), []CloseOption{WithDrainLeasesPolicy(policy)})
	if custom.drain.GracePeriod != 0 {
		t.Fatalf("negative grace should normalize to 0, got %v", custom.drain.GracePeriod)
	}
	if !custom.drain.ForceRelease {
		t.Fatal("force release flag lost")
	}
}

func TestWithDefaultCloseOptions(t *testing.T) {
	var o options
	WithDefaultCloseOptions(WithDrainLeases(2 * time.Second))(&o)
	if len(o.closeDefaults) != 1 {
		t.Fatalf("expected 1 default close option, got %d", len(o.closeDefaults))
	}
	resolved := resolveCloseOptions(defaultCloseOptions(), o.closeDefaults)
	if resolved.drain.GracePeriod != 2*time.Second {
		t.Fatalf("default drain override not applied: %v", resolved.drain.GracePeriod)
	}
}

func TestWithLSFLogInterval(t *testing.T) {
	var o options
	WithLSFLogInterval(5 * time.Second)(&o)
	if len(o.configHooks) != 1 {
		t.Fatalf("expected config hook, got %d", len(o.configHooks))
	}
	var cfg Config
	o.configHooks[0](&cfg)
	if cfg.LSFLogInterval != 5*time.Second || !cfg.LSFLogIntervalSet {
		t.Fatalf("lsf interval not applied: %+v", cfg.LSFLogInterval)
	}
}
