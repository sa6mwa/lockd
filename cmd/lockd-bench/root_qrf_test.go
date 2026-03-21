package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pkt.systems/lockd/internal/qrf"
)

func TestDiskStoreRootParsesAbsoluteDiskURL(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "disk-root")
	got, err := diskStoreRoot("disk://" + root)
	if err != nil {
		t.Fatalf("diskStoreRoot: %v", err)
	}
	if got != root {
		t.Fatalf("root=%q want=%q", got, root)
	}
	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("Stat root: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("root %q is not a directory", root)
	}
}

func TestPrepareBenchRootUsesScopedDiskStoreBase(t *testing.T) {
	base := t.TempDir()
	t.Setenv("LOCKD_STORE", "disk://"+base)
	root, cleanup := prepareBenchRoot(benchConfig{backend: "disk"})
	defer cleanup()

	wantPrefix := filepath.Join(base, benchScopedDiskRootDir) + string(os.PathSeparator)
	if !strings.HasPrefix(root, wantPrefix) {
		t.Fatalf("root=%q want prefix=%q", root, wantPrefix)
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("Stat root: %v", err)
	}
}

func TestValidateBenchQRFStatusRejectsSoftArmAndEngaged(t *testing.T) {
	t.Parallel()

	if err := validateBenchQRFStatus(qrf.Status{State: qrf.StateDisengaged}); err != nil {
		t.Fatalf("disengaged rejected: %v", err)
	}
	if err := validateBenchQRFStatus(qrf.Status{State: qrf.StateRecovery}); err != nil {
		t.Fatalf("recovery rejected: %v", err)
	}
	if err := validateBenchQRFStatus(qrf.Status{State: qrf.StateSoftArm, Reason: "cpu_soft"}); err == nil {
		t.Fatal("expected soft-arm rejection")
	}
	if err := validateBenchQRFStatus(qrf.Status{State: qrf.StateEngaged, Reason: "cpu_hard"}); err == nil {
		t.Fatal("expected engaged rejection")
	}
}
