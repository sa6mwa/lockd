//go:build integration

package queuetestutil

import (
	"testing"
	"time"
)

func TestQueueProbeTimeoutForStore(t *testing.T) {
	fast := 250 * time.Millisecond
	if got := queueProbeTimeoutForStore("aws://bucket", fast); got != 2*time.Second {
		t.Fatalf("aws probe timeout = %s, want %s", got, 2*time.Second)
	}
	if got := queueProbeTimeoutForStore("s3://host/bucket", fast); got != 2*time.Second {
		t.Fatalf("s3 probe timeout = %s, want %s", got, 2*time.Second)
	}
	if got := queueProbeTimeoutForStore("azure://account/container", fast); got != 2*time.Second {
		t.Fatalf("azure probe timeout = %s, want %s", got, 2*time.Second)
	}
	if got := queueProbeTimeoutForStore("disk:///tmp", fast); got != fast {
		t.Fatalf("disk probe timeout = %s, want %s", got, fast)
	}
	if got := queueProbeTimeoutForStore("mem://", 0); got != fast {
		t.Fatalf("default probe timeout = %s, want %s", got, fast)
	}
	if got := queueProbeTimeoutForStore("aws://bucket", 3*time.Second); got != 3*time.Second {
		t.Fatalf("custom probe timeout = %s, want %s", got, 3*time.Second)
	}
}
