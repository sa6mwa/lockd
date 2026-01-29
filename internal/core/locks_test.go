package core

import (
	"testing"
	"time"

	"pkt.systems/lockd/api"
)

func TestResolveBlockNoWait(t *testing.T) {
	svc := &Service{acquireBlock: 5 * time.Second}

	block, waitForever := svc.resolveBlock(api.BlockNoWait)
	if block != 0 {
		t.Fatalf("expected block=0 for BlockNoWait, got %s", block)
	}
	if waitForever {
		t.Fatalf("expected waitForever=false for BlockNoWait")
	}
}

func TestResolveBlockDefaults(t *testing.T) {
	svc := &Service{acquireBlock: 7 * time.Second}

	block, waitForever := svc.resolveBlock(0)
	if block != 7*time.Second {
		t.Fatalf("expected block=%s for default, got %s", 7*time.Second, block)
	}
	if !waitForever {
		t.Fatalf("expected waitForever=true for default block seconds")
	}

	block, waitForever = svc.resolveBlock(2)
	if block != 2*time.Second {
		t.Fatalf("expected block=2s, got %s", block)
	}
	if waitForever {
		t.Fatalf("expected waitForever=false for explicit block seconds")
	}
}
