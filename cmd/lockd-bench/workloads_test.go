package main

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestWorkloadKeyModeForXAUsesSequentialKeys(t *testing.T) {
	t.Parallel()

	if got := workloadKeyModeFor("xa-rollback"); got != workloadKeyModeSequential {
		t.Fatalf("xa-rollback key mode=%v want=%v", got, workloadKeyModeSequential)
	}
	if got := workloadKeyModeFor("xa-commit"); got != workloadKeyModeSequential {
		t.Fatalf("xa-commit key mode=%v want=%v", got, workloadKeyModeSequential)
	}
	if got := workloadKeyModeFor("lock"); got != workloadKeyModeRandom {
		t.Fatalf("lock key mode=%v want=%v", got, workloadKeyModeRandom)
	}
}

func TestPickWorkerKeySequentialExhaustsSubsetBeforeReuse(t *testing.T) {
	t.Parallel()

	keys := buildBenchKeys("bench", 8)
	rng := rand.New(rand.NewSource(1))
	seen := make(map[string]struct{}, len(keys))
	for i := 0; i < len(keys); i++ {
		key := pickWorkerKey(keys, workloadKeyModeSequential, rng, i)
		if _, ok := seen[key.key]; ok {
			t.Fatalf("iteration %d reused key %q before subset exhaustion", i, key.key)
		}
		seen[key.key] = struct{}{}
	}
	if got := pickWorkerKey(keys, workloadKeyModeSequential, rng, len(keys)); got.key != keys[0].key {
		t.Fatalf("wrap key=%q want=%q", got.key, keys[0].key)
	}
}

func TestRunWorkloadOpsXASequentialDoesNotReuseKeysWithinRun(t *testing.T) {
	t.Parallel()

	cfg := benchConfig{
		workload:    "xa-rollback",
		ops:         1000,
		concurrency: 8,
	}
	keys := buildBenchKeys("bench", cfg.ops)
	seen := make(map[string]int, len(keys))
	var mu sync.Mutex

	run, err := runWorkloadOps(context.Background(), cfg, keys, []string{"total"}, func(_ context.Context, key benchKey) opResult {
		// Force heavy worker skew; sequential XA must still consume each key exactly once.
		if key.idx%cfg.concurrency != 0 {
			time.Sleep(time.Millisecond)
		}
		mu.Lock()
		seen[key.key]++
		mu.Unlock()
		return opResult{phases: map[string]time.Duration{"total": 0}}
	})
	if err != nil {
		t.Fatalf("runWorkloadOps: %v", err)
	}
	if got := run.phases["total"].errs; got != 0 {
		t.Fatalf("total errors=%d want=0", got)
	}
	if len(seen) != cfg.ops {
		t.Fatalf("unique keys=%d want=%d", len(seen), cfg.ops)
	}
	for key, count := range seen {
		if count != 1 {
			t.Fatalf("key %q seen %d times want 1", key, count)
		}
	}
}
