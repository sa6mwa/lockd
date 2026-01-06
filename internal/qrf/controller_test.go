package qrf

import (
	"testing"
	"time"

	"pkt.systems/pslog"
)

func TestControllerEngageAndRecover(t *testing.T) {
	ctrl := NewController(Config{
		Enabled:              true,
		QueueSoftLimit:       10,
		QueueHardLimit:       20,
		LockSoftLimit:        10,
		LockHardLimit:        20,
		MemorySoftLimitBytes: 1 << 62,
		MemoryHardLimitBytes: 1 << 63,
		RecoverySamples:      2,
		SoftRetryAfter:       50 * time.Millisecond,
		EngagedRetryAfter:    200 * time.Millisecond,
		RecoveryRetryAfter:   100 * time.Millisecond,
		Logger:               pslog.NoopLogger(),
	})

	ctrl.Observe(Snapshot{
		QueueProducerInflight: 20,
		QueueConsumerInflight: 5,
		LockInflight:          5,
		RSSBytes:              1 << 50,
		Goroutines:            100,
		CollectedAt:           time.Now(),
	})
	if got := ctrl.State(); got != StateEngaged {
		t.Fatalf("expected state %s, got %s", StateEngaged, got)
	}

	ctrl.Observe(Snapshot{
		QueueProducerInflight: 4,
		QueueConsumerInflight: 1,
		LockInflight:          2,
		RSSBytes:              1 << 40,
		Goroutines:            50,
		CollectedAt:           time.Now(),
	})
	if got := ctrl.State(); got != StateEngaged {
		t.Fatalf("expected state to remain engaged until recovery threshold, got %s", got)
	}

	ctrl.Observe(Snapshot{
		QueueProducerInflight: 3,
		QueueConsumerInflight: 1,
		LockInflight:          3,
		RSSBytes:              1 << 40,
		Goroutines:            40,
		CollectedAt:           time.Now(),
	})
	if got := ctrl.State(); got != StateRecovery {
		t.Fatalf("expected transition to recovery, got %s", got)
	}

	ctrl.Observe(Snapshot{
		QueueProducerInflight: 2,
		QueueConsumerInflight: 1,
		LockInflight:          1,
		RSSBytes:              1 << 39,
		Goroutines:            35,
		CollectedAt:           time.Now(),
	})
	if got := ctrl.State(); got != StateRecovery {
		t.Fatalf("expected recovery to persist until second healthy sample, got %s", got)
	}

	ctrl.Observe(Snapshot{
		QueueProducerInflight: 1,
		QueueConsumerInflight: 1,
		LockInflight:          0,
		RSSBytes:              1 << 38,
		Goroutines:            30,
		CollectedAt:           time.Now(),
	})
	if got := ctrl.State(); got != StateDisengaged {
		t.Fatalf("expected disengaged after sustained health, got %s", got)
	}
}

func TestConsumerLimitThrottlingBehaviour(t *testing.T) {
	ctrl := NewController(Config{
		Enabled:                true,
		QueueConsumerSoftLimit: 10,
		QueueConsumerHardLimit: 12,
		RecoverySamples:        1,
		SoftRetryAfter:         25 * time.Millisecond,
		EngagedRetryAfter:      150 * time.Millisecond,
		RecoveryRetryAfter:     75 * time.Millisecond,
		Logger:                 pslog.NoopLogger(),
	})

	now := time.Now()

	// Soft limit exceeded with more consumers than producers: expect throttle.
	ctrl.Observe(Snapshot{
		QueueProducerInflight: 8,
		QueueConsumerInflight: 10,
		CollectedAt:           now,
	})
	if got := ctrl.State(); got != StateSoftArm {
		t.Fatalf("expected soft arm state on consumer soft limit, got %s", got)
	}
	if dec := ctrl.Decide(KindQueueConsumer); !dec.Throttle || dec.State != StateSoftArm {
		t.Fatalf("expected consumer throttle while soft-armed, got %+v", dec)
	}

	// Same soft limit but producers now dominate; allow admission to help drain backlog.
	ctrl.Observe(Snapshot{
		QueueProducerInflight: 16,
		QueueConsumerInflight: 10,
		CollectedAt:           now.Add(10 * time.Millisecond),
	})
	if got := ctrl.State(); got != StateSoftArm {
		t.Fatalf("expected to remain soft-armed, got %s", got)
	}
	if dec := ctrl.Decide(KindQueueConsumer); dec.Throttle {
		t.Fatalf("expected consumer to be admitted when producers dominate, got %+v", dec)
	}

	// Hard limit must enforce throttling regardless of producer pressure.
	ctrl.Observe(Snapshot{
		QueueProducerInflight: 20,
		QueueConsumerInflight: 12,
		CollectedAt:           now.Add(20 * time.Millisecond),
	})
	if got := ctrl.State(); got != StateEngaged {
		t.Fatalf("expected engaged state on consumer hard limit, got %s", got)
	}
	if dec := ctrl.Decide(KindQueueConsumer); !dec.Throttle || dec.State != StateEngaged {
		t.Fatalf("expected hard limit to throttle consumers, got %+v", dec)
	}
}

func TestControllerDisengageStopsQueueThrottling(t *testing.T) {
	cfg := Config{
		Enabled:                 true,
		QueueSoftLimit:          4,
		QueueHardLimit:          8,
		RecoverySamples:         2,
		SoftRetryAfter:          50 * time.Millisecond,
		EngagedRetryAfter:       200 * time.Millisecond,
		RecoveryRetryAfter:      100 * time.Millisecond,
		Logger:                  pslog.NoopLogger(),
		LoadSoftLimitMultiplier: 0,
		LoadHardLimitMultiplier: 0,
	}

	ctrl := NewController(cfg)

	// Step 1: soft arm via queue pressure.
	ctrl.Observe(Snapshot{
		QueueProducerInflight: 5,
		CollectedAt:           time.Now(),
	})
	if got := ctrl.State(); got != StateSoftArm {
		t.Fatalf("expected soft arm state, got %s", got)
	}
	if dec := ctrl.Decide(KindQueueProducer); !dec.Throttle || dec.State != StateSoftArm {
		t.Fatalf("expected producers to be throttled while soft armed, got %+v", dec)
	}

	// Step 2: escalate to full engagement.
	ctrl.Observe(Snapshot{
		QueueProducerInflight: 9,
		CollectedAt:           time.Now(),
	})
	if got := ctrl.State(); got != StateEngaged {
		t.Fatalf("expected engaged state after hard breach, got %s", got)
	}
	if dec := ctrl.Decide(KindQueueProducer); !dec.Throttle || dec.State != StateEngaged || dec.RetryAfter <= 0 {
		t.Fatalf("expected producers to be throttled aggressively while engaged, got %+v", dec)
	}

	healthySnapshot := Snapshot{
		QueueProducerInflight: 0,
		QueueConsumerInflight: 1,
		CollectedAt:           time.Now(),
	}

	// Step 3: drive controller towards recovery (requires consecutive healthy samples).
	ctrl.Observe(healthySnapshot)
	if got := ctrl.State(); got != StateEngaged {
		t.Fatalf("expected to remain engaged until recovery samples satisfied, got %s", got)
	}
	ctrl.Observe(healthySnapshot)
	if got := ctrl.State(); got != StateRecovery {
		t.Fatalf("expected transition to recovery after healthy samples, got %s", got)
	}
	if dec := ctrl.Decide(KindQueueProducer); !dec.Throttle || dec.State != StateRecovery {
		t.Fatalf("expected producers to remain throttled during recovery, got %+v", dec)
	}

	// Step 4: continue observing healthy metrics until fully disengaged.
	ctrl.Observe(healthySnapshot)
	if got := ctrl.State(); got != StateRecovery {
		t.Fatalf("expected recovery to persist until second healthy sample, got %s", got)
	}
	ctrl.Observe(healthySnapshot)
	if got := ctrl.State(); got != StateDisengaged {
		t.Fatalf("expected controller to disengage after sustained health, got %s", got)
	}
	if dec := ctrl.Decide(KindQueueProducer); dec.Throttle {
		t.Fatalf("expected producers to proceed once disengaged, got %+v", dec)
	}
}

func TestControllerSoftArm(t *testing.T) {
	ctrl := NewController(Config{
		Enabled:              true,
		QueueSoftLimit:       4,
		QueueHardLimit:       10,
		LockSoftLimit:        4,
		LockHardLimit:        10,
		MemorySoftLimitBytes: 1 << 62,
		MemoryHardLimitBytes: 1 << 63,
		RecoverySamples:      1,
		Logger:               pslog.NoopLogger(),
	})

	ctrl.Observe(Snapshot{
		QueueProducerInflight: 5,
		QueueConsumerInflight: 0,
		LockInflight:          1,
		RSSBytes:              1 << 40,
		Goroutines:            20,
		CollectedAt:           time.Now(),
	})
	if got := ctrl.State(); got != StateSoftArm {
		t.Fatalf("expected soft arm state, got %s", got)
	}

	ctrl.Observe(Snapshot{
		QueueProducerInflight: 0,
		QueueConsumerInflight: 1,
		LockInflight:          1,
		RSSBytes:              1 << 38,
		Goroutines:            15,
		CollectedAt:           time.Now(),
	})
	if got := ctrl.State(); got != StateDisengaged {
		t.Fatalf("expected disengaged after soft arm recovery, got %s", got)
	}
}

func TestConsumerBiasDuringEngaged(t *testing.T) {
	ctrl := NewController(Config{
		Enabled:            true,
		QueueSoftLimit:     8,
		QueueHardLimit:     12,
		RecoverySamples:    1,
		SoftRetryAfter:     50 * time.Millisecond,
		EngagedRetryAfter:  200 * time.Millisecond,
		RecoveryRetryAfter: 100 * time.Millisecond,
		Logger:             pslog.NoopLogger(),
	})

	ctrl.Observe(Snapshot{
		QueueProducerInflight: 11,
		QueueConsumerInflight: 2,
		QueueAckInflight:      1,
		LockInflight:          1,
		RSSBytes:              1 << 40,
		Goroutines:            90,
		CollectedAt:           time.Now(),
	})
	if ctrl.State() != StateEngaged {
		t.Fatalf("expected engaged state, got %s", ctrl.State())
	}

	prodDecision := ctrl.Decide(KindQueueProducer)
	if !prodDecision.Throttle {
		t.Fatalf("expected producers to be throttled while engaged")
	}

	consDecision := ctrl.Decide(KindQueueConsumer)
	if consDecision.Throttle {
		t.Fatalf("expected consumers to proceed to drain during engagement")
	}
}

func TestControllerThresholdTriggers(t *testing.T) {
	baseCfg := Config{
		Enabled:                 true,
		RecoverySamples:         1,
		Logger:                  pslog.NoopLogger(),
		QueueSoftLimit:          0,
		QueueHardLimit:          0,
		LockSoftLimit:           0,
		LockHardLimit:           0,
		MemorySoftLimitBytes:    0,
		MemoryHardLimitBytes:    0,
		MemorySoftLimitPercent:  0,
		MemoryHardLimitPercent:  0,
		SwapSoftLimitBytes:      0,
		SwapHardLimitBytes:      0,
		SwapSoftLimitPercent:    0,
		SwapHardLimitPercent:    0,
		CPUPercentSoftLimit:     0,
		CPUPercentHardLimit:     0,
		LoadSoftLimitMultiplier: 0,
		LoadHardLimitMultiplier: 0,
	}

	now := time.Now()

	tests := []struct {
		name       string
		cfgMod     func(*Config)
		snapshot   Snapshot
		wantState  State
		wantReason string
	}{
		{
			name: "QueueSoftLimit",
			cfgMod: func(cfg *Config) {
				cfg.QueueSoftLimit = 5
				cfg.QueueHardLimit = 10
			},
			snapshot: Snapshot{
				QueueProducerInflight: 5,
				CollectedAt:           now,
			},
			wantState:  StateSoftArm,
			wantReason: "queue_inflight_soft",
		},
		{
			name: "QueueHardLimit",
			cfgMod: func(cfg *Config) {
				cfg.QueueSoftLimit = 0
				cfg.QueueHardLimit = 6
			},
			snapshot: Snapshot{
				QueueProducerInflight: 6,
				CollectedAt:           now,
			},
			wantState:  StateEngaged,
			wantReason: "queue_inflight_hard",
		},
		{
			name: "QueueConsumerSoftLimit",
			cfgMod: func(cfg *Config) {
				cfg.QueueConsumerSoftLimit = 3
				cfg.QueueConsumerHardLimit = 8
			},
			snapshot: Snapshot{
				QueueConsumerInflight: 3,
				CollectedAt:           now,
			},
			wantState:  StateSoftArm,
			wantReason: "queue_consumer_soft",
		},
		{
			name: "QueueConsumerHardLimit",
			cfgMod: func(cfg *Config) {
				cfg.QueueConsumerSoftLimit = 0
				cfg.QueueConsumerHardLimit = 4
			},
			snapshot: Snapshot{
				QueueConsumerInflight: 4,
				CollectedAt:           now,
			},
			wantState:  StateEngaged,
			wantReason: "queue_consumer_hard",
		},
		{
			name: "LockSoftLimit",
			cfgMod: func(cfg *Config) {
				cfg.LockSoftLimit = 3
				cfg.LockHardLimit = 10
			},
			snapshot: Snapshot{
				LockInflight: 3,
				CollectedAt:  now,
			},
			wantState:  StateSoftArm,
			wantReason: "lock_inflight_soft",
		},
		{
			name: "LockHardLimit",
			cfgMod: func(cfg *Config) {
				cfg.LockSoftLimit = 0
				cfg.LockHardLimit = 4
			},
			snapshot: Snapshot{
				LockInflight: 4,
				CollectedAt:  now,
			},
			wantState:  StateEngaged,
			wantReason: "lock_inflight_hard",
		},
		{
			name: "MemoryPercentSoftLimit",
			cfgMod: func(cfg *Config) {
				cfg.MemorySoftLimitPercent = 70
				cfg.MemoryHardLimitPercent = 85
			},
			snapshot: Snapshot{
				SystemMemoryUsedPercent:         72,
				SystemMemoryIncludesReclaimable: true,
				CollectedAt:                     now,
			},
			wantState:  StateSoftArm,
			wantReason: "memory_soft",
		},
		{
			name: "MemoryPercentHardLimit",
			cfgMod: func(cfg *Config) {
				cfg.MemorySoftLimitPercent = 0
				cfg.MemoryHardLimitPercent = 80
			},
			snapshot: Snapshot{
				SystemMemoryUsedPercent:         85,
				SystemMemoryIncludesReclaimable: true,
				CollectedAt:                     now,
			},
			wantState:  StateEngaged,
			wantReason: "memory_hard",
		},
		{
			name: "MemoryBytesSoftLimit",
			cfgMod: func(cfg *Config) {
				cfg.MemorySoftLimitBytes = 512
				cfg.MemoryHardLimitBytes = 2048
			},
			snapshot: Snapshot{
				RSSBytes:    600,
				CollectedAt: now,
			},
			wantState:  StateSoftArm,
			wantReason: "memory_soft",
		},
		{
			name: "MemoryBytesHardLimit",
			cfgMod: func(cfg *Config) {
				cfg.MemorySoftLimitBytes = 0
				cfg.MemoryHardLimitBytes = 500
			},
			snapshot: Snapshot{
				RSSBytes:    600,
				CollectedAt: now,
			},
			wantState:  StateEngaged,
			wantReason: "memory_hard",
		},
		{
			name: "SwapPercentSoftLimit",
			cfgMod: func(cfg *Config) {
				cfg.SwapSoftLimitPercent = 15
				cfg.SwapHardLimitPercent = 30
			},
			snapshot: Snapshot{
				SystemSwapUsedPercent: 18,
				CollectedAt:           now,
			},
			wantState:  StateSoftArm,
			wantReason: "swap_soft",
		},
		{
			name: "SwapPercentHardLimit",
			cfgMod: func(cfg *Config) {
				cfg.SwapSoftLimitPercent = 0
				cfg.SwapHardLimitPercent = 40
			},
			snapshot: Snapshot{
				SystemSwapUsedPercent: 42,
				CollectedAt:           now,
			},
			wantState:  StateEngaged,
			wantReason: "swap_hard",
		},
		{
			name: "SwapBytesSoftLimit",
			cfgMod: func(cfg *Config) {
				cfg.SwapSoftLimitBytes = 128
				cfg.SwapHardLimitBytes = 1024
			},
			snapshot: Snapshot{
				SwapBytes:   256,
				CollectedAt: now,
			},
			wantState:  StateSoftArm,
			wantReason: "swap_soft",
		},
		{
			name: "SwapBytesHardLimit",
			cfgMod: func(cfg *Config) {
				cfg.SwapSoftLimitBytes = 0
				cfg.SwapHardLimitBytes = 256
			},
			snapshot: Snapshot{
				SwapBytes:   300,
				CollectedAt: now,
			},
			wantState:  StateEngaged,
			wantReason: "swap_hard",
		},
		{
			name: "CPUPercentSoftLimit",
			cfgMod: func(cfg *Config) {
				cfg.CPUPercentSoftLimit = 55
				cfg.CPUPercentHardLimit = 90
			},
			snapshot: Snapshot{
				SystemCPUPercent: 60,
				CollectedAt:      now,
			},
			wantState:  StateSoftArm,
			wantReason: "cpu_soft",
		},
		{
			name: "CPUPercentHardLimit",
			cfgMod: func(cfg *Config) {
				cfg.CPUPercentSoftLimit = 0
				cfg.CPUPercentHardLimit = 95
			},
			snapshot: Snapshot{
				SystemCPUPercent: 97,
				CollectedAt:      now,
			},
			wantState:  StateEngaged,
			wantReason: "cpu_hard",
		},
		{
			name: "LoadMultiplierSoftLimit",
			cfgMod: func(cfg *Config) {
				cfg.LoadSoftLimitMultiplier = 2
				cfg.LoadHardLimitMultiplier = 10
			},
			snapshot: Snapshot{
				Load1Multiplier: 3,
				CollectedAt:     now,
			},
			wantState:  StateSoftArm,
			wantReason: "load_soft",
		},
		{
			name: "LoadMultiplierHardLimit",
			cfgMod: func(cfg *Config) {
				cfg.LoadSoftLimitMultiplier = 0
				cfg.LoadHardLimitMultiplier = 4
			},
			snapshot: Snapshot{
				Load1Multiplier: 5,
				CollectedAt:     now,
			},
			wantState:  StateEngaged,
			wantReason: "load_hard",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseCfg
			if tc.cfgMod != nil {
				tc.cfgMod(&cfg)
			}
			ctrl := NewController(cfg)
			ctrl.Observe(tc.snapshot)
			if got := ctrl.State(); got != tc.wantState {
				t.Fatalf("expected state %s, got %s", tc.wantState, got)
			}
			if status := ctrl.Status(); status.Reason != tc.wantReason {
				t.Fatalf("expected reason %q, got %q", tc.wantReason, status.Reason)
			}
		})
	}
}

func TestControllerMemoryStrictHeadroom(t *testing.T) {
	ctrl := NewController(Config{
		Enabled:                     true,
		MemorySoftLimitPercent:      80,
		MemoryHardLimitPercent:      90,
		MemoryStrictHeadroomPercent: 15,
		RecoverySamples:             1,
		Logger:                      pslog.NoopLogger(),
	})

	now := time.Now()

	ctrl.Observe(Snapshot{
		SystemMemoryUsedPercent:         85,
		SystemMemoryIncludesReclaimable: false,
		CollectedAt:                     now,
	})
	if state := ctrl.State(); state != StateDisengaged {
		t.Fatalf("expected disengaged state below effective limit, got %s", state)
	}

	ctrl.Observe(Snapshot{
		SystemMemoryUsedPercent:         97,
		SystemMemoryIncludesReclaimable: false,
		CollectedAt:                     now.Add(5 * time.Millisecond),
	})
	status := ctrl.Status()
	if status.State != StateSoftArm || status.Reason != "memory_soft" {
		t.Fatalf("expected soft arm with reason memory_soft, got state=%s reason=%s", status.State, status.Reason)
	}

	ctrl.Observe(Snapshot{
		SystemMemoryUsedPercent:         108,
		SystemMemoryIncludesReclaimable: false,
		CollectedAt:                     now.Add(10 * time.Millisecond),
	})
	status = ctrl.Status()
	if status.State != StateEngaged || status.Reason != "memory_hard" {
		t.Fatalf("expected engaged with reason memory_hard, got state=%s reason=%s", status.State, status.Reason)
	}
}
