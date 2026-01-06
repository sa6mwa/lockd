package core

import (
	"sync/atomic"
	"time"

	"pkt.systems/pslog"
)

func startTxnWatchdog(logger pslog.Logger, name string, threshold time.Duration, fields ...any) func() {
	if logger == nil || threshold <= 0 {
		return func() {}
	}
	start := time.Now()
	var fired atomic.Bool
	timer := time.AfterFunc(threshold, func() {
		fired.Store(true)
		logger.Warn(name, append(fields,
			"elapsed_ms", time.Since(start).Milliseconds(),
			"threshold_ms", threshold.Milliseconds(),
		)...)
	})
	return func() {
		if !timer.Stop() && fired.Load() {
			return
		}
	}
}
