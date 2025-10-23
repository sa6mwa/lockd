//go:build integration && mem && lq

package memlq

import "testing"

func TestMemLQOnly(t *testing.T) {
	for _, mode := range memQueueModes {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			RunMemLQScenarios(t, mode)
		})
	}
}
