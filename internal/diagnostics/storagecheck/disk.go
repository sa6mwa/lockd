package storagecheck

import (
	"context"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/storage/disk"
)

// verifyDisk executes the disk backend verification routine and adapts the
// results to the diagnostics Result structure.
func verifyDisk(ctx context.Context, cfg lockd.Config) (Result, error) {
	diskCfg, root, err := lockd.BuildDiskConfig(cfg)
	if err != nil {
		return Result{}, err
	}
	checks := disk.Verify(ctx, diskCfg)
	result := Result{Provider: "disk", Path: root}
	result.Checks = make([]CheckResult, 0, len(checks))
	for _, chk := range checks {
		result.Checks = append(result.Checks, CheckResult{Name: chk.Name, Err: chk.Err})
	}
	return result, nil
}
