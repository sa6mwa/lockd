package storagecheck

import (
	"context"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/disk"
)

// verifyDisk executes the disk backend verification routine and adapts the
// results to the diagnostics Result structure.
func verifyDisk(ctx context.Context, diskCfg disk.Config, root string, crypto *storage.Crypto) (Result, error) {
	checks := disk.Verify(ctx, diskCfg)
	result := Result{Provider: "disk", Path: root}
	result.Checks = make([]CheckResult, 0, len(checks))
	for _, chk := range checks {
		result.Checks = append(result.Checks, CheckResult{Name: chk.Name, Err: chk.Err})
	}
	if crypto != nil && crypto.Enabled() {
		store, err := disk.New(diskCfg)
		if err != nil {
			result.Checks = append(result.Checks, CheckResult{Name: "CryptoInit", Err: err})
			return result, nil
		}
		defer store.Close()
		result.Checks = append(result.Checks, CheckResult{
			Name: "CryptoMetaStateRoundTrip",
			Err:  verifyMetaStateDecryption(ctx, store, crypto),
		})
		result.Checks = append(result.Checks, CheckResult{
			Name: "CryptoQueueRoundTrip",
			Err:  verifyQueueEncryption(ctx, store, crypto),
		})
	}
	return result, nil
}
