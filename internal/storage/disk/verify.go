package disk

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
)

// Check represents a verification step outcome.
type Check struct {
	Name string
	Err  error
}

// Verify exercises the disk backend for concurrency safety and basic IO. It
// creates two independent Store instances (mirroring two lockd replicas),
// coordinates updates through the shared lockfiles, and ensures that CAS
// semantics prevent concurrent writers from corrupting metadata or payloads.
func Verify(ctx context.Context, cfg Config) []Check {
	result := []Check{}
	store1, err := New(cfg)
	if err != nil {
		return append(result, Check{Name: "InitPrimary", Err: err})
	}
	defer store1.Close()
	store2, err := New(cfg)
	if err != nil {
		return append(result, Check{Name: "InitReplica", Err: err})
	}
	defer store2.Close()

	key := "lockd-verify-" + uuidv7.NewString()

	var baseMetaETag string

	checks := []struct {
		name string
		fn   func() error
	}{
		{
			name: "CreateMeta",
			fn: func() error {
				meta := storage.Meta{Version: 1, UpdatedAtUnix: time.Now().Unix()}
				tag, err := store1.StoreMeta(ctx, key, &meta, "")
				if err != nil {
					return err
				}
				baseMetaETag = tag
				return nil
			},
		},
		{
			name: "ConcurrentMetaCAS",
			fn: func() error {
				metaA := storage.Meta{Version: 2, UpdatedAtUnix: time.Now().Unix()}
				metaB := storage.Meta{Version: 3, UpdatedAtUnix: time.Now().Unix()}
				var wg sync.WaitGroup
				wg.Add(2)
				errs := make(chan error, 2)
				go func() {
					defer wg.Done()
					_, err := store1.StoreMeta(ctx, key, &metaA, baseMetaETag)
					errs <- err
				}()
				go func() {
					defer wg.Done()
					_, err := store2.StoreMeta(ctx, key, &metaB, baseMetaETag)
					errs <- err
				}()
				wg.Wait()
				close(errs)
				success := 0
				for err := range errs {
					if err == nil {
						success++
					} else if !errors.Is(err, storage.ErrCASMismatch) {
						return err
					}
				}
				if success != 1 {
					return fmt.Errorf("expected 1 successful meta update, got %d", success)
				}
				return nil
			},
		},
		{
			name: "ConcurrentStateCAS",
			fn: func() error {
				res, err := store1.WriteState(ctx, key, strings.NewReader("one"), storage.PutStateOptions{})
				if err != nil {
					return err
				}
				var wg sync.WaitGroup
				wg.Add(2)
				errs := make(chan error, 2)
				go func() {
					defer wg.Done()
					_, err := store1.WriteState(ctx, key, strings.NewReader("alpha"), storage.PutStateOptions{ExpectedETag: res.NewETag})
					errs <- err
				}()
				go func() {
					defer wg.Done()
					_, err := store2.WriteState(ctx, key, strings.NewReader("beta"), storage.PutStateOptions{ExpectedETag: res.NewETag})
					errs <- err
				}()
				wg.Wait()
				close(errs)
				success := 0
				for err := range errs {
					if err == nil {
						success++
					} else if !errors.Is(err, storage.ErrCASMismatch) {
						return err
					}
				}
				if success != 1 {
					return fmt.Errorf("expected 1 successful state update, got %d", success)
				}
				return nil
			},
		},
		{
			name: "Cleanup",
			fn: func() error {
				if err := store1.RemoveState(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
					return err
				}
				if err := store1.DeleteMeta(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
					return err
				}
				return nil
			},
		},
	}

	for _, check := range checks {
		err := check.fn()
		result = append(result, Check{Name: check.name, Err: err})
	}
	return result
}
