//go:build integration && disk && bench

package diskintegration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/disk"
)

const (
	diskLargeJSONSize      = 5 << 20
	diskSmallJSONSize      = 512
	diskSmallJSONPerBatch  = 8
	diskBenchmarkLeaseTTL  = 60
	diskBenchmarkBlockSecs = 5
)

type diskBenchmarkEnv struct {
	root   string
	cfg    lockd.Config
	store  *disk.Store
	client *lockdclient.Client
}

func setupDiskBenchmarkEnv(b *testing.B, root string, withLockd bool) *diskBenchmarkEnv {
	b.Helper()

	if root == "" {
		b.Skip("disk root unavailable")
	}

	if err := os.MkdirAll(root, 0o755); err != nil {
		b.Fatalf("mkdir root: %v", err)
	}
	b.Cleanup(func() { _ = os.RemoveAll(root) })

	if withLockd {
		cfg := buildDiskConfig(b, root, 0)
		cli := startDiskServer(b, cfg)
		return &diskBenchmarkEnv{
			root:   root,
			cfg:    cfg,
			client: cli,
		}
	}

	store, err := disk.New(disk.Config{Root: root})
	if err != nil {
		b.Fatalf("disk store: %v", err)
	}
	b.Cleanup(func() { _ = store.Close() })
	return &diskBenchmarkEnv{
		root:  root,
		store: store,
	}
}

func BenchmarkDiskRawLargeJSON(b *testing.B) {
	root := prepareDiskRoot(b, "")
	env := setupDiskBenchmarkEnv(b, root, false)
	payload := makeDiskLargeJSON()
	ctx := context.Background()

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextDiskKey("raw-large", i)
		if _, err := env.store.WriteState(ctx, key, bytes.NewReader(payload), storage.PutStateOptions{}); err != nil {
			b.Fatalf("write state: %v", err)
		}
		if err := env.store.RemoveState(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			b.Fatalf("remove state: %v", err)
		}
	}
}

func BenchmarkLockdDiskLargeJSON(b *testing.B) {
	root := prepareDiskRoot(b, "")
	env := setupDiskBenchmarkEnv(b, root, true)
	payload := makeDiskLargeJSON()
	ctx := context.Background()
	cli := env.client

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextDiskKey("lockd-large", i)
		lease := acquireWithRetry(b, ctx, cli, key, "bench-large", diskBenchmarkLeaseTTL, diskBenchmarkBlockSecs)
		opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
		if _, err := cli.UpdateStateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
			b.Fatalf("update state: %v", err)
		}
		if _, err := cli.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
	}
}

func BenchmarkLockdDiskLargeJSONStream(b *testing.B) {
	root := prepareDiskRoot(b, "")
	env := setupDiskBenchmarkEnv(b, root, true)
	ctx := context.Background()
	cli := env.client

	b.ReportAllocs()
	b.SetBytes(diskLargeJSONSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextDiskKey("lockd-large-stream", i)
		lease := acquireWithRetry(b, ctx, cli, key, "bench-large-stream", diskBenchmarkLeaseTTL, diskBenchmarkBlockSecs)
		opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
		stream := newDiskJSONStream(diskLargeJSONSize)
		if _, err := cli.UpdateState(ctx, key, lease.LeaseID, stream, opts); err != nil {
			b.Fatalf("update state stream: %v", err)
		}
		if _, err := cli.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
	}
}

func BenchmarkDiskRawSmallJSON(b *testing.B) {
	root := prepareDiskRoot(b, "")
	env := setupDiskBenchmarkEnv(b, root, false)
	batch := makeDiskSmallJSONBatch()
	ctx := context.Background()

	b.ReportAllocs()
	if len(batch) > 0 {
		b.SetBytes(int64(len(batch[0])))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for idx, payload := range batch {
			key := nextDiskKey(fmt.Sprintf("raw-small-%d", idx), i)
			if _, err := env.store.WriteState(ctx, key, bytes.NewReader(payload), storage.PutStateOptions{}); err != nil {
				b.Fatalf("write state: %v", err)
			}
			if err := env.store.RemoveState(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				b.Fatalf("remove state: %v", err)
			}
		}
	}
}

func BenchmarkLockdDiskSmallJSON(b *testing.B) {
	root := prepareDiskRoot(b, "")
	env := setupDiskBenchmarkEnv(b, root, true)
	batch := makeDiskSmallJSONBatch()
	ctx := context.Background()
	cli := env.client

	b.ReportAllocs()
	if len(batch) > 0 {
		b.SetBytes(int64(len(batch[0])))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextDiskKey("lockd-small", i)
		lease := acquireWithRetry(b, ctx, cli, key, "bench-small", diskBenchmarkLeaseTTL, diskBenchmarkBlockSecs)
		version := strconv.FormatInt(lease.Version, 10)
		for _, payload := range batch {
			if _, err := cli.UpdateStateBytes(ctx, key, lease.LeaseID, payload, lockdclient.UpdateStateOptions{IfVersion: version}); err != nil {
				b.Fatalf("update state: %v", err)
			}
			version = ""
		}
		if _, err := cli.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
	}
}

func BenchmarkLockdDiskSmallJSONStream(b *testing.B) {
	root := prepareDiskRoot(b, "")
	env := setupDiskBenchmarkEnv(b, root, true)
	ctx := context.Background()
	cli := env.client

	b.ReportAllocs()
	b.SetBytes(diskSmallJSONSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextDiskKey("lockd-small-stream", i)
		lease := acquireWithRetry(b, ctx, cli, key, "bench-small-stream", diskBenchmarkLeaseTTL, diskBenchmarkBlockSecs)
		stream := newDiskJSONStream(diskSmallJSONSize)
		if _, err := cli.UpdateState(ctx, key, lease.LeaseID, stream, lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}); err != nil {
			b.Fatalf("update state stream: %v", err)
		}
		if _, err := cli.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
	}
}

func BenchmarkDiskRawConcurrent(b *testing.B) {
	root := prepareDiskRoot(b, "")
	env := setupDiskBenchmarkEnv(b, root, false)
	payload := makeDiskSmallJSONBatch()[0]
	ctx := context.Background()
	var counter uint64

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddUint64(&counter, 1)
			key := fmt.Sprintf("raw-concurrent-%d", id)
			if _, err := env.store.WriteState(ctx, key, bytes.NewReader(payload), storage.PutStateOptions{}); err != nil {
				b.Fatalf("write state: %v", err)
			}
			if err := env.store.RemoveState(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				b.Fatalf("remove state: %v", err)
			}
		}
	})
}

func BenchmarkLockdDiskConcurrent(b *testing.B) {
	root := prepareDiskRoot(b, "")
	env := setupDiskBenchmarkEnv(b, root, true)
	payload := makeDiskSmallJSONBatch()[0]
	ctx := context.Background()
	cli := env.client
	var counter uint64

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddUint64(&counter, 1)
			key := nextDiskKey("lockd-concurrent", int(id))
			owner := fmt.Sprintf("worker-%d", id)
			lease := acquireWithRetry(b, ctx, cli, key, owner, diskBenchmarkLeaseTTL, diskBenchmarkBlockSecs)
			opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
			if _, err := cli.UpdateStateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
				b.Fatalf("update state: %v", err)
			}
			if _, err := cli.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
				b.Fatalf("release: %v", err)
			}
		}
	})
}

func BenchmarkLockdDiskLargeJSONNFS(b *testing.B) {
	root := prepareDiskRoot(b, nfsBasePath())
	if root == "" {
		b.Skip("nfs root unavailable")
	}
	env := setupDiskBenchmarkEnv(b, root, true)
	payload := makeDiskLargeJSON()
	ctx := context.Background()
	cli := env.client

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextDiskKey("lockd-large-nfs", i)
		lease := acquireWithRetry(b, ctx, cli, key, "bench-large-nfs", diskBenchmarkLeaseTTL, diskBenchmarkBlockSecs)
		opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
		if _, err := cli.UpdateStateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
			b.Fatalf("update state: %v", err)
		}
		if _, err := cli.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
	}
}

func makeDiskLargeJSON() []byte {
	payload := map[string]any{
		"records": strings.Repeat("X", diskLargeJSONSize),
	}
	out, _ := json.Marshal(payload)
	return out
}

func makeDiskSmallJSONBatch() [][]byte {
	batch := make([][]byte, 0, diskSmallJSONPerBatch)
	for i := 0; i < diskSmallJSONPerBatch; i++ {
		payload := map[string]any{
			"idx":       i,
			"timestamp": time.Now().UnixNano(),
			"value":     strings.Repeat("v", diskSmallJSONSize-48),
		}
		data, _ := json.Marshal(payload)
		batch = append(batch, data)
	}
	return batch
}

type diskJSONStream struct {
	prefix    []byte
	remaining int64
	suffix    []byte
	stage     int
}

func newDiskJSONStream(bodyLen int64) io.Reader {
	return &diskJSONStream{
		prefix:    []byte(`{"payload":"`),
		remaining: bodyLen,
		suffix:    []byte(`"}`),
	}
}

func (s *diskJSONStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var total int
	for len(p) > 0 {
		switch s.stage {
		case 0:
			if len(s.prefix) == 0 {
				s.stage = 1
				continue
			}
			n := copy(p, s.prefix)
			s.prefix = s.prefix[n:]
			total += n
			p = p[n:]
		case 1:
			if s.remaining == 0 {
				s.stage = 2
				continue
			}
			chunk := int64(len(p))
			if chunk > s.remaining {
				chunk = s.remaining
			}
			for i := 0; i < int(chunk); i++ {
				p[i] = 'x'
			}
			s.remaining -= chunk
			total += int(chunk)
			p = p[chunk:]
		case 2:
			if len(s.suffix) == 0 {
				s.stage = 3
				continue
			}
			n := copy(p, s.suffix)
			s.suffix = s.suffix[n:]
			total += n
			p = p[n:]
		case 3:
			if total == 0 {
				return 0, io.EOF
			}
			return total, nil
		}
	}
	return total, nil
}

func nextDiskKey(prefix string, idx int) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), idx)
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}
