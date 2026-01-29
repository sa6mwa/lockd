package disk

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type fsyncRequest struct {
	file *os.File
	done chan error
}

type fsyncBatcher struct {
	mu           sync.Mutex
	queue        []*fsyncRequest
	batchEntries int
	batchRunning bool
	commitMaxOps int
	batchDelay   time.Duration

	batchStats fsyncBatchStats
}

// FsyncBatchStats captures aggregate fsync batch behaviour for diagnostics.
type FsyncBatchStats struct {
	TotalBatches  uint64
	TotalRequests uint64
	MaxBatchSize  uint64
	TotalSyncNs   uint64
	MaxSyncNs     uint64
	Bounds        []int
	Counts        []uint64
}

type fsyncBatchStats struct {
	totalBatches  uint64
	totalRequests uint64
	maxBatchSize  uint64
	totalSyncNs   uint64
	maxSyncNs     uint64
	buckets       [len(fsyncBatchBounds) + 1]uint64
}

var fsyncBatchBounds = [...]int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}

const defaultFsyncBatchDelay = 2 * time.Millisecond

func newFsyncBatcher(maxOps int) *fsyncBatcher {
	return &fsyncBatcher{
		commitMaxOps: maxOps,
		batchDelay:   defaultFsyncBatchDelay,
	}
}

func (b *fsyncBatcher) enqueue(file *os.File) <-chan error {
	ch := make(chan error, 1)
	b.mu.Lock()
	b.queue = append(b.queue, &fsyncRequest{file: file, done: ch})
	b.batchEntries++
	if !b.batchRunning {
		b.batchRunning = true
		go b.batchLoop()
	}
	b.mu.Unlock()
	return ch
}

func (b *fsyncBatcher) idle() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue) == 0 && !b.batchRunning
}

func (b *fsyncBatcher) batchLoop() {
	for {
		b.mu.Lock()
		if len(b.queue) == 0 {
			b.batchRunning = false
			b.mu.Unlock()
			return
		}
		delay := b.batchDelay
		if b.commitMaxOps > 0 && len(b.queue) >= b.commitMaxOps {
			delay = 0
		}
		b.mu.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}
		b.processBatch()
	}
}

func (b *fsyncBatcher) processBatch() {
	b.mu.Lock()
	if len(b.queue) == 0 {
		b.mu.Unlock()
		return
	}
	maxOps := b.commitMaxOps
	if maxOps <= 0 || maxOps > len(b.queue) {
		maxOps = len(b.queue)
	}
	batch := append([]*fsyncRequest(nil), b.queue[:maxOps]...)
	b.queue = append([]*fsyncRequest(nil), b.queue[maxOps:]...)
	b.batchEntries -= len(batch)
	if b.batchEntries < 0 {
		b.batchEntries = 0
	}
	b.mu.Unlock()

	b.recordBatch(len(batch))

	var err error
	start := time.Now()
	seen := make(map[*os.File]struct{})
	for _, req := range batch {
		if req.file == nil {
			continue
		}
		if _, ok := seen[req.file]; ok {
			continue
		}
		seen[req.file] = struct{}{}
		if syncErr := syncFile(req.file); syncErr != nil && err == nil {
			err = syncErr
		}
	}
	b.recordSync(time.Since(start))
	for _, req := range batch {
		req.done <- err
	}
}

func (b *fsyncBatcher) recordBatch(size int) {
	if size <= 0 {
		return
	}
	atomic.AddUint64(&b.batchStats.totalBatches, 1)
	atomic.AddUint64(&b.batchStats.totalRequests, uint64(size))
	for {
		current := atomic.LoadUint64(&b.batchStats.maxBatchSize)
		if uint64(size) <= current {
			break
		}
		if atomic.CompareAndSwapUint64(&b.batchStats.maxBatchSize, current, uint64(size)) {
			break
		}
	}
	idx := len(fsyncBatchBounds)
	for i, bound := range fsyncBatchBounds {
		if size <= bound {
			idx = i
			break
		}
	}
	atomic.AddUint64(&b.batchStats.buckets[idx], 1)
}

func (b *fsyncBatcher) recordSync(d time.Duration) {
	if d <= 0 {
		return
	}
	atomic.AddUint64(&b.batchStats.totalSyncNs, uint64(d.Nanoseconds()))
	for {
		current := atomic.LoadUint64(&b.batchStats.maxSyncNs)
		if uint64(d.Nanoseconds()) <= current {
			break
		}
		if atomic.CompareAndSwapUint64(&b.batchStats.maxSyncNs, current, uint64(d.Nanoseconds())) {
			break
		}
	}
}

func (b *fsyncBatcher) stats() FsyncBatchStats {
	stats := FsyncBatchStats{
		TotalBatches:  atomic.LoadUint64(&b.batchStats.totalBatches),
		TotalRequests: atomic.LoadUint64(&b.batchStats.totalRequests),
		MaxBatchSize:  atomic.LoadUint64(&b.batchStats.maxBatchSize),
		TotalSyncNs:   atomic.LoadUint64(&b.batchStats.totalSyncNs),
		MaxSyncNs:     atomic.LoadUint64(&b.batchStats.maxSyncNs),
		Bounds:        make([]int, len(fsyncBatchBounds)),
		Counts:        make([]uint64, len(fsyncBatchBounds)+1),
	}
	copy(stats.Bounds, fsyncBatchBounds[:])
	for i := range stats.Counts {
		stats.Counts[i] = atomic.LoadUint64(&b.batchStats.buckets[i])
	}
	return stats
}

type manifestState struct {
	path   string
	mu     sync.Mutex
	offset int64
}

func (m *manifestState) appendSegment(op, name string) error {
	if m == nil || m.path == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	f, err := os.OpenFile(m.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := lockFile(f); err != nil {
		return err
	}
	defer unlockFile(f)
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "%s %s\n", op, name); err != nil {
		return err
	}
	return nil
}

func (m *manifestState) readNewSegments() ([]string, error) {
	if m == nil || m.path == "" {
		return nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	f, err := os.Open(m.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	if m.offset > 0 {
		if _, err := f.Seek(m.offset, io.SeekStart); err != nil {
			return nil, err
		}
	}
	reader := bufio.NewReader(f)
	var segments []string
	var consumed int64
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		consumed += int64(len(line))
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			continue
		}
		segments = append(segments, strings.TrimSpace(parts[1]))
	}
	m.offset += consumed
	return segments, nil
}
