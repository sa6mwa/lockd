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

var fsyncBatchBounds = [...]int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096}

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

// drained reports whether there is no queued fsync work.
// This intentionally ignores batchRunning because the loop can still be
// unwinding after delivering completion to waiters.
func (b *fsyncBatcher) drained() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue) == 0
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
	path              string
	mu                sync.Mutex
	offset            int64
	segmentSealed     map[string]bool
	sawSegmentOpen    bool
	sawSegmentSeal    bool
	currentSnapshot   string
	obsoleteSegments  map[string]time.Time
	obsoleteSnapshots map[string]time.Time
}

type manifestEntry struct {
	op        string
	name      string
	timestamp time.Time
}

type manifestChanges struct {
	segmentStates     map[string]bool
	sawSegmentOpen    bool
	sawSegmentSeal    bool
	snapshotChanged   bool
	currentSnapshot   string
	obsoleteChanged   bool
	obsoleteSegments  map[string]time.Time
	obsoleteSnapshots map[string]time.Time
}

func (m *manifestState) appendEntries(entries ...manifestEntry) error {
	if m == nil || m.path == "" {
		return nil
	}
	if len(entries) == 0 {
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
	for _, entry := range entries {
		if strings.TrimSpace(entry.name) == "" {
			continue
		}
		switch entry.op {
		case "open", "seal", "snapshot-install":
			if _, err := fmt.Fprintf(f, "%s %s\n", entry.op, entry.name); err != nil {
				return err
			}
		case "obsolete-segment", "obsolete-snapshot":
			if _, err := fmt.Fprintf(f, "%s %s %d\n", entry.op, entry.name, entry.timestamp.Unix()); err != nil {
				return err
			}
		default:
			return fmt.Errorf("disk: logstore unknown manifest op %q", entry.op)
		}
	}
	return nil
}

func (m *manifestState) appendSegment(op, name string) error {
	return m.appendEntries(manifestEntry{op: op, name: name})
}

func (m *manifestState) readChanges() (manifestChanges, error) {
	var changes manifestChanges
	if m == nil || m.path == "" {
		return changes, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	f, err := os.Open(m.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return changes, nil
		}
		return changes, err
	}
	defer f.Close()
	if m.offset > 0 {
		if _, err := f.Seek(m.offset, io.SeekStart); err != nil {
			return changes, err
		}
	}
	reader := bufio.NewReader(f)
	var consumed int64
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return changes, err
		}
		consumed += int64(len(line))
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		op := parts[0]
		name := strings.TrimSpace(parts[1])
		switch op {
		case "open":
			if m.segmentSealed == nil {
				m.segmentSealed = make(map[string]bool)
			}
			m.sawSegmentOpen = true
			m.segmentSealed[name] = false
		case "seal":
			if m.segmentSealed == nil {
				m.segmentSealed = make(map[string]bool)
			}
			m.sawSegmentSeal = true
			m.segmentSealed[name] = true
		case "snapshot-install":
			if m.currentSnapshot != name {
				m.currentSnapshot = name
				changes.snapshotChanged = true
			}
		case "obsolete-segment":
			if len(parts) < 3 {
				continue
			}
			ts, parseErr := parseManifestUnix(parts[2])
			if parseErr != nil {
				continue
			}
			if m.obsoleteSegments == nil {
				m.obsoleteSegments = make(map[string]time.Time)
			}
			if prev, ok := m.obsoleteSegments[name]; !ok || !prev.Equal(ts) {
				m.obsoleteSegments[name] = ts
				changes.obsoleteChanged = true
			}
		case "obsolete-snapshot":
			if len(parts) < 3 {
				continue
			}
			ts, parseErr := parseManifestUnix(parts[2])
			if parseErr != nil {
				continue
			}
			if m.obsoleteSnapshots == nil {
				m.obsoleteSnapshots = make(map[string]time.Time)
			}
			if prev, ok := m.obsoleteSnapshots[name]; !ok || !prev.Equal(ts) {
				m.obsoleteSnapshots[name] = ts
				changes.obsoleteChanged = true
			}
		}
	}
	m.offset += consumed
	changes.segmentStates = copyBoolMap(m.segmentSealed)
	changes.sawSegmentOpen = m.sawSegmentOpen
	changes.sawSegmentSeal = m.sawSegmentSeal
	changes.currentSnapshot = m.currentSnapshot
	changes.obsoleteSegments = copyTimeMap(m.obsoleteSegments)
	changes.obsoleteSnapshots = copyTimeMap(m.obsoleteSnapshots)
	return changes, nil
}

func parseManifestUnix(raw string) (time.Time, error) {
	secs, err := parseInt64(raw)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(secs, 0).UTC(), nil
}

func parseInt64(raw string) (int64, error) {
	var val int64
	_, err := fmt.Sscanf(strings.TrimSpace(raw), "%d", &val)
	return val, err
}

func copyTimeMap(src map[string]time.Time) map[string]time.Time {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]time.Time, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func copyBoolMap(src map[string]bool) map[string]bool {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]bool, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
