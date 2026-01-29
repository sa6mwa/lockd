package disk

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pkt.systems/kryptograf"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
)

type logStore struct {
	root             string
	now              func() time.Time
	crypto           *storage.Crypto
	writerID         string
	commitMaxOps     int
	segmentSize      int64
	closeAfterCommit bool
	markerTouchEvery time.Duration
	markerScanEvery  time.Duration
	singleWriter     atomic.Bool
	inlineBufPool    sync.Pool
	inlineRecordPool sync.Pool
	metaBufPool      sync.Pool
	readFileMu       sync.Mutex
	readFileLRU      *list.List
	readFiles        map[string]*cachedReadFile
	readFileMax      int
	mu               sync.Mutex
	namespaces       map[string]*logNamespace
}

func (s *logStore) FsyncStats() FsyncBatchStats {
	if s == nil {
		return FsyncBatchStats{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var merged fsyncBatchStats
	for _, ns := range s.namespaces {
		if ns == nil || ns.batcher == nil {
			continue
		}
		stats := ns.batcher.stats()
		merged.totalBatches += stats.TotalBatches
		merged.totalRequests += stats.TotalRequests
		if stats.MaxBatchSize > merged.maxBatchSize {
			merged.maxBatchSize = stats.MaxBatchSize
		}
		merged.totalSyncNs += stats.TotalSyncNs
		if stats.MaxSyncNs > merged.maxSyncNs {
			merged.maxSyncNs = stats.MaxSyncNs
		}
		for i := range merged.buckets {
			if i < len(stats.Counts) {
				merged.buckets[i] += stats.Counts[i]
			}
		}
	}
	return FsyncBatchStats{
		TotalBatches:  merged.totalBatches,
		TotalRequests: merged.totalRequests,
		MaxBatchSize:  merged.maxBatchSize,
		TotalSyncNs:   merged.totalSyncNs,
		MaxSyncNs:     merged.maxSyncNs,
		Bounds:        append([]int(nil), fsyncBatchBounds[:]...),
		Counts:        append([]uint64(nil), merged.buckets[:]...),
	}
}

func (s *logStore) setSingleWriter(enabled bool) {
	if s == nil {
		return
	}
	if s.singleWriter.Load() == enabled {
		return
	}
	s.singleWriter.Store(enabled)
	if enabled {
		s.markerTouchEvery = singleWriterTouchEvery
	} else {
		s.markerTouchEvery = defaultMarkerTouchEvery
	}
	s.mu.Lock()
	for _, ns := range s.namespaces {
		if ns != nil {
			ns.setSingleWriter()
		}
	}
	s.mu.Unlock()
}

type logNamespace struct {
	store        *logStore
	namespace    string
	logDir       string
	segmentsDir  string
	manifestDir  string
	snapshotsDir string
	markerDir    string
	markerSelf   string
	markerPath   string

	mu       sync.Mutex
	segments map[string]*logSegment
	active   *logSegment
	nextSeq  int
	manifest *manifestState

	metaIndex   map[string]*recordRef
	stateIndex  map[string]*recordRef
	objectIndex map[string]*recordRef

	sortedObjects []string

	batcher *fsyncBatcher

	appendCh             chan *appendRequest
	appendOnce           sync.Once
	writeMu              sync.Mutex
	pending              []*pendingRef
	pendingMeta          map[string]*pendingRef
	pendingState         map[string]*pendingRef
	pendingObject        map[string]*pendingRef
	appendBuf            []byte
	appendRefs           []*recordRef
	prefixBuf            []byte
	appendGroupSet       map[*storage.CommitGroup]struct{}
	appendGroupNeedsSync map[*storage.CommitGroup]struct{}
	refSlab              []recordRef
	refSlabIdx           int
	refSlabs             [][]recordRef

	pendingCounts map[*storage.CommitGroup]int
	committed     map[*storage.CommitGroup]struct{}
	noSyncEpoch   uint64
	syncedEpoch   uint64

	markerMu       sync.Mutex
	markerSnapshot map[string]markerInfo
	markerDirMod   time.Time
	markerSynced   bool
	markerTouchAt  time.Time
	markerScanAt   time.Time
	markerToggle   bool
	markerDirReady bool
	dirsReady      bool
}

type appendRequest struct {
	recType         logRecordType
	key             string
	meta            recordMeta
	record          []byte
	recordBuf       []byte
	payloadLen      int64
	payloadStartRel int64
	written         chan *recordRef
	done            chan error
	ready           chan struct{}
	group           *storage.CommitGroup
	noSync          bool
}

type logSegment struct {
	name       string
	path       string
	file       *os.File
	size       int64
	readOffset int64
	sealed     bool
	inFlight   int
	pending    int
}

type cachedReadFile struct {
	path string
	file *os.File
	refs int
	elem *list.Element
}

type recordRef struct {
	recType       logRecordType
	key           string
	meta          recordMeta
	segment       *logSegment
	offset        int64
	payloadOffset int64
	payloadLen    int64
	link          *recordLink
	cachedMeta    *storage.Meta
}

type recordLink struct {
	path   string
	offset int64
	length int64
}

type pendingRef struct {
	ref     *recordRef
	group   *storage.CommitGroup
	applied bool
	done    chan struct{}
	err     error
}

type markerInfo struct {
	modTime time.Time
	size    int64
}

const (
	maxInlinePayload        = 1 << 20
	payloadWriteBufferSize  = 128 << 10
	defaultMarkerTouchEvery = 0
	defaultMarkerScanEvery  = 1 * time.Second
	singleWriterTouchEvery  = 100 * time.Millisecond
	maxMetaBufPoolCap       = 64 << 10
	maxInlineRecordPoolCap  = 256 << 10
	maxAppendBufCap         = maxInlinePayload
	recordRefSlabSize       = 4096
	defaultReadFileCache    = 64
)

func newLogStore(root string, now func() time.Time, crypto *storage.Crypto, commitMaxOps int, segmentSize int64, closeAfterCommit bool) *logStore {
	writerID := strings.ReplaceAll(uuidv7.NewString(), "-", "")
	return &logStore{
		root:             root,
		now:              now,
		crypto:           crypto,
		writerID:         writerID,
		commitMaxOps:     commitMaxOps,
		segmentSize:      segmentSize,
		closeAfterCommit: closeAfterCommit,
		markerTouchEvery: defaultMarkerTouchEvery,
		markerScanEvery:  defaultMarkerScanEvery,
		inlineBufPool: sync.Pool{
			New: func() any {
				return &bytes.Buffer{}
			},
		},
		inlineRecordPool: sync.Pool{
			New: func() any {
				return make([]byte, 0, 1024)
			},
		},
		metaBufPool: sync.Pool{
			New: func() any {
				return make([]byte, 0, 256)
			},
		},
		readFileLRU: list.New(),
		readFiles:   make(map[string]*cachedReadFile),
		readFileMax: defaultReadFileCache,
		namespaces:  make(map[string]*logNamespace),
	}
}

func (s *logStore) acquireMetaBuf(size int) []byte {
	if size <= 0 {
		return nil
	}
	buf := s.metaBufPool.Get().([]byte)
	if cap(buf) < size {
		s.metaBufPool.Put(buf[:0]) //nolint:staticcheck // avoid extra allocation by pooling value slice
		return make([]byte, size)
	}
	return buf[:size]
}

func (s *logStore) releaseMetaBuf(buf []byte) {
	if len(buf) == 0 && cap(buf) == 0 {
		return
	}
	if cap(buf) > maxMetaBufPoolCap {
		return
	}
	s.metaBufPool.Put(buf[:0]) //nolint:staticcheck // avoid extra allocation by pooling value slice
}

func (s *logStore) acquireInlineRecordBuf(size int) []byte {
	if size <= 0 {
		return nil
	}
	if size > maxInlineRecordPoolCap {
		return make([]byte, size)
	}
	buf := s.inlineRecordPool.Get().([]byte)
	if cap(buf) < size {
		s.inlineRecordPool.Put(buf[:0]) //nolint:staticcheck // avoid extra allocation by pooling value slice
		return make([]byte, size)
	}
	return buf[:size]
}

func (s *logStore) releaseInlineRecordBuf(buf []byte) {
	if len(buf) == 0 && cap(buf) == 0 {
		return
	}
	if cap(buf) > maxInlineRecordPoolCap {
		return
	}
	s.inlineRecordPool.Put(buf[:0]) //nolint:staticcheck // avoid extra allocation by pooling value slice
}

func (s *logStore) acquireReadFile(path string) (*os.File, func(), error) {
	if path == "" {
		return nil, nil, fmt.Errorf("disk: logstore missing segment path")
	}
	s.readFileMu.Lock()
	if cached, ok := s.readFiles[path]; ok {
		cached.refs++
		s.readFileLRU.MoveToFront(cached.elem)
		s.readFileMu.Unlock()
		return cached.file, func() { s.releaseReadFile(path) }, nil
	}
	s.readFileMu.Unlock()

	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	s.readFileMu.Lock()
	if cached, ok := s.readFiles[path]; ok {
		cached.refs++
		s.readFileLRU.MoveToFront(cached.elem)
		s.readFileMu.Unlock()
		_ = file.Close()
		return cached.file, func() { s.releaseReadFile(path) }, nil
	}
	cached := &cachedReadFile{
		path: path,
		file: file,
		refs: 1,
	}
	cached.elem = s.readFileLRU.PushFront(cached)
	s.readFiles[path] = cached
	toClose := s.evictReadFilesLocked()
	s.readFileMu.Unlock()
	for _, closeFile := range toClose {
		_ = closeFile.Close()
	}
	return file, func() { s.releaseReadFile(path) }, nil
}

func (s *logStore) releaseReadFile(path string) {
	if path == "" {
		return
	}
	s.readFileMu.Lock()
	cached, ok := s.readFiles[path]
	if !ok {
		s.readFileMu.Unlock()
		return
	}
	if cached.refs > 0 {
		cached.refs--
	}
	toClose := s.evictReadFilesLocked()
	s.readFileMu.Unlock()
	for _, closeFile := range toClose {
		_ = closeFile.Close()
	}
}

func (s *logStore) evictReadFilesLocked() []*os.File {
	if s.readFileMax <= 0 {
		return nil
	}
	if len(s.readFiles) <= s.readFileMax {
		return nil
	}
	var toClose []*os.File
	for len(s.readFiles) > s.readFileMax {
		back := s.readFileLRU.Back()
		if back == nil {
			break
		}
		cached, ok := back.Value.(*cachedReadFile)
		if !ok {
			s.readFileLRU.Remove(back)
			continue
		}
		if cached.refs > 0 {
			break
		}
		delete(s.readFiles, cached.path)
		s.readFileLRU.Remove(back)
		toClose = append(toClose, cached.file)
	}
	return toClose
}

func (s *logStore) namespace(ns string) (*logNamespace, error) {
	ns = strings.TrimSpace(ns)
	if ns == "" {
		return nil, fmt.Errorf("disk: namespace required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.namespaces[ns]; ok {
		return existing, nil
	}
	logDir := filepath.Join(s.root, ns, "logstore")
	markerDir := filepath.Join(logDir, "markers")
	markerSelf := fmt.Sprintf("writer-%s.marker", s.writerID)
	ln := &logNamespace{
		store:         s,
		namespace:     ns,
		logDir:        logDir,
		segmentsDir:   filepath.Join(logDir, "segments"),
		manifestDir:   filepath.Join(logDir, "manifest"),
		snapshotsDir:  filepath.Join(logDir, "snapshots"),
		markerDir:     markerDir,
		markerSelf:    markerSelf,
		markerPath:    filepath.Join(markerDir, markerSelf),
		segments:      make(map[string]*logSegment),
		metaIndex:     make(map[string]*recordRef),
		stateIndex:    make(map[string]*recordRef),
		objectIndex:   make(map[string]*recordRef),
		batcher:       newFsyncBatcher(s.commitMaxOps),
		appendCh:      make(chan *appendRequest, 4096),
		markerTouchAt: time.Time{},
	}
	ln.appendOnce.Do(func() {
		go ln.appendLoop()
	})
	s.namespaces[ns] = ln
	return ln, nil
}

func (n *logNamespace) ensureDirs() error {
	if n.dirsReady {
		return nil
	}
	for _, dir := range []string{n.segmentsDir, n.manifestDir, n.snapshotsDir, n.markerDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("disk: logstore mkdir %q: %w", dir, err)
		}
	}
	n.dirsReady = true
	return nil
}

func (n *logNamespace) refresh() error {
	return n.refreshWithMode(false)
}

func (n *logNamespace) refreshForce() error {
	return n.refreshWithMode(true)
}

func (n *logNamespace) refreshWithMode(force bool) error {
	if err := n.ensureDirs(); err != nil {
		return err
	}
	if n.store != nil && n.store.singleWriter.Load() {
		n.markerMu.Lock()
		synced := n.markerSynced
		n.markerMu.Unlock()
		if synced && !force {
			return nil
		}
		n.mu.Lock()
		defer n.mu.Unlock()
		if err := n.loadManifestLocked(); err != nil {
			return err
		}
		if err := n.scanSegmentsLocked(); err != nil {
			return err
		}
		for _, seg := range n.orderedSegmentsLocked() {
			if err := n.readSegmentLocked(seg); err != nil {
				return err
			}
		}
		n.markerMu.Lock()
		n.markerSnapshot = nil
		n.markerDirMod = time.Time{}
		n.markerSynced = true
		n.markerMu.Unlock()
		return nil
	}
	var (
		snapshot map[string]markerInfo
		dirMod   time.Time
		err      error
	)
	if force {
		dirInfo, statErr := os.Stat(n.markerDir)
		if statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				if err := os.MkdirAll(n.markerDir, 0o755); err != nil {
					return err
				}
				dirInfo, statErr = os.Stat(n.markerDir)
			}
		}
		if statErr != nil {
			return statErr
		}
		dirMod = dirInfo.ModTime()
		snapshot, err = n.readMarkerSnapshot()
		if err != nil {
			return err
		}
	} else {
		var needsRefresh bool
		snapshot, dirMod, needsRefresh, err = n.markerSnapshotChanged()
		if err != nil {
			return err
		}
		if !needsRefresh {
			return nil
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.loadManifestLocked(); err != nil {
		return err
	}
	if err := n.scanSegmentsLocked(); err != nil {
		return err
	}
	for _, seg := range n.orderedSegmentsLocked() {
		if err := n.readSegmentLocked(seg); err != nil {
			return err
		}
	}
	n.markerMu.Lock()
	n.markerSnapshot = snapshot
	n.markerDirMod = dirMod
	n.markerSynced = true
	n.markerMu.Unlock()
	return nil
}

func (n *logNamespace) setSingleWriter() {
	n.markerMu.Lock()
	n.markerSnapshot = nil
	n.markerDirMod = time.Time{}
	n.markerSynced = false
	n.markerTouchAt = time.Time{}
	n.markerDirReady = false
	n.markerMu.Unlock()
}

func (n *logNamespace) markerSnapshotChanged() (map[string]markerInfo, time.Time, bool, error) {
	n.markerMu.Lock()
	prevSnapshot := n.markerSnapshot
	prevDirMod := n.markerDirMod
	synced := n.markerSynced
	lastScan := n.markerScanAt
	n.markerMu.Unlock()

	dirInfo, err := os.Stat(n.markerDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := os.MkdirAll(n.markerDir, 0o755); err != nil {
				return nil, time.Time{}, false, err
			}
			dirInfo, err = os.Stat(n.markerDir)
		}
		if err != nil {
			return nil, time.Time{}, false, err
		}
	}
	dirMod := dirInfo.ModTime()

	if !synced {
		snapshot, err := n.readMarkerSnapshot()
		return snapshot, dirMod, true, err
	}
	if len(prevSnapshot) == 0 {
		if !dirMod.After(prevDirMod) {
			return nil, dirMod, false, nil
		}
		snapshot, err := n.readMarkerSnapshot()
		if err == nil {
			n.markerMu.Lock()
			n.markerScanAt = n.store.now()
			n.markerMu.Unlock()
		}
		if err != nil {
			return nil, dirMod, false, err
		}
		if markerSnapshotEqual(snapshot, prevSnapshot) {
			return nil, dirMod, false, nil
		}
		return snapshot, dirMod, true, nil
	}
	if !dirMod.After(prevDirMod) {
		snapshot, err := n.statMarkerSnapshot(prevSnapshot)
		if err != nil {
			return nil, dirMod, false, err
		}
		if markerSnapshotEqual(snapshot, prevSnapshot) {
			return nil, dirMod, false, nil
		}
		return snapshot, dirMod, true, nil
	}
	useFullScan := dirMod.After(prevDirMod)
	if n.store.markerScanEvery > 0 {
		now := n.store.now()
		if lastScan.IsZero() || now.Sub(lastScan) >= n.store.markerScanEvery {
			useFullScan = true
		}
	}
	var snapshot map[string]markerInfo
	if useFullScan {
		snapshot, err = n.readMarkerSnapshot()
		if err == nil {
			n.markerMu.Lock()
			n.markerScanAt = n.store.now()
			n.markerMu.Unlock()
		}
	} else {
		snapshot, err = n.statMarkerSnapshot(prevSnapshot)
	}
	if err != nil {
		return nil, dirMod, false, err
	}
	if markerSnapshotEqual(snapshot, prevSnapshot) {
		return nil, dirMod, false, nil
	}
	return snapshot, dirMod, true, nil
}

func (n *logNamespace) readMarkerSnapshot() (map[string]markerInfo, error) {
	if n.markerDir == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(n.markerDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := os.MkdirAll(n.markerDir, 0o755); err != nil {
				return nil, err
			}
			entries = nil
		} else {
			return nil, err
		}
	}
	snapshot := make(map[string]markerInfo)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == "" || name == n.markerSelf {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		snapshot[name] = markerInfo{modTime: info.ModTime(), size: info.Size()}
	}
	return snapshot, nil
}

func (n *logNamespace) statMarkerSnapshot(prev map[string]markerInfo) (map[string]markerInfo, error) {
	snapshot := make(map[string]markerInfo, len(prev))
	for name := range prev {
		path := filepath.Join(n.markerDir, name)
		info, err := os.Stat(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		snapshot[name] = markerInfo{modTime: info.ModTime(), size: info.Size()}
	}
	return snapshot, nil
}

func markerSnapshotEqual(a, b map[string]markerInfo) bool {
	if len(a) != len(b) {
		return false
	}
	for key, av := range a {
		bv, ok := b[key]
		if !ok {
			return false
		}
		if av.size != bv.size || !av.modTime.Equal(bv.modTime) {
			return false
		}
	}
	return true
}

func (n *logNamespace) touchMarker() error {
	if n.markerPath == "" {
		return nil
	}
	n.markerMu.Lock()
	defer n.markerMu.Unlock()
	now := n.store.now()
	if n.store.markerTouchEvery > 0 && !n.markerTouchAt.IsZero() {
		if now.Sub(n.markerTouchAt) < n.store.markerTouchEvery {
			return nil
		}
	}
	if !n.markerDirReady {
		if err := os.MkdirAll(n.markerDir, 0o755); err != nil {
			return err
		}
		n.markerDirReady = true
	}
	payload := []byte("0")
	if n.markerToggle {
		payload = []byte("00")
	}
	if err := os.WriteFile(n.markerPath, payload, 0o644); err != nil {
		return err
	}
	n.markerToggle = !n.markerToggle
	n.markerTouchAt = now
	return nil
}

func (n *logNamespace) loadManifestLocked() error {
	if n.manifest == nil {
		n.manifest = &manifestState{
			path: filepath.Join(n.manifestDir, "manifest.log"),
		}
	}
	segments, err := n.manifest.readNewSegments()
	if err != nil {
		return err
	}
	for _, name := range segments {
		if _, ok := n.segments[name]; ok {
			continue
		}
		n.segments[name] = &logSegment{
			name: name,
			path: filepath.Join(n.segmentsDir, name),
		}
	}
	return nil
}

func (n *logNamespace) scanSegmentsLocked() error {
	entries, err := os.ReadDir(n.segmentsDir)
	if err != nil {
		return fmt.Errorf("disk: logstore scan segments: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "seg-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		if _, ok := n.segments[name]; ok {
			continue
		}
		n.segments[name] = &logSegment{
			name: name,
			path: filepath.Join(n.segmentsDir, name),
		}
	}
	return nil
}

func (n *logNamespace) orderedSegmentsLocked() []*logSegment {
	segments := make([]*logSegment, 0, len(n.segments))
	for _, seg := range n.segments {
		segments = append(segments, seg)
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].name < segments[j].name })
	return segments
}

func (n *logNamespace) readSegmentLocked(seg *logSegment) error {
	info, err := os.Stat(seg.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("disk: logstore stat segment: %w", err)
	}
	size := info.Size()
	readLimit := size
	if seg == n.active && (seg.pending > 0 || seg.inFlight > 0) && seg.readOffset < readLimit {
		readLimit = seg.readOffset
	}
	if readLimit <= seg.readOffset {
		if size > seg.size {
			seg.size = size
		}
		return nil
	}
	file, err := os.Open(seg.path)
	if err != nil {
		return fmt.Errorf("disk: logstore open segment: %w", err)
	}
	defer file.Close()
	if _, err := file.Seek(seg.readOffset, io.SeekStart); err != nil {
		return fmt.Errorf("disk: logstore seek segment: %w", err)
	}
	reader := bufio.NewReader(file)
	offset := seg.readOffset
	for {
		headerBuf := make([]byte, logHeaderSize)
		if _, err := io.ReadFull(reader, headerBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return fmt.Errorf("disk: logstore read header: %w", err)
		}
		hdr, err := decodeHeader(headerBuf)
		if err != nil {
			return nil
		}
		recordStart := offset
		offset += int64(logHeaderSize)
		keyBytes := make([]byte, hdr.keyLen)
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			break
		}
		offset += int64(hdr.keyLen)
		metaBytes := make([]byte, hdr.metaLen)
		if _, err := io.ReadFull(reader, metaBytes); err != nil {
			break
		}
		offset += int64(hdr.metaLen)
		payloadStart := offset
		payloadLen := int64(hdr.payloadLen)
		var crc uint32
		var link *recordLink
		if payloadLen > 0 {
			crcReader := crc32.NewIEEE()
			if hdr.recType == logRecordStateLink {
				linkPayload := make([]byte, payloadLen)
				if _, err := io.ReadFull(reader, linkPayload); err != nil {
					break
				}
				if _, err := crcReader.Write(linkPayload); err != nil {
					break
				}
				decoded, derr := decodeStateLinkPayload(linkPayload)
				if derr != nil {
					break
				}
				link = &recordLink{
					path:   filepath.Join(n.segmentsDir, decoded.segment),
					offset: decoded.offset,
					length: decoded.length,
				}
				crc = crcReader.Sum32()
			} else {
				if _, err := io.CopyN(io.MultiWriter(io.Discard, crcReader), reader, payloadLen); err != nil {
					break
				}
				crc = crcReader.Sum32()
			}
		}
		offset += payloadLen
		if hdr.payloadLen > 0 && crc != hdr.payloadCRC {
			break
		}
		meta, err := decodeMeta(hdr.recType, metaBytes)
		if err != nil {
			break
		}
		ref := &recordRef{
			recType:       hdr.recType,
			key:           string(keyBytes),
			meta:          meta,
			segment:       seg,
			offset:        recordStart,
			payloadOffset: payloadStart,
			payloadLen:    payloadLen,
			link:          link,
		}
		n.applyRecordLocked(ref)
		seg.readOffset = offset
	}
	if size > seg.size {
		seg.size = size
	}
	return nil
}

func (n *logNamespace) applyRecordLocked(ref *recordRef) {
	switch ref.recType {
	case logRecordMetaPut:
		current := n.metaIndex[ref.key]
		if current == nil || ref.meta.gen >= current.meta.gen {
			n.metaIndex[ref.key] = ref
		}
	case logRecordMetaDelete:
		if current := n.metaIndex[ref.key]; current == nil || ref.meta.gen >= current.meta.gen {
			delete(n.metaIndex, ref.key)
		}
	case logRecordStatePut:
		current := n.stateIndex[ref.key]
		if current == nil || ref.meta.gen >= current.meta.gen {
			n.stateIndex[ref.key] = ref
		}
	case logRecordStateLink:
		current := n.stateIndex[ref.key]
		if current == nil || ref.meta.gen >= current.meta.gen {
			n.stateIndex[ref.key] = ref
		}
	case logRecordStateDelete:
		if current := n.stateIndex[ref.key]; current == nil || ref.meta.gen >= current.meta.gen {
			delete(n.stateIndex, ref.key)
		}
	case logRecordObjectPut:
		current := n.objectIndex[ref.key]
		if current == nil || ref.meta.gen >= current.meta.gen {
			if current == nil {
				n.insertObjectKeyLocked(ref.key)
			}
			n.objectIndex[ref.key] = ref
		}
	case logRecordObjectDelete:
		if current := n.objectIndex[ref.key]; current == nil || ref.meta.gen >= current.meta.gen {
			if current != nil {
				n.removeObjectKeyLocked(ref.key)
			}
			delete(n.objectIndex, ref.key)
		}
	}
}

func (n *logNamespace) insertObjectKeyLocked(key string) {
	idx := sort.SearchStrings(n.sortedObjects, key)
	if idx < len(n.sortedObjects) && n.sortedObjects[idx] == key {
		return
	}
	n.sortedObjects = append(n.sortedObjects, "")
	copy(n.sortedObjects[idx+1:], n.sortedObjects[idx:])
	n.sortedObjects[idx] = key
}

func (n *logNamespace) removeObjectKeyLocked(key string) {
	idx := sort.SearchStrings(n.sortedObjects, key)
	if idx < len(n.sortedObjects) && n.sortedObjects[idx] == key {
		n.sortedObjects = append(n.sortedObjects[:idx], n.sortedObjects[idx+1:]...)
	}
}

func (n *logNamespace) ensureActiveLocked() error {
	if n.active != nil {
		return nil
	}
	if err := n.ensureDirs(); err != nil {
		return err
	}
	n.nextSeq = n.nextSegmentSeqLocked()
	return n.openNewSegmentLocked()
}

func (n *logNamespace) nextSegmentSeqLocked() int {
	seq := 1
	prefix := fmt.Sprintf("seg-%s-", n.store.writerID)
	for name := range n.segments {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		base := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".log")
		var val int
		if _, err := fmt.Sscanf(base, "%d", &val); err == nil && val >= seq {
			seq = val + 1
		}
	}
	if seq < 1 {
		seq = 1
	}
	return seq
}

func (n *logNamespace) openNewSegmentLocked() error {
	seq := n.nextSeq
	n.nextSeq++
	name := fmt.Sprintf("seg-%s-%08d.log", n.store.writerID, seq)
	path := filepath.Join(n.segmentsDir, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("disk: logstore open segment: %w", err)
	}
	n.active = &logSegment{name: name, path: path, file: file}
	n.segments[name] = n.active
	if n.manifest != nil {
		_ = n.manifest.appendSegment("open", name)
	}
	return nil
}

func (n *logNamespace) closeActiveLocked() error {
	if n.active == nil {
		return nil
	}
	if n.active.file != nil {
		_ = n.active.file.Close()
	}
	n.active.sealed = true
	if n.manifest != nil {
		_ = n.manifest.appendSegment("seal", n.active.name)
	}
	n.active = nil
	return nil
}

func (n *logNamespace) appendRecord(ctx context.Context, recType logRecordType, key string, meta recordMeta, payload io.Reader) (*recordRef, error) {
	var commitGroup *storage.CommitGroup
	waitForCommit := false
	if group, ok := storage.CommitGroupFromContext(ctx); ok && group != nil {
		commitGroup = group
	} else {
		commitGroup = storage.NewCommitGroup()
		waitForCommit = true
	}
	noSync := storage.NoSyncFromContext(ctx)

	n.mu.Lock()
	locked := true
	defer func() {
		if locked {
			n.mu.Unlock()
		}
	}()
	if err := n.ensureActiveLocked(); err != nil {
		return nil, err
	}
	active := n.active
	if active == nil || active.file == nil {
		return nil, fmt.Errorf("disk: logstore missing segment file")
	}
	active.inFlight++
	n.mu.Unlock()
	locked = false
	defer func() {
		n.mu.Lock()
		active.inFlight--
		if active == n.active {
			n.maybeCloseActiveLocked()
		}
		n.mu.Unlock()
	}()

	now := n.store.now().Unix()
	if meta.modifiedAt == 0 {
		meta.modifiedAt = now
	}

	var stateMaterial kryptograf.Material
	stateMaterialSet := false
	if recType == logRecordMetaPut && meta.etag == "" {
		meta.etag = uuidv7.NewString()
	}

	if recType == logRecordStatePut {
		encrypted := n.store.crypto != nil && n.store.crypto.Enabled()
		if encrypted {
			defaultCtx := storage.StateObjectContext(pathJoin(n.namespace, key))
			objectCtx := storage.StateObjectContextFromContext(ctx, defaultCtx)
			descFromCtx, ok := storage.StateDescriptorFromContext(ctx)
			var mat kryptograf.Material
			var err error
			if ok && len(descFromCtx) > 0 {
				meta.descriptor = append([]byte(nil), descFromCtx...)
				mat, err = n.store.crypto.MaterialFromDescriptor(objectCtx, meta.descriptor)
			} else {
				var minted storage.MaterialResult
				minted, err = n.store.crypto.MintMaterial(objectCtx)
				if err == nil {
					meta.descriptor = append([]byte(nil), minted.Descriptor...)
					mat = minted.Material
				}
			}
			if err != nil {
				return nil, err
			}
			stateMaterial = mat
			stateMaterialSet = true
		}
	}

	var inlineBuf *bytes.Buffer
	var payloadBuf []byte
	overflow := false
	if payload != nil {
		inlineBuf = n.store.inlineBufPool.Get().(*bytes.Buffer)
		inlineBuf.Reset()
	}
	var err error
	payloadBuf, overflow, err = readPayloadInline(payload, maxInlinePayload, inlineBuf)
	if err != nil {
		if inlineBuf != nil {
			inlineBuf.Reset()
			n.store.inlineBufPool.Put(inlineBuf)
		}
		return nil, err
	}
	if inlineBuf != nil {
		defer func() {
			inlineBuf.Reset()
			n.store.inlineBufPool.Put(inlineBuf)
		}()
	}
	if payloadBuf != nil && overflow {
		payload = io.MultiReader(bytes.NewReader(append([]byte(nil), payloadBuf...)), payload)
		payloadBuf = nil
	}

	etagBytes, err := etagBytesForRecord(recType, meta.etag)
	if err != nil {
		return nil, err
	}
	metaSize, _, err := metaSize(recType, meta)
	if err != nil {
		return nil, err
	}
	metaBuf := n.store.acquireMetaBuf(metaSize)
	metaBytes, _, _, err := encodeMetaInto(metaBuf, recType, meta, etagBytes)
	if err != nil {
		n.store.releaseMetaBuf(metaBuf)
		return nil, err
	}
	defer n.store.releaseMetaBuf(metaBytes)
	keyLen := len(key)
	header := recordHeader{
		recType: recType,
		keyLen:  uint32(keyLen),
		metaLen: uint32(len(metaBytes)),
	}

	var (
		inlineRecord          []byte
		inlinePayloadLen      int64
		inlinePayloadStartRel int64
	)
	if payloadBuf != nil || payload == nil {
		isLink := recType == logRecordStateLink
		var plainLen int64
		var payloadBytes []byte
		var hasher hash.Hash
		var sumBuf [sha256.Size]byte
		if !isLink {
			hasher = sha256.New()
		}
		switch recType {
		case logRecordStatePut:
			plainLen = int64(len(payloadBuf))
			if _, err := hasher.Write(payloadBuf); err != nil {
				return nil, err
			}
			if n.store.crypto != nil && n.store.crypto.Enabled() {
				if !stateMaterialSet {
					return nil, fmt.Errorf("disk: logstore missing state material")
				}
				var encBuf bytes.Buffer
				encWriter, err := n.store.crypto.EncryptWriterForMaterial(&encBuf, stateMaterial)
				if err != nil {
					return nil, err
				}
				if _, err := encWriter.Write(payloadBuf); err != nil {
					_ = encWriter.Close()
					return nil, err
				}
				if err := encWriter.Close(); err != nil {
					return nil, err
				}
				payloadBytes = encBuf.Bytes()
			} else {
				payloadBytes = payloadBuf
			}
		case logRecordObjectPut:
			plainLen = int64(len(payloadBuf))
			if _, err := hasher.Write(payloadBuf); err != nil {
				return nil, err
			}
			payloadBytes = payloadBuf
			meta.plaintextSize = int64(len(payloadBytes))
			meta.cipherSize = int64(len(payloadBytes))
		case logRecordStateLink:
			payloadBytes = payloadBuf
		default:
			plainLen = int64(len(payloadBuf))
			payloadBytes = payloadBuf
		}
		inlinePayloadLen = int64(len(payloadBytes))
		if !isLink {
			meta.plaintextSize = plainLen
			meta.cipherSize = inlinePayloadLen
			if recType == logRecordStatePut || recType == logRecordObjectPut {
				sum := hasher.Sum(sumBuf[:0])
				meta.etag = hex.EncodeToString(sum)
			}
		}
		etagBytes, err = etagBytesForRecord(recType, meta.etag)
		if err != nil {
			return nil, err
		}
		metaBytes, _, _, err = encodeMetaInto(metaBytes, recType, meta, etagBytes)
		if err != nil {
			return nil, err
		}
		inlinePayloadStartRel = int64(logHeaderSize + keyLen + len(metaBytes))
		header.payloadLen = uint32(inlinePayloadLen)
		header.payloadCRC = crc32.ChecksumIEEE(payloadBytes)
		var headerBuf [logHeaderSize]byte
		encodeHeader(headerBuf[:], header)
		recordLen := logHeaderSize + keyLen + len(metaBytes) + int(inlinePayloadLen)
		inlineRecord = n.store.acquireInlineRecordBuf(recordLen)
		if len(inlineRecord) != recordLen {
			inlineRecord = inlineRecord[:recordLen]
		}
		copy(inlineRecord, headerBuf[:])
		copy(inlineRecord[logHeaderSize:], key)
		copy(inlineRecord[logHeaderSize+keyLen:], metaBytes)
		if inlinePayloadLen > 0 {
			copy(inlineRecord[logHeaderSize+keyLen+len(metaBytes):], payloadBytes)
		}
	}

	if inlineRecord != nil || payload == nil {
		return n.appendInline(ctx, recType, key, meta, inlineRecord, inlinePayloadLen, inlinePayloadStartRel)
	}

	n.writeMu.Lock()
	writeLocked := true
	defer func() {
		if writeLocked {
			n.writeMu.Unlock()
		}
	}()

	n.mu.Lock()
	if active != n.active {
		active = n.active
		if active == nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("disk: logstore missing active segment")
		}
	}
	if active.file == nil {
		n.mu.Unlock()
		return nil, fmt.Errorf("disk: logstore missing segment file")
	}
	offset := active.size
	n.mu.Unlock()

	payloadStart := int64(0)
	payloadLen := int64(0)
	{
		var headerBuf [logHeaderSize]byte
		encodeHeader(headerBuf[:], header)
		prefixLen := logHeaderSize + keyLen + len(metaBytes)
		prefixBuf := n.prefixBuf
		if cap(prefixBuf) < prefixLen {
			prefixBuf = make([]byte, prefixLen)
		} else {
			prefixBuf = prefixBuf[:prefixLen]
		}
		copy(prefixBuf, headerBuf[:])
		copy(prefixBuf[logHeaderSize:], key)
		copy(prefixBuf[logHeaderSize+keyLen:], metaBytes)
		if _, err := active.file.WriteAt(prefixBuf, offset); err != nil {
			return nil, err
		}
		n.prefixBuf = prefixBuf[:0]
		payloadStart = offset + int64(prefixLen)

		crcWriter := crc32.NewIEEE()
		isLink := recType == logRecordStateLink
		var hasher hash.Hash
		var sumBuf [sha256.Size]byte
		if !isLink {
			hasher = sha256.New()
		}
		ow := &offsetWriter{file: active.file, off: payloadStart}
		var bufWriter *bufio.Writer
		payloadWriter := io.Writer(ow)
		if payload != nil {
			bufWriter = bufio.NewWriterSize(ow, payloadWriteBufferSize)
			payloadWriter = bufWriter
		}
		payloadSink := &crc32Writer{w: payloadWriter, crc: crcWriter}
		var plainLen int64
		var writeErr error

		if payload != nil {
			switch recType {
			case logRecordStatePut:
				encrypted := n.store.crypto != nil && n.store.crypto.Enabled()
				if encrypted {
					if !stateMaterialSet {
						return nil, fmt.Errorf("disk: logstore missing state material")
					}
					encWriter, err := n.store.crypto.EncryptWriterForMaterial(payloadSink, stateMaterial)
					if err != nil {
						return nil, err
					}
					plainLen, writeErr = io.Copy(io.MultiWriter(encWriter, hasher), payload)
					if err := encWriter.Close(); err != nil && writeErr == nil {
						writeErr = err
					}
				} else {
					plainLen, writeErr = io.Copy(io.MultiWriter(payloadSink, hasher), payload)
				}
			case logRecordStateLink:
				plainLen, writeErr = io.Copy(payloadSink, payload)
			default:
				plainLen, writeErr = io.Copy(io.MultiWriter(payloadSink, hasher), payload)
			}
		}
		if bufWriter != nil {
			if err := bufWriter.Flush(); err != nil && writeErr == nil {
				writeErr = err
			}
		}
		if writeErr != nil {
			return nil, writeErr
		}
		payloadLen = ow.off - payloadStart
		active.size = payloadStart + payloadLen

		if !isLink {
			meta.plaintextSize = plainLen
			meta.cipherSize = payloadLen
			if recType == logRecordStatePut || recType == logRecordObjectPut {
				sum := hasher.Sum(sumBuf[:0])
				meta.etag = hex.EncodeToString(sum)
			}
			if recType == logRecordObjectPut {
				meta.plaintextSize = payloadLen
				meta.cipherSize = payloadLen
			}
		}

		etagBytes, err = etagBytesForRecord(recType, meta.etag)
		if err != nil {
			return nil, err
		}
		metaBytes, _, _, err = encodeMetaInto(metaBytes, recType, meta, etagBytes)
		if err != nil {
			return nil, err
		}
		finalPrefixLen := logHeaderSize + keyLen + len(metaBytes)
		if finalPrefixLen != prefixLen {
			return nil, fmt.Errorf("disk: logstore meta size changed")
		}
		header.payloadLen = uint32(payloadLen)
		header.payloadCRC = crcWriter.Sum32()
		encodeHeader(headerBuf[:], header)
		finalPrefix := n.prefixBuf
		if cap(finalPrefix) < finalPrefixLen {
			finalPrefix = make([]byte, finalPrefixLen)
		} else {
			finalPrefix = finalPrefix[:finalPrefixLen]
		}
		copy(finalPrefix, headerBuf[:])
		copy(finalPrefix[logHeaderSize:], key)
		copy(finalPrefix[logHeaderSize+keyLen:], metaBytes)
		if _, err := active.file.WriteAt(finalPrefix, offset); err != nil {
			return nil, err
		}
		n.prefixBuf = finalPrefix[:0]
	}

	n.mu.Lock()
	if active != n.active {
		n.mu.Unlock()
		return nil, fmt.Errorf("disk: logstore active segment changed")
	}
	active.size = payloadStart + payloadLen

	ref := &recordRef{
		recType:       recType,
		key:           key,
		meta:          meta,
		segment:       active,
		offset:        offset,
		payloadOffset: payloadStart,
		payloadLen:    payloadLen,
	}
	n.queuePendingLocked(ref, commitGroup)
	added := commitGroup.AddCommitter(active.file, func() <-chan error {
		if noSync {
			return n.commitPendingNoSync(commitGroup, active)
		}
		return n.commitPending(commitGroup, active)
	})
	if added {
		active.pending++
	}
	n.mu.Unlock()
	n.writeMu.Unlock()
	writeLocked = false
	if waitForCommit {
		if err := commitGroup.Wait(); err != nil {
			return nil, err
		}
	}
	return ref, nil
}

func (n *logNamespace) appendInline(ctx context.Context, recType logRecordType, key string, meta recordMeta, record []byte, payloadLen int64, payloadStartRel int64) (*recordRef, error) {
	var commitGroup *storage.CommitGroup
	waitForCommit := false
	if group, ok := storage.CommitGroupFromContext(ctx); ok && group != nil {
		commitGroup = group
	} else {
		commitGroup = storage.NewCommitGroup()
		waitForCommit = true
	}
	noSync := storage.NoSyncFromContext(ctx)
	req := &appendRequest{
		recType:         recType,
		key:             key,
		meta:            meta,
		record:          record,
		recordBuf:       record,
		payloadLen:      payloadLen,
		payloadStartRel: payloadStartRel,
		written:         make(chan *recordRef, 1),
		done:            make(chan error, 1),
		ready:           make(chan struct{}),
		group:           commitGroup,
		noSync:          noSync,
	}
	commitGroup.Add(req.done)
	n.appendCh <- req
	ref := <-req.written
	if ref == nil {
		if req.ready != nil {
			close(req.ready)
			req.ready = nil
		}
		err := <-req.done
		if err == nil {
			err = fmt.Errorf("disk: logstore append failed")
		}
		return nil, err
	}
	if req.ready != nil {
		<-req.ready
		req.ready = nil
	}
	if waitForCommit {
		if err := commitGroup.Wait(); err != nil {
			return nil, err
		}
	}
	return ref, nil
}

func (n *logNamespace) appendLoop() {
	var pending []*appendRequest
	for {
		if len(pending) == 0 {
			req, ok := <-n.appendCh
			if !ok {
				return
			}
			pending = append(pending, req)
		}
		maxOps := n.appendBatchMaxOps()
		for maxOps == 0 || len(pending) < maxOps {
			select {
			case req, ok := <-n.appendCh:
				if !ok {
					n.flushAppends(pending)
					return
				}
				pending = append(pending, req)
			default:
				goto drainDone
			}
		}
	drainDone:
		if maxOps > 0 && len(pending) >= maxOps {
			n.flushAppends(pending[:maxOps])
			pending = pending[maxOps:]
			continue
		}
		n.flushAppends(pending)
		pending = pending[:0]
	}
}

func (n *logNamespace) appendBatchMaxOps() int {
	if n.store.commitMaxOps <= 0 {
		return 0
	}
	return n.store.commitMaxOps
}

func (n *logNamespace) allocRef() *recordRef {
	if n.refSlabIdx >= len(n.refSlab) {
		slab := make([]recordRef, recordRefSlabSize)
		n.refSlabs = append(n.refSlabs, slab)
		n.refSlab = slab
		n.refSlabIdx = 0
	}
	ref := &n.refSlab[n.refSlabIdx]
	n.refSlabIdx++
	*ref = recordRef{}
	return ref
}

func (n *logNamespace) flushAppends(batch []*appendRequest) {
	if len(batch) == 0 {
		return
	}
	n.writeMu.Lock()
	defer n.writeMu.Unlock()
	n.mu.Lock()
	if err := n.ensureActiveLocked(); err != nil {
		n.mu.Unlock()
		n.failBatch(batch, err)
		return
	}
	active := n.active
	if active == nil || active.file == nil {
		n.mu.Unlock()
		n.failBatch(batch, fmt.Errorf("disk: logstore missing segment file"))
		return
	}
	offset := active.size
	active.inFlight++
	n.mu.Unlock()
	inFlight := true
	defer func() {
		if !inFlight {
			return
		}
		n.mu.Lock()
		if active == n.active && active.inFlight > 0 {
			active.inFlight--
			n.maybeCloseActiveLocked()
		}
		n.mu.Unlock()
	}()

	if len(batch) == 1 {
		req := batch[0]
		if req == nil || len(req.record) == 0 {
			n.failBatch(batch, fmt.Errorf("disk: logstore append missing record"))
			return
		}
		if _, err := active.file.WriteAt(req.record, offset); err != nil {
			n.failBatch(batch, err)
			return
		}
		newSize := offset + int64(len(req.record))
		if req.recordBuf != nil {
			n.store.releaseInlineRecordBuf(req.recordBuf)
			req.recordBuf = nil
		}
		req.record = nil

		n.mu.Lock()
		if active != n.active {
			n.mu.Unlock()
			n.failBatch(batch, fmt.Errorf("disk: logstore active segment changed"))
			return
		}
		active.size = newSize
		if active.inFlight > 0 {
			active.inFlight--
			inFlight = false
		}
		ref := n.allocRef()
		ref.recType = req.recType
		ref.key = req.key
		ref.meta = req.meta
		ref.segment = active
		ref.offset = offset
		ref.payloadOffset = offset + req.payloadStartRel
		ref.payloadLen = req.payloadLen
		n.queuePendingLocked(ref, req.group)
		if req.group != nil {
			group := req.group
			noSync := req.noSync
			added := group.AddCommitter(active.file, func() <-chan error {
				if noSync {
					return n.commitPendingNoSync(group, active)
				}
				return n.commitPending(group, active)
			})
			if added {
				active.pending++
			}
		}
		if req.ready != nil {
			close(req.ready)
			req.ready = nil
		}
		n.mu.Unlock()
		req.written <- ref
		req.done <- nil
		return
	}

	refs := n.appendRefs
	if cap(refs) < len(batch) {
		refs = make([]*recordRef, len(batch))
	} else {
		refs = refs[:len(batch)]
		for i := range refs {
			refs[i] = nil
		}
	}
	totalLen := 0
	for _, req := range batch {
		if req == nil {
			continue
		}
		if len(req.record) == 0 {
			n.failBatch(batch, fmt.Errorf("disk: logstore append missing record"))
			return
		}
		totalLen += len(req.record)
	}
	buf := n.appendBuf
	if cap(buf) < totalLen {
		buf = make([]byte, totalLen)
	} else {
		buf = buf[:totalLen]
	}
	cursor := 0
	for i, req := range batch {
		if req == nil {
			continue
		}
		recordLen := len(req.record)
		copy(buf[cursor:], req.record)
		if req.recordBuf != nil {
			n.store.releaseInlineRecordBuf(req.recordBuf)
			req.recordBuf = nil
		}
		req.record = nil
		ref := n.allocRef()
		ref.recType = req.recType
		ref.key = req.key
		ref.meta = req.meta
		ref.segment = active
		ref.offset = offset + int64(cursor)
		ref.payloadOffset = offset + int64(cursor) + req.payloadStartRel
		ref.payloadLen = req.payloadLen
		refs[i] = ref
		cursor += recordLen
	}
	if len(buf) > 0 {
		if _, err := active.file.WriteAt(buf, offset); err != nil {
			n.failBatch(batch, err)
			return
		}
	}
	if cap(buf) > maxAppendBufCap {
		n.appendBuf = nil
	} else {
		n.appendBuf = buf[:0]
	}
	newSize := offset + int64(len(buf))

	n.mu.Lock()
	defer n.mu.Unlock()
	if active != n.active {
		n.failBatch(batch, fmt.Errorf("disk: logstore active segment changed"))
		return
	}
	active.size = newSize
	if active.inFlight > 0 {
		active.inFlight--
		inFlight = false
	}

	groupSet := n.appendGroupSet
	if groupSet == nil {
		groupSet = make(map[*storage.CommitGroup]struct{})
	} else {
		for k := range groupSet {
			delete(groupSet, k)
		}
	}
	groupNeedsSync := n.appendGroupNeedsSync
	if groupNeedsSync == nil {
		groupNeedsSync = make(map[*storage.CommitGroup]struct{})
	} else {
		for k := range groupNeedsSync {
			delete(groupNeedsSync, k)
		}
	}
	for _, req := range batch {
		if req == nil || req.group == nil {
			continue
		}
		groupSet[req.group] = struct{}{}
		if !req.noSync {
			groupNeedsSync[req.group] = struct{}{}
		}
	}
	for i, req := range batch {
		if req == nil {
			continue
		}
		ref := refs[i]
		if ref == nil {
			continue
		}
		n.queuePendingLocked(ref, req.group)
	}

	if len(groupSet) > 0 {
		for group := range groupSet {
			group := group
			_, needsSync := groupNeedsSync[group]
			noSync := !needsSync
			added := group.AddCommitter(active.file, func() <-chan error {
				if noSync {
					return n.commitPendingNoSync(group, active)
				}
				return n.commitPending(group, active)
			})
			if added {
				active.pending++
			}
		}
	}
	for _, req := range batch {
		if req == nil || req.ready == nil {
			continue
		}
		close(req.ready)
		req.ready = nil
	}

	for i, req := range batch {
		if req == nil {
			continue
		}
		req.written <- refs[i]
	}

	for _, req := range batch {
		if req == nil {
			continue
		}
		req.done <- nil
	}
	for i := range refs {
		refs[i] = nil
	}
	n.appendRefs = refs[:0]
	n.appendGroupSet = groupSet
	n.appendGroupNeedsSync = groupNeedsSync
}

func (n *logNamespace) failBatch(batch []*appendRequest, err error) {
	if err == nil {
		err = fmt.Errorf("disk: logstore append failed")
	}
	for _, req := range batch {
		if req == nil {
			continue
		}
		if req.recordBuf != nil {
			n.store.releaseInlineRecordBuf(req.recordBuf)
			req.recordBuf = nil
		}
		req.record = nil
		req.written <- nil
		req.done <- err
	}
}

func recordEndOffset(ref *recordRef) int64 {
	if ref == nil {
		return 0
	}
	return ref.payloadOffset + ref.payloadLen
}

func (n *logNamespace) queuePendingLocked(ref *recordRef, group *storage.CommitGroup) {
	if ref == nil {
		return
	}
	p := &pendingRef{ref: ref, group: group, done: make(chan struct{})}
	n.pending = append(n.pending, p)
	n.trackPendingLocked(p)
	if group == nil {
		return
	}
	if n.pendingCounts == nil {
		n.pendingCounts = make(map[*storage.CommitGroup]int)
	}
	n.pendingCounts[group]++
}

func (n *logNamespace) trackPendingLocked(p *pendingRef) {
	if p == nil || p.ref == nil {
		return
	}
	switch p.ref.recType {
	case logRecordMetaPut, logRecordMetaDelete:
		if n.pendingMeta == nil {
			n.pendingMeta = make(map[string]*pendingRef)
		}
		n.pendingMeta[p.ref.key] = p
	case logRecordStatePut, logRecordStateDelete, logRecordStateLink:
		if n.pendingState == nil {
			n.pendingState = make(map[string]*pendingRef)
		}
		n.pendingState[p.ref.key] = p
	case logRecordObjectPut, logRecordObjectDelete:
		if n.pendingObject == nil {
			n.pendingObject = make(map[string]*pendingRef)
		}
		n.pendingObject[p.ref.key] = p
	}
}

func (n *logNamespace) clearPendingLocked(p *pendingRef) {
	if p == nil || p.ref == nil {
		return
	}
	switch p.ref.recType {
	case logRecordMetaPut, logRecordMetaDelete:
		if n.pendingMeta != nil && n.pendingMeta[p.ref.key] == p {
			delete(n.pendingMeta, p.ref.key)
		}
	case logRecordStatePut, logRecordStateDelete, logRecordStateLink:
		if n.pendingState != nil && n.pendingState[p.ref.key] == p {
			delete(n.pendingState, p.ref.key)
		}
	case logRecordObjectPut, logRecordObjectDelete:
		if n.pendingObject != nil && n.pendingObject[p.ref.key] == p {
			delete(n.pendingObject, p.ref.key)
		}
	}
}

func (n *logNamespace) markCommittedLocked(group *storage.CommitGroup) {
	if group == nil {
		return
	}
	if n.committed == nil {
		n.committed = make(map[*storage.CommitGroup]struct{})
	}
	n.committed[group] = struct{}{}
}

func (n *logNamespace) isCommittedLocked(group *storage.CommitGroup) bool {
	if group == nil {
		return true
	}
	if n.committed == nil {
		return false
	}
	_, ok := n.committed[group]
	return ok
}

func (n *logNamespace) applyPendingLocked() {
	if len(n.pending) == 0 {
		return
	}
	appliedCount := 0
	for _, p := range n.pending {
		if p == nil || p.ref == nil || p.applied {
			continue
		}
		if !n.isCommittedLocked(p.group) {
			continue
		}
		n.applyRecordLocked(p.ref)
		p.applied = true
		n.clearPendingLocked(p)
		if p.done != nil {
			p.err = nil
			close(p.done)
			p.done = nil
		}
		appliedCount++
		if p.group != nil && n.pendingCounts != nil {
			if remaining := n.pendingCounts[p.group] - 1; remaining > 0 {
				n.pendingCounts[p.group] = remaining
			} else {
				delete(n.pendingCounts, p.group)
				if n.committed != nil {
					delete(n.committed, p.group)
				}
			}
		}
	}
	if n.pendingCounts != nil && appliedCount == 0 && len(n.pendingCounts) == 0 && n.committed != nil {
		for group := range n.committed {
			delete(n.committed, group)
		}
	}

	drain := 0
	for drain < len(n.pending) {
		p := n.pending[drain]
		if p == nil || p.ref == nil {
			drain++
			continue
		}
		if !p.applied {
			break
		}
		if seg := p.ref.segment; seg != nil {
			if end := recordEndOffset(p.ref); end > seg.readOffset {
				seg.readOffset = end
			}
		}
		drain++
	}
	if drain > 0 {
		copy(n.pending, n.pending[drain:])
		n.pending = n.pending[:len(n.pending)-drain]
	}
}

func (n *logNamespace) failPendingLocked(group *storage.CommitGroup, err error) {
	if group == nil || len(n.pending) == 0 {
		return
	}
	for _, p := range n.pending {
		if p == nil {
			continue
		}
		if p.group != group {
			continue
		}
		if p.done != nil {
			p.err = err
			close(p.done)
			p.done = nil
		}
		p.applied = true
		n.clearPendingLocked(p)
	}
	if n.pendingCounts != nil {
		delete(n.pendingCounts, group)
	}
	if n.committed != nil {
		delete(n.committed, group)
	}

	drain := 0
	for drain < len(n.pending) {
		p := n.pending[drain]
		if p == nil || p.ref == nil {
			drain++
			continue
		}
		if !p.applied {
			break
		}
		if seg := p.ref.segment; seg != nil {
			if end := recordEndOffset(p.ref); end > seg.readOffset {
				seg.readOffset = end
			}
		}
		drain++
	}
	if drain > 0 {
		copy(n.pending, n.pending[drain:])
		n.pending = n.pending[:len(n.pending)-drain]
	}
}

func (n *logNamespace) maybeCloseActiveLocked() {
	if n.active == nil {
		return
	}
	if n.active.inFlight > 0 {
		return
	}
	if n.active.pending > 0 {
		return
	}
	if n.syncedEpoch < n.noSyncEpoch {
		return
	}
	if n.store.closeAfterCommit && n.batcher.idle() {
		_ = n.closeActiveLocked()
		return
	}
	if n.store.segmentSize > 0 && n.active.size >= n.store.segmentSize && n.batcher.idle() {
		_ = n.closeActiveLocked()
	}
}

func (n *logNamespace) commitPending(group *storage.CommitGroup, seg *logSegment) <-chan error {
	ch := make(chan error, 1)
	if seg == nil || seg.file == nil {
		ch <- fmt.Errorf("disk: logstore missing segment file")
		return ch
	}
	n.mu.Lock()
	syncEpoch := n.noSyncEpoch
	n.mu.Unlock()
	wait := n.batcher.enqueue(seg.file)
	go func() {
		err := <-wait
		n.mu.Lock()
		if seg != nil && seg.pending > 0 {
			seg.pending--
		}
		if err == nil {
			if syncEpoch > n.syncedEpoch {
				n.syncedEpoch = syncEpoch
			}
			n.markCommittedLocked(group)
			n.applyPendingLocked()
			if markerErr := n.touchMarker(); markerErr != nil && err == nil {
				err = markerErr
			}
			n.maybeCloseActiveLocked()
		} else {
			n.failPendingLocked(group, err)
		}
		n.mu.Unlock()
		ch <- err
	}()
	return ch
}

func (n *logNamespace) commitPendingNoSync(group *storage.CommitGroup, seg *logSegment) <-chan error {
	ch := make(chan error, 1)
	n.mu.Lock()
	n.noSyncEpoch++
	if seg != nil && seg.pending > 0 {
		seg.pending--
	}
	n.markCommittedLocked(group)
	n.applyPendingLocked()
	if err := n.touchMarker(); err != nil {
		n.mu.Unlock()
		ch <- err
		return ch
	}
	n.maybeCloseActiveLocked()
	n.mu.Unlock()
	ch <- nil
	return ch
}

func (n *logNamespace) readPayload(ref *recordRef) ([]byte, error) {
	if ref == nil {
		return nil, nil
	}
	path := ref.segment.path
	file, release, err := n.store.acquireReadFile(path)
	if err != nil {
		return nil, err
	}
	resolvedPath, resolvedOffset, resolvedLen, err := n.resolvePayloadSpan(ref, file)
	if err != nil {
		release()
		return nil, err
	}
	if resolvedPath != path {
		release()
		file, release, err = n.store.acquireReadFile(resolvedPath)
		if err != nil {
			return nil, err
		}
	}
	defer release()
	reader := io.NewSectionReader(file, resolvedOffset, resolvedLen)
	data := make([]byte, resolvedLen)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}
	return data, nil
}

type segmentReader struct {
	reader  *io.SectionReader
	release func()
}

func (s *segmentReader) Read(p []byte) (int, error) { return s.reader.Read(p) }
func (s *segmentReader) Close() error {
	if s.release != nil {
		s.release()
	}
	return nil
}

func (n *logNamespace) openPayloadReader(ref *recordRef) (io.ReadCloser, error) {
	path := ref.segment.path
	file, release, err := n.store.acquireReadFile(path)
	if err != nil {
		return nil, err
	}
	resolvedPath, resolvedOffset, resolvedLen, err := n.resolvePayloadSpan(ref, file)
	if err != nil {
		release()
		return nil, err
	}
	if resolvedPath != path {
		release()
		file, release, err = n.store.acquireReadFile(resolvedPath)
		if err != nil {
			return nil, err
		}
	}
	reader := io.NewSectionReader(file, resolvedOffset, resolvedLen)
	return &segmentReader{reader: reader, release: release}, nil
}

func (n *logNamespace) resolvePayloadSpan(ref *recordRef, file *os.File) (string, int64, int64, error) {
	if ref == nil {
		return "", 0, 0, fmt.Errorf("disk: logstore missing record ref")
	}
	if ref.link != nil {
		return ref.link.path, ref.link.offset, ref.link.length, nil
	}
	if ref.segment == nil {
		return "", 0, 0, fmt.Errorf("disk: logstore missing segment")
	}
	if file == nil {
		return "", 0, 0, fmt.Errorf("disk: logstore missing segment file")
	}
	var headerBuf [logHeaderSize]byte
	if _, err := file.ReadAt(headerBuf[:], ref.offset); err != nil {
		return "", 0, 0, err
	}
	hdr, err := decodeHeader(headerBuf[:])
	if err != nil {
		return "", 0, 0, err
	}
	if hdr.keyLen > 0 {
		keyBytes := make([]byte, hdr.keyLen)
		if _, err := file.ReadAt(keyBytes, ref.offset+int64(logHeaderSize)); err != nil {
			return "", 0, 0, err
		}
		if ref.key != "" && string(keyBytes) != ref.key {
			return "", 0, 0, fmt.Errorf("disk: logstore key mismatch for %q", ref.key)
		}
	}
	payloadOffset := ref.offset + int64(logHeaderSize) + int64(hdr.keyLen) + int64(hdr.metaLen)
	return ref.segment.path, payloadOffset, int64(hdr.payloadLen), nil
}

type offsetWriter struct {
	file *os.File
	off  int64
}

func (w *offsetWriter) Write(p []byte) (int, error) {
	if w.file == nil {
		return 0, fmt.Errorf("disk: logstore missing file")
	}
	n, err := w.file.WriteAt(p, w.off)
	w.off += int64(n)
	return n, err
}

type crc32Writer struct {
	w   io.Writer
	crc hash.Hash32
}

func (w *crc32Writer) Write(p []byte) (int, error) {
	if w.w == nil || w.crc == nil {
		return 0, fmt.Errorf("disk: logstore missing writer")
	}
	n, err := w.w.Write(p)
	if n > 0 {
		_, _ = w.crc.Write(p[:n])
	}
	return n, err
}

func pathJoin(namespace, key string) string {
	if namespace == "" {
		return key
	}
	if key == "" {
		return namespace
	}
	if namespace[len(namespace)-1] == '/' {
		return namespace + key
	}
	return namespace + "/" + key
}

func readPayloadInline(payload io.Reader, limit int64, buf *bytes.Buffer) ([]byte, bool, error) {
	if payload == nil {
		return nil, false, nil
	}
	if limit <= 0 {
		limit = maxInlinePayload
	}
	if buf == nil {
		buf = &bytes.Buffer{}
	} else {
		buf.Reset()
	}
	if _, err := buf.ReadFrom(io.LimitReader(payload, limit+1)); err != nil {
		return nil, false, err
	}
	data := buf.Bytes()
	if int64(len(data)) > limit {
		return data, true, nil
	}
	return data, false, nil
}
