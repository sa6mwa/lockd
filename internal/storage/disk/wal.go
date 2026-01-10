package disk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	walMagic             = uint32(0x4c574c44) // "LWLD"
	walVersion           = uint8(1)
	walRecordMutations   = uint8(1)
	walHeaderSize        = 24
	walSegmentSize       = int64(64 << 20)
	walCheckpointOps     = uint64(1000)
	walCheckpointEvery   = time.Second
	walSegmentFilePrefix = "segment-"
	walSegmentFileSuffix = ".wal"
)

type walOp uint8

const (
	walOpWrite  walOp = 1
	walOpDelete walOp = 2
)

type walEntry struct {
	op       walOp
	path     string
	data     []byte
	dataPath string
	dataSize int64
}

type walStore struct {
	store           *Store
	namespace       string
	dir             string
	maxSegment      int64
	mu              sync.Mutex
	recoverOnce     sync.Once
	recoverErr      error
	nextLSN         uint64
	appliedLSN      uint64
	pendingApply    map[uint64]struct{}
	lastCheckpoint  time.Time
	checkpointCount uint64
	currentSegment  *os.File
	currentIndex    uint64
	segmentMaxLSN   map[uint64]uint64
}

func (s *Store) withWAL(namespace string, entries []walEntry, apply func() error) error {
	if !s.walEnabled {
		return apply()
	}
	wal, err := s.walForNamespace(namespace)
	if err != nil {
		return err
	}
	if wal == nil {
		return apply()
	}
	lsn, err := wal.append(entries)
	if err != nil {
		return err
	}
	if err := apply(); err != nil {
		return err
	}
	wal.markApplied(lsn)
	return nil
}

func (s *Store) walRelPath(namespace, absPath string) (string, error) {
	base := s.namespacePath(namespace)
	rel, err := filepath.Rel(base, absPath)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("disk: wal path outside namespace %q", absPath)
	}
	return filepath.ToSlash(rel), nil
}

func (s *Store) walForNamespace(namespace string) (*walStore, error) {
	if !s.walEnabled {
		return nil, nil
	}
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		return nil, fmt.Errorf("disk: wal namespace required")
	}
	s.walMu.Lock()
	wal := s.walByNamespace[ns]
	if wal == nil {
		wal = &walStore{
			store:          s,
			namespace:      ns,
			dir:            s.namespaceWalDir(ns),
			maxSegment:     walSegmentSize,
			pendingApply:   make(map[uint64]struct{}),
			lastCheckpoint: time.Now(),
			segmentMaxLSN:  make(map[uint64]uint64),
		}
		s.walByNamespace[ns] = wal
	}
	s.walMu.Unlock()
	if err := wal.ensureRecovered(); err != nil {
		return nil, err
	}
	return wal, nil
}

func (w *walStore) ensureRecovered() error {
	w.recoverOnce.Do(func() {
		w.recoverErr = w.recover()
	})
	return w.recoverErr
}

func (w *walStore) recover() error {
	if w == nil || w.store == nil {
		return nil
	}
	if err := os.MkdirAll(w.dir, 0o755); err != nil {
		return fmt.Errorf("disk: wal mkdir: %w", err)
	}
	lf, err := w.openWALLock()
	if err != nil {
		return err
	}
	defer func() {
		_ = unlockFile(lf)
		_ = lf.Close()
	}()

	if err := lockFile(lf); err != nil {
		return err
	}

	checkpoint, err := w.readCheckpoint()
	if err != nil {
		return err
	}
	w.appliedLSN = checkpoint
	w.nextLSN = checkpoint + 1

	segments, err := w.listSegments()
	if err != nil {
		return err
	}
	for _, seg := range segments {
		maxLSN, err := w.replaySegment(seg, checkpoint)
		if err != nil {
			return err
		}
		if maxLSN > 0 {
			w.segmentMaxLSN[seg.index] = maxLSN
		}
		if maxLSN > w.appliedLSN {
			w.appliedLSN = maxLSN
			w.nextLSN = maxLSN + 1
		}
	}
	_ = w.writeCheckpointLocked()
	_ = w.pruneSegmentsLocked()
	return nil
}

func (w *walStore) append(entries []walEntry) (uint64, error) {
	if err := w.ensureRecovered(); err != nil {
		return 0, err
	}
	for i := range entries {
		if entries[i].op != walOpWrite || entries[i].dataPath == "" || entries[i].dataSize > 0 || len(entries[i].data) > 0 {
			continue
		}
		info, err := os.Stat(entries[i].dataPath)
		if err != nil {
			return 0, err
		}
		entries[i].dataSize = info.Size()
	}
	payloadSize, err := walPayloadSize(entries)
	if err != nil {
		return 0, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	lf, err := w.openWALLock()
	if err != nil {
		return 0, err
	}
	if err := lockFile(lf); err != nil {
		lf.Close()
		return 0, err
	}
	defer func() {
		_ = unlockFile(lf)
		_ = lf.Close()
	}()

	if err := w.ensureSegment(payloadSize); err != nil {
		return 0, err
	}
	lsn := w.nextLSN
	w.nextLSN++

	offset, err := w.currentSegment.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	header := make([]byte, walHeaderSize)
	if _, err := w.currentSegment.Write(header); err != nil {
		return 0, err
	}
	writer := newWalPayloadWriter(w.currentSegment)
	if err := writer.writeUint16(uint16(len(entries))); err != nil {
		return 0, err
	}
	for _, entry := range entries {
		if err := writer.writeUint8(uint8(entry.op)); err != nil {
			return 0, err
		}
		if err := writer.writeString(entry.path); err != nil {
			return 0, err
		}
		if entry.op == walOpWrite {
			dataLen := entry.dataSize
			if len(entry.data) > 0 {
				dataLen = int64(len(entry.data))
			}
			if dataLen < 0 || dataLen > int64(^uint32(0)) {
				return 0, fmt.Errorf("disk: wal entry too large (%d bytes)", dataLen)
			}
			if err := writer.writeUint32(uint32(dataLen)); err != nil {
				return 0, err
			}
			if len(entry.data) > 0 {
				if _, err := writer.Write(entry.data); err != nil {
					return 0, err
				}
				continue
			}
			if entry.dataPath == "" {
				return 0, fmt.Errorf("disk: wal entry missing data")
			}
			f, err := os.Open(entry.dataPath)
			if err != nil {
				return 0, err
			}
			_, err = io.CopyN(writer, f, dataLen)
			_ = f.Close()
			if err != nil {
				return 0, err
			}
		}
	}
	payloadLen := writer.length()
	crc := writer.checksum()
	if err := writeWalHeaderAt(w.currentSegment, offset, lsn, payloadLen, crc); err != nil {
		return 0, err
	}
	if _, err := w.currentSegment.Seek(0, io.SeekEnd); err != nil {
		return 0, err
	}
	if err := w.currentSegment.Sync(); err != nil {
		return 0, err
	}
	w.segmentMaxLSN[w.currentIndex] = lsn
	return lsn, nil
}

func (w *walStore) markApplied(lsn uint64) {
	if lsn == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if lsn <= w.appliedLSN {
		return
	}
	if w.pendingApply == nil {
		w.pendingApply = make(map[uint64]struct{})
	}
	w.pendingApply[lsn] = struct{}{}
	for {
		next := w.appliedLSN + 1
		if _, ok := w.pendingApply[next]; !ok {
			break
		}
		delete(w.pendingApply, next)
		w.appliedLSN = next
		w.checkpointCount++
	}
	if w.checkpointCount >= walCheckpointOps || time.Since(w.lastCheckpoint) >= walCheckpointEvery {
		_ = w.writeCheckpointLocked()
		_ = w.pruneSegmentsLocked()
		w.checkpointCount = 0
		w.lastCheckpoint = time.Now()
	}
}

func (w *walStore) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.currentSegment != nil {
		err := w.currentSegment.Close()
		w.currentSegment = nil
		return err
	}
	return nil
}

type walSegment struct {
	path  string
	index uint64
}

func (w *walStore) listSegments() ([]walSegment, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var segments []walSegment
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, walSegmentFilePrefix) || !strings.HasSuffix(name, walSegmentFileSuffix) {
			continue
		}
		indexStr := strings.TrimSuffix(strings.TrimPrefix(name, walSegmentFilePrefix), walSegmentFileSuffix)
		index, err := strconv.ParseUint(indexStr, 10, 64)
		if err != nil {
			continue
		}
		segments = append(segments, walSegment{path: filepath.Join(w.dir, name), index: index})
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].index < segments[j].index })
	return segments, nil
}

func (w *walStore) ensureSegment(nextPayload uint32) error {
	if w.currentSegment == nil {
		return w.openSegment(0)
	}
	info, err := w.currentSegment.Stat()
	if err != nil {
		return err
	}
	nextSize := info.Size() + int64(walHeaderSize) + int64(nextPayload)
	if nextSize <= w.maxSegment {
		return nil
	}
	return w.openSegment(w.currentIndex + 1)
}

func (w *walStore) openSegment(index uint64) error {
	if w.currentSegment != nil {
		_ = w.currentSegment.Close()
		w.currentSegment = nil
	}
	if index == 0 {
		segments, err := w.listSegments()
		if err != nil {
			return err
		}
		if len(segments) > 0 {
			index = segments[len(segments)-1].index
		} else {
			index = 1
		}
	}
	name := fmt.Sprintf("%s%08d%s", walSegmentFilePrefix, index, walSegmentFileSuffix)
	path := filepath.Join(w.dir, name)
	if err := os.MkdirAll(w.dir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return err
	}
	w.currentSegment = f
	w.currentIndex = index
	return nil
}

func (w *walStore) replaySegment(seg walSegment, checkpoint uint64) (uint64, error) {
	f, err := os.OpenFile(seg.path, os.O_RDWR, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var maxLSN uint64
	var offset int64
	for {
		header := make([]byte, walHeaderSize)
		n, err := io.ReadFull(f, header)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if n == 0 {
					return maxLSN, nil
				}
				_ = f.Truncate(offset)
				return maxLSN, nil
			}
			return maxLSN, err
		}
		magic := binary.LittleEndian.Uint32(header[0:4])
		if magic != walMagic {
			_ = f.Truncate(offset)
			return maxLSN, nil
		}
		if header[4] != walVersion || header[5] != walRecordMutations {
			_ = f.Truncate(offset)
			return maxLSN, nil
		}
		lsn := binary.LittleEndian.Uint64(header[8:16])
		payloadLen := binary.LittleEndian.Uint32(header[16:20])
		crc := binary.LittleEndian.Uint32(header[20:24])

		if payloadLen == 0 {
			_ = f.Truncate(offset)
			return maxLSN, nil
		}
		payloadOffset := offset + walHeaderSize
		if _, err := f.Seek(payloadOffset, io.SeekStart); err != nil {
			return maxLSN, err
		}
		reader := newWalPayloadReader(f, payloadLen)
		var actions []walAction
		if lsn > checkpoint {
			var parseErr error
			actions, parseErr = w.parseWalPayload(reader)
			if parseErr != nil {
				cleanupWalActions(actions)
				return maxLSN, parseErr
			}
		} else if err := reader.discard(); err != nil {
			return maxLSN, err
		}
		if reader.checksum() != crc {
			cleanupWalActions(actions)
			_ = f.Truncate(offset)
			return maxLSN, nil
		}
		if lsn > checkpoint {
			if err := w.applyWalActions(actions); err != nil {
				cleanupWalActions(actions)
				return maxLSN, err
			}
		}
		offset = payloadOffset + int64(payloadLen)
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return maxLSN, err
		}
		if lsn > checkpoint {
			maxLSN = lsn
		}
	}
}

type walAction struct {
	op       walOp
	destPath string
	tmpPath  string
}

func (w *walStore) parseWalPayload(reader *walPayloadReader) ([]walAction, error) {
	if reader == nil {
		return nil, nil
	}
	entryCount, err := reader.readUint16()
	if err != nil {
		return nil, err
	}
	var actions []walAction
	for i := 0; i < int(entryCount); i++ {
		opVal, err := reader.readUint8()
		if err != nil {
			return nil, err
		}
		pathStr, err := reader.readString()
		if err != nil {
			return nil, err
		}
		rel := filepath.FromSlash(pathStr)
		if strings.HasPrefix(rel, "..") {
			return nil, fmt.Errorf("disk: wal path outside namespace %q", pathStr)
		}
		dest := filepath.Join(w.store.namespacePath(w.namespace), rel)
		switch walOp(opVal) {
		case walOpWrite:
			dataLen, err := reader.readUint32()
			if err != nil {
				return nil, err
			}
			tmpDir := w.store.namespaceTmpDir(w.namespace)
			if err := os.MkdirAll(tmpDir, 0o755); err != nil {
				return nil, err
			}
			tmp, err := os.CreateTemp(tmpDir, "lockd-wal-*")
			if err != nil {
				return nil, err
			}
			if _, err := io.CopyN(tmp, reader, int64(dataLen)); err != nil {
				tmp.Close()
				os.Remove(tmp.Name())
				return nil, err
			}
			if err := tmp.Close(); err != nil {
				os.Remove(tmp.Name())
				return nil, err
			}
			actions = append(actions, walAction{op: walOpWrite, destPath: dest, tmpPath: tmp.Name()})
		case walOpDelete:
			actions = append(actions, walAction{op: walOpDelete, destPath: dest})
		default:
			return nil, fmt.Errorf("disk: wal unknown op %d", opVal)
		}
	}
	if reader.remainingBytes() != 0 {
		return nil, fmt.Errorf("disk: wal payload length mismatch")
	}
	return actions, nil
}

func (w *walStore) applyWalActions(actions []walAction) error {
	for _, act := range actions {
		switch act.op {
		case walOpWrite:
			if err := os.MkdirAll(filepath.Dir(act.destPath), 0o755); err != nil {
				return err
			}
			if err := renameReplace(act.tmpPath, act.destPath); err != nil {
				return err
			}
		case walOpDelete:
			if err := os.Remove(act.destPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}

func cleanupWalActions(actions []walAction) {
	for _, act := range actions {
		if act.tmpPath != "" {
			_ = os.Remove(act.tmpPath)
		}
	}
}

func (w *walStore) readCheckpoint() (uint64, error) {
	path := filepath.Join(w.dir, "checkpoint")
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	if len(data) < 8 {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(data[:8]), nil
}

func (w *walStore) writeCheckpointLocked() error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, w.appliedLSN)
	path := filepath.Join(w.dir, "checkpoint")
	tmpDir := w.store.namespaceTmpDir(w.namespace)
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(tmpDir, "lockd-wal-checkpoint-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(buf); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return nil
}

func (w *walStore) pruneSegmentsLocked() error {
	for index, maxLSN := range w.segmentMaxLSN {
		if index == w.currentIndex {
			continue
		}
		if maxLSN == 0 || maxLSN > w.appliedLSN {
			continue
		}
		name := fmt.Sprintf("%s%08d%s", walSegmentFilePrefix, index, walSegmentFileSuffix)
		path := filepath.Join(w.dir, name)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			continue
		}
		delete(w.segmentMaxLSN, index)
	}
	return nil
}

func (w *walStore) openWALLock() (*os.File, error) {
	lockPath := filepath.Join(w.dir, "lock")
	if err := os.MkdirAll(w.dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("disk: wal lock: %w", err)
	}
	return f, nil
}

type walPayloadWriter struct {
	w   io.Writer
	crc hash32
	n   uint32
}

type hash32 interface {
	Write([]byte) (int, error)
	Sum32() uint32
}

func newWalPayloadWriter(w io.Writer) *walPayloadWriter {
	return &walPayloadWriter{w: w, crc: crc32.NewIEEE()}
}

func (p *walPayloadWriter) Write(b []byte) (int, error) {
	n, err := p.w.Write(b)
	if n > 0 {
		_, _ = p.crc.Write(b[:n])
		p.n += uint32(n)
	}
	if err == nil && n != len(b) {
		return n, io.ErrShortWrite
	}
	return n, err
}

func (p *walPayloadWriter) writeUint8(v uint8) error {
	return p.writeBytes([]byte{v})
}

func (p *walPayloadWriter) writeUint16(v uint16) error {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], v)
	return p.writeBytes(buf[:])
}

func (p *walPayloadWriter) writeUint32(v uint32) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	return p.writeBytes(buf[:])
}

func (p *walPayloadWriter) writeString(s string) error {
	if len(s) > int(^uint16(0)) {
		return fmt.Errorf("disk: wal path too long")
	}
	if err := p.writeUint16(uint16(len(s))); err != nil {
		return err
	}
	return p.writeBytes([]byte(s))
}

func (p *walPayloadWriter) writeBytes(b []byte) error {
	_, err := p.Write(b)
	return err
}

func (p *walPayloadWriter) length() uint32   { return p.n }
func (p *walPayloadWriter) checksum() uint32 { return p.crc.Sum32() }

type walPayloadReader struct {
	r         io.Reader
	remaining uint32
	crc       hash32
}

func newWalPayloadReader(r io.Reader, n uint32) *walPayloadReader {
	return &walPayloadReader{r: io.LimitReader(r, int64(n)), remaining: n, crc: crc32.NewIEEE()}
}

func (p *walPayloadReader) Read(b []byte) (int, error) {
	if p.remaining == 0 {
		return 0, io.EOF
	}
	if uint32(len(b)) > p.remaining {
		b = b[:p.remaining]
	}
	n, err := p.r.Read(b)
	if n > 0 {
		_, _ = p.crc.Write(b[:n])
		p.remaining -= uint32(n)
	}
	return n, err
}

func (p *walPayloadReader) readUint8() (uint8, error) {
	var buf [1]byte
	if _, err := io.ReadFull(p, buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}

func (p *walPayloadReader) readUint16() (uint16, error) {
	var buf [2]byte
	if _, err := io.ReadFull(p, buf[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(buf[:]), nil
}

func (p *walPayloadReader) readUint32() (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(p, buf[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

func (p *walPayloadReader) readString() (string, error) {
	n, err := p.readUint16()
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(p, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func (p *walPayloadReader) discard() error {
	if p.remaining == 0 {
		return nil
	}
	_, err := io.CopyN(io.Discard, p, int64(p.remaining))
	return err
}

func (p *walPayloadReader) remainingBytes() uint32 { return p.remaining }
func (p *walPayloadReader) checksum() uint32       { return p.crc.Sum32() }

func walPayloadSize(entries []walEntry) (uint32, error) {
	var total uint64 = 2 // entryCount
	for _, entry := range entries {
		if entry.path == "" {
			return 0, fmt.Errorf("disk: wal entry path required")
		}
		if len(entry.path) > int(^uint16(0)) {
			return 0, fmt.Errorf("disk: wal path too long")
		}
		total += 1 + 2 + uint64(len(entry.path))
		if entry.op == walOpWrite {
			size := entry.dataSize
			if len(entry.data) > 0 {
				size = int64(len(entry.data))
			}
			if size < 0 {
				return 0, fmt.Errorf("disk: wal entry size invalid")
			}
			total += 4 + uint64(size)
		}
	}
	if total > uint64(^uint32(0)) {
		return 0, fmt.Errorf("disk: wal payload too large")
	}
	return uint32(total), nil
}

func writeWalHeaderAt(f *os.File, offset int64, lsn uint64, payloadLen uint32, crc uint32) error {
	header := make([]byte, walHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], walMagic)
	header[4] = walVersion
	header[5] = walRecordMutations
	binary.LittleEndian.PutUint16(header[6:8], 0)
	binary.LittleEndian.PutUint64(header[8:16], lsn)
	binary.LittleEndian.PutUint32(header[16:20], payloadLen)
	binary.LittleEndian.PutUint32(header[20:24], crc)
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	if _, err := f.Write(header); err != nil {
		return err
	}
	return nil
}

func renameReplace(src, dest string) error {
	if err := os.Rename(src, dest); err == nil {
		return nil
	}
	if err := os.Remove(dest); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(src, dest)
}
