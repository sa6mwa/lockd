package disk

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"pkt.systems/lockd/internal/uuidv7"
)

type compactionCapture struct {
	records      []*capturedRecord
	candidates   []*logSegment
	candidateSet map[string]struct{}
	reclaimBytes int64
}

type capturedRecord struct {
	indexKind compactIndexKind
	key       string
	current   *recordRef
}

type compactIndexKind uint8

const (
	compactIndexMeta compactIndexKind = iota + 1
	compactIndexState
	compactIndexObject
)

type compactionBuildResult struct {
	tempPath string
	segment  *logSegment
	records  []*compactedRecord
}

type compactedRecord struct {
	indexKind compactIndexKind
	key       string
	old       *recordRef
	newRef    *recordRef
}

func (s *logStore) setCompactionConfig(cfg logCompactionConfig) {
	s.mu.Lock()
	s.compaction = cfg
	enabled := cfg.enabled && cfg.interval > 0
	if enabled && s.stopCompaction == nil {
		s.stopCompaction = make(chan struct{})
		s.doneCompaction = make(chan struct{})
		go s.compactionLoop(s.stopCompaction, s.doneCompaction)
	}
	stopCh := s.stopCompaction
	doneCh := s.doneCompaction
	if !enabled && stopCh != nil {
		s.stopCompaction = nil
		s.doneCompaction = nil
	}
	s.mu.Unlock()
	if !enabled && stopCh != nil {
		close(stopCh)
		<-doneCh
	}
}

func (s *logStore) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		stopCh := s.stopCompaction
		doneCh := s.doneCompaction
		s.stopCompaction = nil
		s.doneCompaction = nil
		s.mu.Unlock()
		if stopCh != nil {
			close(stopCh)
			<-doneCh
		}
		s.readFileMu.Lock()
		var files []*os.File
		for path, cached := range s.readFiles {
			delete(s.readFiles, path)
			if cached != nil && cached.file != nil {
				files = append(files, cached.file)
			}
		}
		s.readFileLRU.Init()
		s.readFileMu.Unlock()
		for _, file := range files {
			_ = file.Close()
		}
	})
}

func (s *logStore) compactionLoop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	s.runCompactionPass()
	for {
		s.mu.Lock()
		interval := s.compaction.interval
		s.mu.Unlock()
		if interval <= 0 {
			return
		}
		timer := time.NewTimer(interval)
		select {
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		}
		s.runCompactionPass()
	}
}

func (s *logStore) runCompactionPass() {
	s.mu.Lock()
	cfg := s.compaction
	namespaces := make([]*logNamespace, 0, len(s.namespaces))
	for _, ns := range s.namespaces {
		if ns != nil {
			namespaces = append(namespaces, ns)
		}
	}
	s.mu.Unlock()
	if !cfg.enabled {
		return
	}
	for _, ns := range namespaces {
		_ = ns.compactOnce(cfg)
		_ = ns.cleanupObsolete(cfg.deleteGrace)
	}
}

func (n *logNamespace) compactOnce(cfg logCompactionConfig) error {
	if n == nil || !cfg.enabled {
		return nil
	}
	n.mu.Lock()
	if n.compacting {
		n.mu.Unlock()
		return nil
	}
	n.compacting = true
	defer func() {
		n.mu.Lock()
		n.compacting = false
		n.mu.Unlock()
	}()
	if err := n.ensureDirs(); err != nil {
		n.mu.Unlock()
		return err
	}
	if err := n.loadManifestLocked(); err != nil {
		n.mu.Unlock()
		return err
	}
	if err := n.scanSnapshotsLocked(); err != nil {
		n.mu.Unlock()
		return err
	}
	if err := n.scanSegmentsLocked(); err != nil {
		n.mu.Unlock()
		return err
	}
	capture := n.captureCompactionLocked(cfg)
	n.mu.Unlock()
	if capture == nil {
		return nil
	}
	result, err := n.buildCompactionSnapshot(capture, cfg)
	if err != nil {
		return err
	}
	installed := false
	defer func() {
		if installed {
			return
		}
		_ = os.Remove(result.tempPath)
	}()
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.validateCompactionLocked(capture) {
		return nil
	}
	if err := n.installCompactionLocked(result, capture, n.store.now()); err != nil {
		return err
	}
	installed = true
	return nil
}

func (n *logNamespace) captureCompactionLocked(cfg logCompactionConfig) *compactionCapture {
	var candidates []*logSegment
	candidateSet := make(map[string]struct{})
	var reclaimBytes int64
	if n.installedSnapshot != nil && !n.isObsoleteSnapshotLocked(n.installedSnapshot.name) {
		candidates = append(candidates, n.installedSnapshot)
		candidateSet[n.installedSnapshot.name] = struct{}{}
		reclaimBytes += n.installedSnapshot.size
	}
	for _, seg := range n.segments {
		if seg == nil || seg == n.active || !seg.sealed || n.isObsoleteSegmentLocked(seg.name) {
			continue
		}
		candidates = append(candidates, seg)
		candidateSet[seg.name] = struct{}{}
		reclaimBytes += seg.size
	}
	if len(candidates) < cfg.minSegments {
		return nil
	}
	if reclaimBytes < cfg.minReclaimBytes {
		return nil
	}
	capture := &compactionCapture{
		candidates:   candidates,
		candidateSet: candidateSet,
		reclaimBytes: reclaimBytes,
	}
	capture.records = append(capture.records, n.captureMetaRecordsLocked(candidateSet)...)
	capture.records = append(capture.records, n.captureStateRecordsLocked(candidateSet)...)
	capture.records = append(capture.records, n.captureObjectRecordsLocked(candidateSet)...)
	return capture
}

func (n *logNamespace) captureMetaRecordsLocked(candidates map[string]struct{}) []*capturedRecord {
	var records []*capturedRecord
	for _, key := range n.sortedMeta {
		ref := n.metaIndex[key]
		if ref == nil || ref.segment == nil {
			continue
		}
		if _, ok := candidates[ref.segment.name]; !ok {
			continue
		}
		records = append(records, &capturedRecord{indexKind: compactIndexMeta, key: key, current: ref})
	}
	return records
}

func (n *logNamespace) captureStateRecordsLocked(candidates map[string]struct{}) []*capturedRecord {
	if len(n.stateIndex) == 0 {
		return nil
	}
	keys := make([]string, 0, len(n.stateIndex))
	for key := range n.stateIndex {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var records []*capturedRecord
	for _, key := range keys {
		ref := n.stateIndex[key]
		if ref == nil || ref.segment == nil {
			continue
		}
		if _, ok := candidates[ref.segment.name]; !ok {
			continue
		}
		records = append(records, &capturedRecord{indexKind: compactIndexState, key: key, current: ref})
	}
	return records
}

func (n *logNamespace) captureObjectRecordsLocked(candidates map[string]struct{}) []*capturedRecord {
	var records []*capturedRecord
	for _, key := range n.sortedObjects {
		ref := n.objectIndex[key]
		if ref == nil || ref.segment == nil {
			continue
		}
		if _, ok := candidates[ref.segment.name]; !ok {
			continue
		}
		records = append(records, &capturedRecord{indexKind: compactIndexObject, key: key, current: ref})
	}
	return records
}

func (n *logNamespace) buildCompactionSnapshot(capture *compactionCapture, cfg logCompactionConfig) (*compactionBuildResult, error) {
	if err := os.MkdirAll(n.snapshotsDir, 0o755); err != nil {
		return nil, fmt.Errorf("disk: prepare snapshot dir: %w", err)
	}
	name := fmt.Sprintf("snap-%s-%s.log", n.store.writerID, strings.ReplaceAll(uuidv7.NewString(), "-", ""))
	tempPath := filepath.Join(n.snapshotsDir, "."+name+".tmp")
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("disk: create compaction snapshot: %w", err)
	}
	segment := &logSegment{name: name, path: filepath.Join(n.snapshotsDir, name), sealed: true}
	result := &compactionBuildResult{
		tempPath: tempPath,
		segment:  segment,
		records:  make([]*compactedRecord, 0, len(capture.records)),
	}
	defer file.Close()
	writer := &compactionWriter{
		file:  file,
		limit: cfg.maxIOBytesPerSec,
	}
	offset := int64(0)
	for _, record := range capture.records {
		ref, nextOffset, err := n.writeCompactedRecord(writer, segment, offset, record)
		if err != nil {
			return nil, err
		}
		offset = nextOffset
		result.records = append(result.records, &compactedRecord{
			indexKind: record.indexKind,
			key:       record.key,
			old:       record.current,
			newRef:    ref,
		})
	}
	if err := syncFile(file); err != nil {
		return nil, fmt.Errorf("disk: sync compaction snapshot: %w", err)
	}
	segment.size = offset
	return result, nil
}

func (n *logNamespace) writeCompactedRecord(writer *compactionWriter, segment *logSegment, offset int64, record *capturedRecord) (*recordRef, int64, error) {
	if record == nil || record.current == nil {
		return nil, offset, fmt.Errorf("disk: missing compacted record")
	}
	recType := record.current.recType
	if recType == logRecordStateLink {
		recType = logRecordStatePut
	}
	etagBytes, err := etagBytesForRecord(recType, record.current.meta.etag)
	if err != nil {
		return nil, offset, err
	}
	metaBytes, _, _, err := encodeMetaInto(nil, recType, record.current.meta, etagBytes)
	if err != nil {
		return nil, offset, err
	}
	payloadReader, payloadLen, err := n.compactionPayloadReader(record.current, recType)
	if err != nil {
		return nil, offset, err
	}
	if payloadReader != nil {
		defer payloadReader.Close()
	}
	crc, err := writer.writeRecord(record.key, recType, metaBytes, payloadReader, payloadLen)
	if err != nil {
		return nil, offset, err
	}
	payloadOffset := offset + int64(logHeaderSize+len(record.key)+len(metaBytes))
	if payloadLen == 0 {
		payloadOffset = 0
	}
	ref := &recordRef{
		recType:       recType,
		key:           record.key,
		meta:          record.current.meta,
		segment:       segment,
		offset:        offset,
		payloadOffset: payloadOffset,
		payloadLen:    payloadLen,
	}
	_ = crc
	return ref, writer.offset, nil
}

func (n *logNamespace) compactionPayloadReader(ref *recordRef, recType logRecordType) (io.ReadCloser, int64, error) {
	if ref == nil {
		return nil, 0, fmt.Errorf("disk: missing compaction ref")
	}
	switch recType {
	case logRecordMetaPut:
		return nil, 0, nil
	case logRecordStatePut, logRecordObjectPut:
		reader, err := n.openPayloadReader(ref)
		if err != nil {
			return nil, 0, err
		}
		_, _, resolvedLen, err := n.resolvePayloadSpan(ref, nil)
		if err != nil {
			reader.Close()
			return nil, 0, err
		}
		return reader, resolvedLen, nil
	default:
		return nil, 0, fmt.Errorf("disk: unsupported compaction record type %d", recType)
	}
}

func (n *logNamespace) validateCompactionLocked(capture *compactionCapture) bool {
	for _, record := range capture.records {
		if record == nil {
			continue
		}
		switch record.indexKind {
		case compactIndexMeta:
			if n.metaIndex[record.key] != record.current {
				return false
			}
		case compactIndexState:
			if n.stateIndex[record.key] != record.current {
				return false
			}
		case compactIndexObject:
			if n.objectIndex[record.key] != record.current {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func (n *logNamespace) installCompactionLocked(result *compactionBuildResult, capture *compactionCapture, now time.Time) error {
	if result == nil || result.segment == nil {
		return fmt.Errorf("disk: missing compaction result")
	}
	if err := os.Rename(result.tempPath, result.segment.path); err != nil {
		return fmt.Errorf("disk: install compaction snapshot: %w", err)
	}
	oldSnapshot := n.installedSnapshot
	var manifestEntries []manifestEntry
	manifestEntries = append(manifestEntries, manifestEntry{op: "snapshot-install", name: result.segment.name})
	newObsoleteSegments := make(map[string]time.Time)
	newObsoleteSnapshots := make(map[string]time.Time)
	for _, seg := range capture.candidates {
		if seg == nil {
			continue
		}
		if oldSnapshot != nil && seg == oldSnapshot {
			newObsoleteSnapshots[seg.name] = now.UTC()
			manifestEntries = append(manifestEntries, manifestEntry{op: "obsolete-snapshot", name: seg.name, timestamp: now.UTC()})
			continue
		}
		newObsoleteSegments[seg.name] = now.UTC()
		manifestEntries = append(manifestEntries, manifestEntry{op: "obsolete-segment", name: seg.name, timestamp: now.UTC()})
	}
	if oldSnapshot != nil && oldSnapshot != result.segment {
		if _, ok := newObsoleteSnapshots[oldSnapshot.name]; !ok {
			newObsoleteSnapshots[oldSnapshot.name] = now.UTC()
			manifestEntries = append(manifestEntries, manifestEntry{op: "obsolete-snapshot", name: oldSnapshot.name, timestamp: now.UTC()})
		}
	}
	if n.manifest != nil {
		if err := n.manifest.appendEntries(manifestEntries...); err != nil {
			_ = os.Remove(result.segment.path)
			return err
		}
	}
	result.segment.sealed = true
	result.segment.readOffset = 0
	n.snapshots[result.segment.name] = result.segment
	n.installedSnapshot = result.segment
	for _, record := range result.records {
		if record == nil || record.newRef == nil {
			continue
		}
		switch record.indexKind {
		case compactIndexMeta:
			n.metaIndex[record.key] = record.newRef
		case compactIndexState:
			n.stateIndex[record.key] = record.newRef
		case compactIndexObject:
			n.objectIndex[record.key] = record.newRef
		}
	}
	if n.obsoleteSegments == nil {
		n.obsoleteSegments = make(map[string]time.Time)
	}
	if n.obsoleteSnapshots == nil {
		n.obsoleteSnapshots = make(map[string]time.Time)
	}
	for name, ts := range newObsoleteSegments {
		n.obsoleteSegments[name] = ts
	}
	for name, ts := range newObsoleteSnapshots {
		n.obsoleteSnapshots[name] = ts
	}
	return nil
}

func (n *logNamespace) cleanupObsolete(grace time.Duration) error {
	if grace < 0 {
		return nil
	}
	n.mu.Lock()
	now := n.store.now()
	var removeSegments []*logSegment
	var removeSnapshots []*logSegment
	for name, ts := range n.obsoleteSegments {
		if now.Sub(ts) < grace {
			continue
		}
		if seg := n.segments[name]; seg != nil {
			removeSegments = append(removeSegments, seg)
		}
		delete(n.obsoleteSegments, name)
		delete(n.segments, name)
	}
	for name, ts := range n.obsoleteSnapshots {
		if now.Sub(ts) < grace {
			continue
		}
		if seg := n.snapshots[name]; seg != nil {
			removeSnapshots = append(removeSnapshots, seg)
		}
		delete(n.obsoleteSnapshots, name)
		delete(n.snapshots, name)
	}
	n.mu.Unlock()
	for _, seg := range removeSegments {
		_ = os.Remove(seg.path)
	}
	for _, seg := range removeSnapshots {
		_ = os.Remove(seg.path)
	}
	return nil
}

type compactionWriter struct {
	file   *os.File
	limit  int64
	offset int64
}

func (w *compactionWriter) writeRecord(key string, recType logRecordType, metaBytes []byte, payload io.Reader, payloadLen int64) (uint32, error) {
	header := recordHeader{
		recType:    recType,
		keyLen:     uint32(len(key)),
		metaLen:    uint32(len(metaBytes)),
		payloadLen: uint32(payloadLen),
	}
	var headerBuf [logHeaderSize]byte
	prefixLen := logHeaderSize + len(key) + len(metaBytes)
	prefix := make([]byte, prefixLen)
	encodeHeader(headerBuf[:], header)
	copy(prefix, headerBuf[:])
	copy(prefix[logHeaderSize:], key)
	copy(prefix[logHeaderSize+len(key):], metaBytes)
	if _, err := w.file.WriteAt(prefix, w.offset); err != nil {
		return 0, err
	}
	crc := crc32.NewIEEE()
	if payload != nil && payloadLen > 0 {
		reader := io.TeeReader(payload, crc)
		if err := w.writePayload(reader, w.offset+int64(prefixLen), payloadLen); err != nil {
			return 0, err
		}
	}
	header.payloadCRC = crc.Sum32()
	encodeHeader(headerBuf[:], header)
	if _, err := w.file.WriteAt(headerBuf[:], w.offset); err != nil {
		return 0, err
	}
	w.offset += int64(prefixLen) + payloadLen
	return header.payloadCRC, nil
}

func (w *compactionWriter) writePayload(reader io.Reader, offset int64, payloadLen int64) error {
	buf := make([]byte, 128<<10)
	written := int64(0)
	for written < payloadLen {
		chunk := buf
		remaining := payloadLen - written
		if int64(len(chunk)) > remaining {
			chunk = chunk[:remaining]
		}
		n, err := io.ReadFull(reader, chunk)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return fmt.Errorf("disk: compaction payload truncated")
			}
			return err
		}
		if err := w.writeChunk(offset+written, chunk[:n]); err != nil {
			return err
		}
		written += int64(n)
	}
	return nil
}

func (w *compactionWriter) writeChunk(offset int64, chunk []byte) error {
	if len(chunk) == 0 {
		return nil
	}
	if _, err := w.file.WriteAt(chunk, offset); err != nil {
		return err
	}
	if w.limit > 0 {
		sleep := time.Duration((int64(len(chunk)) * int64(time.Second)) / w.limit)
		if sleep > 0 {
			time.Sleep(sleep)
		}
	}
	return nil
}

func (s *Store) CompactLogstore(ctx context.Context, namespace string) error {
	_ = ctx
	if s == nil || s.logstore == nil {
		return nil
	}
	ln, err := s.logstore.namespace(namespace)
	if err != nil {
		return err
	}
	s.logstore.mu.Lock()
	cfg := s.logstore.compaction
	s.logstore.mu.Unlock()
	return ln.compactOnce(cfg)
}

func writeCompactionRecordBytes(recType logRecordType, key string, meta recordMeta, payload []byte) ([]byte, error) {
	etagBytes, err := etagBytesForRecord(recType, meta.etag)
	if err != nil {
		return nil, err
	}
	metaBytes, _, _, err := encodeMetaInto(nil, recType, meta, etagBytes)
	if err != nil {
		return nil, err
	}
	header := recordHeader{
		recType:    recType,
		keyLen:     uint32(len(key)),
		metaLen:    uint32(len(metaBytes)),
		payloadLen: uint32(len(payload)),
		payloadCRC: crc32.ChecksumIEEE(payload),
	}
	var headerBuf [logHeaderSize]byte
	encodeHeader(headerBuf[:], header)
	buf := bytes.NewBuffer(make([]byte, 0, logHeaderSize+len(key)+len(metaBytes)+len(payload)))
	buf.Write(headerBuf[:])
	buf.WriteString(key)
	buf.Write(metaBytes)
	buf.Write(payload)
	return buf.Bytes(), nil
}
