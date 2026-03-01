package search

import "strconv"

const (
	// MetadataQueryCandidatesSeen is the metadata key for streamed candidate count.
	MetadataQueryCandidatesSeen = "query_candidates_seen"
	// MetadataQueryCandidatesMatched is the metadata key for matched candidate count.
	MetadataQueryCandidatesMatched = "query_candidates_matched"
	// MetadataQueryBytesCaptured is the metadata key for captured candidate bytes.
	MetadataQueryBytesCaptured = "query_bytes_captured"
	// MetadataQuerySpillCount is the metadata key for spill event count.
	MetadataQuerySpillCount = "query_spill_count"
	// MetadataQuerySpillBytes is the metadata key for bytes spilled to disk.
	MetadataQuerySpillBytes = "query_spill_bytes"
)

// StreamStats tracks streaming selector counters for query return=documents.
type StreamStats struct {
	CandidatesSeen    int64
	CandidatesMatched int64
	BytesCaptured     int64
	SpillCount        int64
	SpillBytes        int64
}

// Add aggregates stream counters from one evaluation run.
func (s *StreamStats) Add(seen, matched, captured, spillCount, spillBytes int64) {
	if s == nil {
		return
	}
	s.CandidatesSeen += seen
	s.CandidatesMatched += matched
	s.BytesCaptured += captured
	s.SpillCount += spillCount
	s.SpillBytes += spillBytes
}

// AddCandidate increments candidate counters for non-LQL fast paths.
func (s *StreamStats) AddCandidate(matched bool) {
	if s == nil {
		return
	}
	s.CandidatesSeen++
	if matched {
		s.CandidatesMatched++
	}
}

// ApplyMetadata writes stream counters into query metadata.
func (s StreamStats) ApplyMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata[MetadataQueryCandidatesSeen] = strconv.FormatInt(s.CandidatesSeen, 10)
	metadata[MetadataQueryCandidatesMatched] = strconv.FormatInt(s.CandidatesMatched, 10)
	metadata[MetadataQueryBytesCaptured] = strconv.FormatInt(s.BytesCaptured, 10)
	metadata[MetadataQuerySpillCount] = strconv.FormatInt(s.SpillCount, 10)
	metadata[MetadataQuerySpillBytes] = strconv.FormatInt(s.SpillBytes, 10)
	return metadata
}
