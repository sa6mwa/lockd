package index

import (
	"fmt"
	"sort"
	"time"

	indexproto "pkt.systems/lockd/internal/proto"
)

// Segment represents the immutable postings for a slice of data.
type Segment struct {
	ID        string
	CreatedAt time.Time
	Fields    map[string]FieldBlock
}

// FieldBlock holds postings per term.
type FieldBlock struct {
	Postings map[string][]string
}

// NewSegment initialises an empty segment with the provided identifier.
func NewSegment(id string, created time.Time) *Segment {
	return &Segment{
		ID:        id,
		CreatedAt: created,
		Fields:    make(map[string]FieldBlock),
	}
}

// Validate ensures the segment is structurally sound.
func (s *Segment) Validate() error {
	if s == nil {
		return fmt.Errorf("segment nil")
	}
	if s.ID == "" {
		return fmt.Errorf("segment id required")
	}
	for field, block := range s.Fields {
		if field == "" {
			return fmt.Errorf("segment %s contains empty field name", s.ID)
		}
		for term, keys := range block.Postings {
			if term == "" {
				return fmt.Errorf("segment %s field %s has empty term", s.ID, field)
			}
			if len(keys) == 0 {
				return fmt.Errorf("segment %s field %s term %s has no keys", s.ID, field, term)
			}
		}
	}
	return nil
}

// DocCount estimates the unique number of keys in the segment.
func (s *Segment) DocCount() uint64 {
	if s == nil {
		return 0
	}
	seen := make(map[string]struct{})
	for _, block := range s.Fields {
		for _, keys := range block.Postings {
			for _, key := range keys {
				seen[key] = struct{}{}
			}
		}
	}
	return uint64(len(seen))
}

// ToProto converts the segment to protobuf.
func (s *Segment) ToProto() *indexproto.IndexSegment {
	if s == nil {
		return nil
	}
	msg := &indexproto.IndexSegment{
		SegmentId:     s.ID,
		CreatedAtUnix: s.CreatedAt.Unix(),
		Fields:        make([]*indexproto.FieldBlock, 0, len(s.Fields)),
	}
	fields := make([]string, 0, len(s.Fields))
	for name := range s.Fields {
		fields = append(fields, name)
	}
	sort.Strings(fields)
	for _, name := range fields {
		block := s.Fields[name]
		protoBlock := &indexproto.FieldBlock{Name: name}
		terms := make([]string, 0, len(block.Postings))
		for term := range block.Postings {
			terms = append(terms, term)
		}
		sort.Strings(terms)
		for _, term := range terms {
			keys := append([]string(nil), block.Postings[term]...)
			protoBlock.Postings = append(protoBlock.Postings, &indexproto.TermPosting{Term: term, Keys: keys})
		}
		msg.Fields = append(msg.Fields, protoBlock)
	}
	return msg
}

// SegmentFromProto decodes a segment from protobuf.
func SegmentFromProto(msg *indexproto.IndexSegment) *Segment {
	if msg == nil {
		return nil
	}
	segment := &Segment{
		ID:        msg.GetSegmentId(),
		CreatedAt: time.Unix(msg.GetCreatedAtUnix(), 0).UTC(),
		Fields:    make(map[string]FieldBlock, len(msg.GetFields())),
	}
	for _, block := range msg.GetFields() {
		if block == nil {
			continue
		}
		fb := FieldBlock{Postings: make(map[string][]string, len(block.GetPostings()))}
		for _, posting := range block.GetPostings() {
			if posting == nil {
				continue
			}
			fb.Postings[posting.GetTerm()] = append([]string(nil), posting.GetKeys()...)
		}
		segment.Fields[block.GetName()] = fb
	}
	return segment
}
