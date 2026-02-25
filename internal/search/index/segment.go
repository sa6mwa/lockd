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
	DocMeta   map[string]DocumentMetadata
	Format    uint32
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
		DocMeta:   make(map[string]DocumentMetadata),
		Format:    IndexFormatVersionV4,
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
	if s.Format == 0 {
		return fmt.Errorf("segment %s format required", s.ID)
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
	for key := range s.DocMeta {
		if key == "" {
			return fmt.Errorf("segment %s contains empty doc meta key", s.ID)
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
		DocMeta:       make([]*indexproto.DocumentMeta, 0, len(s.DocMeta)),
		FormatVersion: s.Format,
	}
	if s.Format >= IndexFormatVersionV5 {
		msg.V5 = segmentV5ToProto(s)
	} else {
		msg.Fields = make([]*indexproto.FieldBlock, 0, len(s.Fields))
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
	}
	if len(s.DocMeta) > 0 {
		keys := make([]string, 0, len(s.DocMeta))
		for key := range s.DocMeta {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			meta := s.DocMeta[key]
			entry := &indexproto.DocumentMeta{
				Key:                 key,
				StateEtag:           meta.StateETag,
				StatePlaintextBytes: meta.StatePlaintextBytes,
				PublishedVersion:    meta.PublishedVersion,
				QueryExcluded:       meta.QueryExcluded,
			}
			if len(meta.StateDescriptor) > 0 {
				entry.StateDescriptor = append([]byte(nil), meta.StateDescriptor...)
			}
			msg.DocMeta = append(msg.DocMeta, entry)
		}
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
		DocMeta:   make(map[string]DocumentMetadata, len(msg.GetDocMeta())),
		Format:    msg.GetFormatVersion(),
	}
	if segment.Format == 0 {
		segment.Format = IndexFormatVersionLegacy
	}
	if msg.GetV5() != nil && segment.Format >= IndexFormatVersionV5 {
		decodeSegmentV5(msg.GetV5(), segment)
	} else {
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
	}
	for _, entry := range msg.GetDocMeta() {
		if entry == nil {
			continue
		}
		meta := DocumentMetadata{
			StateETag:           entry.GetStateEtag(),
			StatePlaintextBytes: entry.GetStatePlaintextBytes(),
			PublishedVersion:    entry.GetPublishedVersion(),
			QueryExcluded:       entry.GetQueryExcluded(),
		}
		if len(entry.GetStateDescriptor()) > 0 {
			meta.StateDescriptor = append([]byte(nil), entry.GetStateDescriptor()...)
		}
		if key := entry.GetKey(); key != "" {
			segment.DocMeta[key] = meta
		}
	}
	return segment
}

func segmentV5ToProto(s *Segment) *indexproto.IndexSegmentV5 {
	if s == nil {
		return nil
	}
	keySet := make(map[string]struct{})
	for _, block := range s.Fields {
		for _, keys := range block.Postings {
			for _, key := range keys {
				if key == "" {
					continue
				}
				keySet[key] = struct{}{}
			}
		}
	}
	for key := range s.DocMeta {
		if key == "" {
			continue
		}
		keySet[key] = struct{}{}
	}
	docTable := make([]string, 0, len(keySet))
	for key := range keySet {
		docTable = append(docTable, key)
	}
	sort.Strings(docTable)
	keyToDocID := make(map[string]uint32, len(docTable))
	for i, key := range docTable {
		keyToDocID[key] = uint32(i)
	}

	fieldDict := make([]string, 0, len(s.Fields))
	for field := range s.Fields {
		fieldDict = append(fieldDict, field)
	}
	sort.Strings(fieldDict)
	fieldBlocks := make([]*indexproto.FieldBlockV5, 0, len(fieldDict))
	for fieldID, field := range fieldDict {
		block := s.Fields[field]
		terms := make([]string, 0, len(block.Postings))
		for term := range block.Postings {
			terms = append(terms, term)
		}
		sort.Strings(terms)
		postings := make([]*indexproto.PostingDocIDs, 0, len(terms))
		for termID, term := range terms {
			docIDs := docIDsForKeys(block.Postings[term], keyToDocID)
			if len(docIDs) == 0 {
				continue
			}
			postings = append(postings, &indexproto.PostingDocIDs{
				TermId: uint32(termID),
				DocIds: docIDs,
			})
		}
		fieldBlocks = append(fieldBlocks, &indexproto.FieldBlockV5{
			FieldId:  uint32(fieldID),
			TermDict: terms,
			Postings: postings,
		})
	}
	return &indexproto.IndexSegmentV5{
		DocTable:  docTable,
		FieldDict: fieldDict,
		Fields:    fieldBlocks,
	}
}

func decodeSegmentV5(v5 *indexproto.IndexSegmentV5, segment *Segment) {
	if v5 == nil || segment == nil {
		return
	}
	docTable := v5.GetDocTable()
	fieldDict := v5.GetFieldDict()
	for _, block := range v5.GetFields() {
		if block == nil {
			continue
		}
		fieldID := block.GetFieldId()
		if int(fieldID) >= len(fieldDict) {
			continue
		}
		field := fieldDict[fieldID]
		if field == "" {
			continue
		}
		termDict := block.GetTermDict()
		fb := FieldBlock{Postings: make(map[string][]string, len(block.GetPostings()))}
		for _, posting := range block.GetPostings() {
			if posting == nil {
				continue
			}
			termID := posting.GetTermId()
			if int(termID) >= len(termDict) {
				continue
			}
			term := termDict[termID]
			if term == "" {
				continue
			}
			keys := keysForDocIDs(posting.GetDocIds(), docTable)
			if len(keys) == 0 {
				continue
			}
			fb.Postings[term] = keys
		}
		if len(fb.Postings) > 0 {
			segment.Fields[field] = fb
		}
	}
}

func docIDsForKeys(keys []string, keyToDocID map[string]uint32) []uint32 {
	if len(keys) == 0 || len(keyToDocID) == 0 {
		return nil
	}
	ids := make([]uint32, 0, len(keys))
	for _, key := range keys {
		id, ok := keyToDocID[key]
		if !ok {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	w := 1
	for i := 1; i < len(ids); i++ {
		if ids[i] == ids[w-1] {
			continue
		}
		ids[w] = ids[i]
		w++
	}
	return ids[:w]
}

func keysForDocIDs(docIDs []uint32, docTable []string) []string {
	if len(docIDs) == 0 || len(docTable) == 0 {
		return nil
	}
	keys := make([]string, 0, len(docIDs))
	for _, id := range docIDs {
		if int(id) >= len(docTable) {
			continue
		}
		key := docTable[id]
		if key == "" {
			continue
		}
		keys = append(keys, key)
	}
	if len(keys) <= 1 {
		return keys
	}
	sort.Strings(keys)
	w := 1
	for i := 1; i < len(keys); i++ {
		if keys[i] == keys[w-1] {
			continue
		}
		keys[w] = keys[i]
		w++
	}
	return keys[:w]
}
