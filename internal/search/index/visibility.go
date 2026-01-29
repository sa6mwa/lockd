package index

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// VisibilityManifest tracks persisted visibility segments.
type VisibilityManifest struct {
	Seq       uint64
	UpdatedAt time.Time
	Segments  []VisibilitySegmentRef
}

// VisibilitySegmentRef references a visibility segment.
type VisibilitySegmentRef struct {
	ID        string
	CreatedAt time.Time
	Entries   uint64
}

// VisibilitySegment stores visibility overrides for keys.
type VisibilitySegment struct {
	ID        string
	CreatedAt time.Time
	Entries   []VisibilityEntry
}

// VisibilityEntry captures a visibility decision for a key.
type VisibilityEntry struct {
	Key     string
	Visible bool
}

// NewVisibilityManifest returns an empty visibility manifest.
func NewVisibilityManifest() *VisibilityManifest {
	return &VisibilityManifest{}
}

// Validate checks required fields on the visibility manifest.
func (m *VisibilityManifest) Validate() error {
	if m == nil {
		return fmt.Errorf("visibility manifest nil")
	}
	for _, seg := range m.Segments {
		if seg.ID == "" {
			return fmt.Errorf("visibility manifest segment id required")
		}
	}
	return nil
}

// Validate checks required fields on the visibility segment.
func (s *VisibilitySegment) Validate() error {
	if s == nil {
		return fmt.Errorf("visibility segment nil")
	}
	if s.ID == "" {
		return fmt.Errorf("visibility segment id required")
	}
	for _, entry := range s.Entries {
		if entry.Key == "" {
			return fmt.Errorf("visibility segment %s contains empty key", s.ID)
		}
	}
	return nil
}

// MarshalJSON encodes the visibility manifest.
func (m *VisibilityManifest) MarshalJSON() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	segments := make([]visibilitySegmentRefJSON, 0, len(m.Segments))
	for _, seg := range m.Segments {
		segments = append(segments, visibilitySegmentRefJSON{
			ID:            seg.ID,
			CreatedAtUnix: seg.CreatedAt.Unix(),
			EntryCount:    seg.Entries,
		})
	}
	payload := visibilityManifestJSON{
		Seq:           m.Seq,
		UpdatedAtUnix: m.UpdatedAt.Unix(),
		Segments:      segments,
	}
	return json.Marshal(payload)
}

// UnmarshalJSON decodes the visibility manifest.
func (m *VisibilityManifest) UnmarshalJSON(data []byte) error {
	if m == nil {
		return nil
	}
	var payload visibilityManifestJSON
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	m.Seq = payload.Seq
	if payload.UpdatedAtUnix > 0 {
		m.UpdatedAt = time.Unix(payload.UpdatedAtUnix, 0).UTC()
	}
	m.Segments = nil
	for _, seg := range payload.Segments {
		created := time.Unix(seg.CreatedAtUnix, 0).UTC()
		m.Segments = append(m.Segments, VisibilitySegmentRef{
			ID:        seg.ID,
			CreatedAt: created,
			Entries:   seg.EntryCount,
		})
	}
	return nil
}

// MarshalJSON encodes the visibility segment.
func (s *VisibilitySegment) MarshalJSON() ([]byte, error) {
	if s == nil {
		return nil, nil
	}
	entries := make([]visibilityEntryJSON, 0, len(s.Entries))
	for _, entry := range s.Entries {
		entries = append(entries, visibilityEntryJSON(entry))
	}
	payload := visibilitySegmentJSON{
		ID:            s.ID,
		CreatedAtUnix: s.CreatedAt.Unix(),
		Entries:       entries,
	}
	return json.Marshal(payload)
}

// UnmarshalJSON decodes the visibility segment.
func (s *VisibilitySegment) UnmarshalJSON(data []byte) error {
	if s == nil {
		return nil
	}
	var payload visibilitySegmentJSON
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	s.ID = payload.ID
	if payload.CreatedAtUnix > 0 {
		s.CreatedAt = time.Unix(payload.CreatedAtUnix, 0).UTC()
	}
	s.Entries = nil
	for _, entry := range payload.Entries {
		s.Entries = append(s.Entries, VisibilityEntry(entry))
	}
	sort.Slice(s.Entries, func(i, j int) bool { return s.Entries[i].Key < s.Entries[j].Key })
	return nil
}

type visibilityManifestJSON struct {
	Seq           uint64                     `json:"seq"`
	UpdatedAtUnix int64                      `json:"updated_at_unix,omitempty"`
	Segments      []visibilitySegmentRefJSON `json:"segments,omitempty"`
}

type visibilitySegmentRefJSON struct {
	ID            string `json:"id"`
	CreatedAtUnix int64  `json:"created_at_unix"`
	EntryCount    uint64 `json:"entries"`
}

type visibilitySegmentJSON struct {
	ID            string                `json:"id"`
	CreatedAtUnix int64                 `json:"created_at_unix"`
	Entries       []visibilityEntryJSON `json:"entries"`
}

type visibilityEntryJSON struct {
	Key     string `json:"key"`
	Visible bool   `json:"visible"`
}
