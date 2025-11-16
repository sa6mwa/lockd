package index

import (
	"fmt"
	"sort"
	"time"

	indexproto "pkt.systems/lockd/internal/proto"
)

// Manifest captures the shard -> segment mapping for a namespace.
type Manifest struct {
	Seq       uint64
	UpdatedAt time.Time
	Shards    map[uint32]*Shard
}

// Shard holds the ordered list of segments covering a shard.
type Shard struct {
	ID       uint32
	Segments []SegmentRef
}

// SegmentRef references an immutable segment.
type SegmentRef struct {
	ID        string
	CreatedAt time.Time
	DocCount  uint64
}

// NewManifest returns an initial manifest.
func NewManifest() *Manifest {
	return &Manifest{Shards: make(map[uint32]*Shard)}
}

// Clone creates a deep copy of the manifest.
func (m *Manifest) Clone() *Manifest {
	if m == nil {
		return nil
	}
	out := &Manifest{
		Seq:       m.Seq,
		UpdatedAt: m.UpdatedAt,
		Shards:    make(map[uint32]*Shard, len(m.Shards)),
	}
	for id, shard := range m.Shards {
		if shard == nil {
			continue
		}
		copyShard := &Shard{ID: shard.ID}
		copyShard.Segments = append(copyShard.Segments, shard.Segments...)
		out.Shards[id] = copyShard
	}
	return out
}

// Validate ensures the manifest is structurally sound.
func (m *Manifest) Validate() error {
	if m == nil {
		return fmt.Errorf("manifest nil")
	}
	for id, shard := range m.Shards {
		if shard == nil {
			return fmt.Errorf("shard %d missing", id)
		}
		if shard.ID != id {
			return fmt.Errorf("shard id mismatch: map key %d != shard.ID %d", id, shard.ID)
		}
		for _, seg := range shard.Segments {
			if seg.ID == "" {
				return fmt.Errorf("shard %d contains segment with empty id", id)
			}
		}
	}
	return nil
}

// ToProto converts the manifest to its protobuf representation.
func (m *Manifest) ToProto() *indexproto.IndexManifest {
	if m == nil {
		return nil
	}
	msg := &indexproto.IndexManifest{
		Seq:           m.Seq,
		UpdatedAtUnix: m.UpdatedAt.Unix(),
		Shards:        make([]*indexproto.IndexShard, 0, len(m.Shards)),
	}
	ids := make([]int, 0, len(m.Shards))
	for id := range m.Shards {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	for _, rawID := range ids {
		id := uint32(rawID)
		shard := m.Shards[id]
		if shard == nil {
			continue
		}
		protoShard := &indexproto.IndexShard{ShardId: id}
		for _, seg := range shard.Segments {
			protoShard.Segments = append(protoShard.Segments, &indexproto.IndexSegmentRef{
				SegmentId:     seg.ID,
				CreatedAtUnix: seg.CreatedAt.Unix(),
				DocCount:      seg.DocCount,
			})
		}
		msg.Shards = append(msg.Shards, protoShard)
	}
	return msg
}

// ManifestFromProto decodes a manifest from protobuf.
func ManifestFromProto(msg *indexproto.IndexManifest) *Manifest {
	if msg == nil {
		return NewManifest()
	}
	manifest := &Manifest{
		Seq:       msg.GetSeq(),
		UpdatedAt: time.Unix(msg.GetUpdatedAtUnix(), 0).UTC(),
		Shards:    make(map[uint32]*Shard, len(msg.GetShards())),
	}
	for _, shard := range msg.GetShards() {
		if shard == nil {
			continue
		}
		out := &Shard{ID: shard.GetShardId()}
		for _, ref := range shard.GetSegments() {
			if ref == nil {
				continue
			}
			out.Segments = append(out.Segments, SegmentRef{
				ID:        ref.GetSegmentId(),
				CreatedAt: time.Unix(ref.GetCreatedAtUnix(), 0).UTC(),
				DocCount:  ref.GetDocCount(),
			})
		}
		manifest.Shards[out.ID] = out
	}
	return manifest
}
