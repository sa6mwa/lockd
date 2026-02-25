package index

import (
	"testing"
	"time"
)

func TestCloneManifestPreservesFormat(t *testing.T) {
	manifest := NewManifest()
	manifest.Format = IndexFormatVersionV5
	manifest.Seq = 12
	manifest.UpdatedAt = time.Unix(1_700_000_200, 0).UTC()
	manifest.Shards[0] = &Shard{
		ID: 0,
		Segments: []SegmentRef{
			{ID: "seg-a", CreatedAt: manifest.UpdatedAt, DocCount: 3},
		},
	}
	clone := cloneManifest(manifest)
	if clone == nil {
		t.Fatalf("expected clone")
	}
	if clone.Format != IndexFormatVersionV5 {
		t.Fatalf("expected format v5, got %d", clone.Format)
	}
	if clone.Seq != manifest.Seq {
		t.Fatalf("expected seq %d, got %d", manifest.Seq, clone.Seq)
	}
	if clone.Shards[0] == nil || len(clone.Shards[0].Segments) != 1 {
		t.Fatalf("expected shard segments in clone")
	}
}
