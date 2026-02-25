package index

import (
	"slices"
	"testing"
)

func TestAdaptivePostingRoundTripSparseDelta(t *testing.T) {
	src := docIDSet{3, 19, 82, 127, 901, 1337}
	posting := newAdaptivePosting(src)
	if posting.encoding != postingEncodingDeltaVarint {
		t.Fatalf("expected delta-varint encoding, got %v", posting.encoding)
	}
	got := posting.decodeInto(nil)
	if !slices.Equal(got, src) {
		t.Fatalf("decoded mismatch\ngot:  %v\nwant: %v", got, src)
	}
}

func TestAdaptivePostingRoundTripDenseBitset(t *testing.T) {
	src := make(docIDSet, 0, 256)
	for i := uint32(0); i < 256; i++ {
		src = append(src, i)
	}
	posting := newAdaptivePosting(src)
	if posting.encoding != postingEncodingBitset {
		t.Fatalf("expected bitset encoding, got %v", posting.encoding)
	}
	got := posting.decodeInto(nil)
	if !slices.Equal(got, src) {
		t.Fatalf("decoded mismatch\ngot:  %v\nwant: %v", got, src)
	}
}

func TestAdaptivePostingEncodingPolicy(t *testing.T) {
	sparse := docIDSet{1, 150, 720, 2401}
	sparsePosting := newAdaptivePosting(sparse)
	if sparsePosting.encoding != postingEncodingDeltaVarint {
		t.Fatalf("expected sparse posting to use delta-varint, got %v", sparsePosting.encoding)
	}

	dense := make(docIDSet, 0, 512)
	for i := uint32(0); i < 512; i++ {
		dense = append(dense, i)
	}
	densePosting := newAdaptivePosting(dense)
	if densePosting.encoding != postingEncodingBitset {
		t.Fatalf("expected dense posting to use bitset, got %v", densePosting.encoding)
	}
}
