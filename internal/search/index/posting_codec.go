package index

import (
	"encoding/binary"
	"math/bits"
)

type postingEncoding uint8

const (
	postingEncodingDeltaVarint postingEncoding = iota + 1
	postingEncodingBitset
)

const postingBitsetDensityThreshold = 0.18

type adaptivePosting struct {
	encoding postingEncoding
	count    int
	maxDocID uint32
	sparse   []byte
	dense    []uint64
}

func newAdaptivePosting(docIDs docIDSet) adaptivePosting {
	if len(docIDs) == 0 {
		return adaptivePosting{}
	}
	docIDs = sortUniqueDocIDs(append(docIDSet(nil), docIDs...))
	if len(docIDs) == 0 {
		return adaptivePosting{}
	}
	maxDocID := docIDs[len(docIDs)-1]
	universe := int(maxDocID) + 1
	density := float64(len(docIDs)) / float64(universe)
	if shouldUseBitsetEncoding(docIDs, universe, density) {
		return adaptivePosting{
			encoding: postingEncodingBitset,
			count:    len(docIDs),
			maxDocID: maxDocID,
			dense:    encodeBitset(docIDs, maxDocID),
		}
	}
	return adaptivePosting{
		encoding: postingEncodingDeltaVarint,
		count:    len(docIDs),
		maxDocID: maxDocID,
		sparse:   encodeDeltaVarint(docIDs),
	}
}

func shouldUseBitsetEncoding(docIDs docIDSet, universe int, density float64) bool {
	if len(docIDs) == 0 || universe <= 0 {
		return false
	}
	if density < postingBitsetDensityThreshold {
		return false
	}
	bitsetBytes := ((universe + 63) / 64) * 8
	varintBytes := estimateDeltaVarintSize(docIDs)
	return bitsetBytes <= varintBytes
}

func estimateDeltaVarintSize(docIDs docIDSet) int {
	total := 0
	var prev uint32
	for i, id := range docIDs {
		delta := id
		if i > 0 {
			delta = id - prev
		}
		total += binary.MaxVarintLen32
		if delta < 1<<7 {
			total -= 4
		} else if delta < 1<<14 {
			total -= 3
		} else if delta < 1<<21 {
			total -= 2
		} else if delta < 1<<28 {
			total--
		}
		prev = id
	}
	return total
}

func encodeDeltaVarint(docIDs docIDSet) []byte {
	if len(docIDs) == 0 {
		return nil
	}
	out := make([]byte, 0, estimateDeltaVarintSize(docIDs))
	var prev uint32
	var tmp [binary.MaxVarintLen32]byte
	for i, id := range docIDs {
		delta := id
		if i > 0 {
			delta = id - prev
		}
		n := binary.PutUvarint(tmp[:], uint64(delta))
		out = append(out, tmp[:n]...)
		prev = id
	}
	return out
}

func decodeDeltaVarintInto(dst docIDSet, payload []byte) docIDSet {
	if len(payload) == 0 {
		return dst[:0]
	}
	dst = dst[:0]
	var cur uint32
	for len(payload) > 0 {
		delta, n := binary.Uvarint(payload)
		if n <= 0 {
			break
		}
		cur += uint32(delta)
		dst = append(dst, cur)
		payload = payload[n:]
	}
	return dst
}

func encodeBitset(docIDs docIDSet, maxDocID uint32) []uint64 {
	if len(docIDs) == 0 {
		return nil
	}
	words := int(maxDocID)/64 + 1
	out := make([]uint64, words)
	for _, id := range docIDs {
		word := id / 64
		bit := id % 64
		out[word] |= uint64(1) << bit
	}
	return out
}

func decodeBitsetInto(dst docIDSet, words []uint64) docIDSet {
	dst = dst[:0]
	for wordIdx, word := range words {
		base := uint32(wordIdx * 64)
		for word != 0 {
			tz := bits.TrailingZeros64(word)
			dst = append(dst, base+uint32(tz))
			word &= word - 1
		}
	}
	return dst
}

func (p adaptivePosting) decodeInto(dst docIDSet) docIDSet {
	switch p.encoding {
	case postingEncodingBitset:
		return decodeBitsetInto(dst, p.dense)
	case postingEncodingDeltaVarint:
		return decodeDeltaVarintInto(dst, p.sparse)
	default:
		return dst[:0]
	}
}
