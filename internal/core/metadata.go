package core

import "pkt.systems/lockd/internal/storage"

// MetadataMutation mirrors the HTTP-layer mutation but is transport-neutral.
type MetadataMutation struct {
	QueryHidden *bool
}

func (m MetadataMutation) empty() bool {
	return m.QueryHidden == nil
}

func (m MetadataMutation) apply(meta *storage.Meta) {
	if meta == nil {
		return
	}
	if m.QueryHidden != nil {
		meta.SetQueryHidden(*m.QueryHidden)
	}
}
