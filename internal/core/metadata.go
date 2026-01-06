package core

import "pkt.systems/lockd/internal/storage"

// MetadataMutation mirrors the HTTP-layer mutation but is transport-neutral.
type MetadataMutation struct {
	QueryHidden *bool
}

func (m MetadataMutation) empty() bool {
	return m.QueryHidden == nil
}

// apply mutates metadata either immediately or in the staged attribute set.
func (m MetadataMutation) apply(meta *storage.Meta, staged bool) {
	if meta == nil {
		return
	}
	if m.QueryHidden != nil {
		if staged {
			meta.SetStagedQueryHidden(m.QueryHidden)
		} else {
			meta.SetQueryHidden(*m.QueryHidden)
		}
	}
}
