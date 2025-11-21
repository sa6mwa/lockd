package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/storage"
)

// queueBlockModeLabel converts block seconds into a readable label for logs/metrics.
func queueBlockModeLabel(blockSeconds int64) string {
	switch blockSeconds {
	case api.BlockNoWait:
		return "nowait"
	case 0:
		return "forever"
	default:
		if blockSeconds < 0 {
			return "custom"
		}
		return fmt.Sprintf("%ds", blockSeconds)
	}
}

type metadataMutation struct {
	QueryHidden *bool `json:"query_hidden,omitempty"`
}

func (m metadataMutation) empty() bool {
	return m.QueryHidden == nil
}

func (m *metadataMutation) merge(other metadataMutation) {
	if other.QueryHidden != nil {
		m.QueryHidden = other.QueryHidden
	}
}

func metadataAttributesFromMeta(meta *storage.Meta) api.MetadataAttributes {
	attr := api.MetadataAttributes{}
	if meta == nil {
		return attr
	}
	if meta.QueryExcluded() {
		val := true
		attr.QueryHidden = &val
	}
	return attr
}

func parseMetadataHeaders(r *http.Request) (metadataMutation, error) {
	value := strings.TrimSpace(r.Header.Get(headerMetadataQueryHidden))
	if value == "" {
		return metadataMutation{}, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return metadataMutation{}, fmt.Errorf("invalid %s header: %w", headerMetadataQueryHidden, err)
	}
	return metadataMutation{QueryHidden: &parsed}, nil
}

func decodeMetadataMutation(body io.Reader) (metadataMutation, error) {
	if body == nil {
		return metadataMutation{}, nil
	}
	limited := io.LimitReader(body, 4<<10)
	dec := json.NewDecoder(limited)
	dec.DisallowUnknownFields()
	var mut metadataMutation
	if err := dec.Decode(&mut); err != nil {
		if errors.Is(err, io.EOF) {
			return metadataMutation{}, nil
		}
		return metadataMutation{}, err
	}
	return mut, nil
}
