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

type jsonDecodeOptions struct {
	allowEmpty       bool
	disallowUnknowns bool
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
	val, ok := meta.StagedAttributes[storage.MetaAttributeQueryExclude]
	if !ok && meta.Attributes != nil {
		val, ok = meta.Attributes[storage.MetaAttributeQueryExclude]
	}
	if ok {
		hidden := strings.EqualFold(val, "true") || strings.EqualFold(val, "1") || strings.EqualFold(val, "yes")
		if hidden {
			attr.QueryHidden = &hidden
		}
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
	var mut metadataMutation
	if err := decodeJSONBody(limited, &mut, jsonDecodeOptions{
		allowEmpty:       true,
		disallowUnknowns: true,
	}); err != nil {
		return metadataMutation{}, err
	}
	return mut, nil
}

func decodeJSONBody(body io.Reader, dst any, opts jsonDecodeOptions) error {
	if body == nil {
		if opts.allowEmpty {
			return nil
		}
		return io.EOF
	}
	dec := json.NewDecoder(body)
	if opts.disallowUnknowns {
		dec.DisallowUnknownFields()
	}
	if err := dec.Decode(dst); err != nil {
		if opts.allowEmpty && errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	return fmt.Errorf("unexpected trailing JSON value")
}
