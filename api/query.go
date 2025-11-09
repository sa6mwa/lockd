package api

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// QueryRequest expresses a selector-based search within a namespace.
type QueryRequest struct {
	Namespace string         `json:"namespace,omitempty"`
	Selector  Selector       `json:"selector,omitempty"`
	Limit     int            `json:"limit,omitempty"`
	Cursor    string         `json:"cursor,omitempty"`
	Fields    map[string]any `json:"fields,omitempty"`
}

// QueryResponse returns matching keys plus cursor metadata.
type QueryResponse struct {
	Namespace string            `json:"namespace,omitempty"`
	Keys      []string          `json:"keys,omitempty"`
	Cursor    string            `json:"cursor,omitempty"`
	IndexSeq  uint64            `json:"index_seq,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// Selector represents the recursive selector AST.
type Selector struct {
	And    []Selector `json:"and,omitempty"`
	Or     []Selector `json:"or,omitempty"`
	Not    *Selector  `json:"not,omitempty"`
	Eq     *Term      `json:"eq,omitempty"`
	Prefix *Term      `json:"prefix,omitempty"`
	Range  *RangeTerm `json:"range,omitempty"`
	In     *InTerm    `json:"in,omitempty"`
	Exists string     `json:"exists,omitempty"`
}

// Term represents a simple field/value predicate.
type Term struct {
	Field string `json:"field"`
	Value string `json:"value"`
}

// UnmarshalJSON accepts string/bool/number for value and converts to string.
func (t *Term) UnmarshalJSON(data []byte) error {
	type alias struct {
		Field string      `json:"field"`
		Value interface{} `json:"value"`
	}
	var tmp alias
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	if tmp.Field == "" {
		return fmt.Errorf("term field required")
	}
	t.Field = tmp.Field
	switch v := tmp.Value.(type) {
	case string:
		t.Value = v
	case bool:
		if v {
			t.Value = "true"
		} else {
			t.Value = "false"
		}
	case float64:
		t.Value = strconv.FormatFloat(v, 'f', -1, 64)
	case nil:
		t.Value = ""
	default:
		t.Value = fmt.Sprint(v)
	}
	return nil
}

// RangeTerm captures numeric/timestamp bounds.
type RangeTerm struct {
	Field string   `json:"field"`
	GTE   *float64 `json:"gte,omitempty"`
	GT    *float64 `json:"gt,omitempty"`
	LTE   *float64 `json:"lte,omitempty"`
	LT    *float64 `json:"lt,omitempty"`
}

// InTerm represents a small set membership filter.
type InTerm struct {
	Field string   `json:"field"`
	Any   []string `json:"any"`
}

// UnmarshalJSON ensures empty selectors stay zeroed without nil pointers.
func (s *Selector) UnmarshalJSON(data []byte) error {
	type alias Selector
	var tmp alias
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	*s = Selector(tmp)
	return nil
}
