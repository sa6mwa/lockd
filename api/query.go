package api

import "pkt.systems/lql"

// QueryRequest expresses a selector-based search within a namespace.
type QueryRequest struct {
	Namespace string         `json:"namespace,omitempty"`
	Selector  lql.Selector   `json:"selector,omitempty" swaggertype:"object" extensions:"x-example={\"eq\":{\"field\":\"type\",\"value\":\"alpha\"}}"`
	Limit     int            `json:"limit,omitempty"`
	Cursor    string         `json:"cursor,omitempty"`
	Fields    map[string]any `json:"fields,omitempty"`
	Return    QueryReturn    `json:"return,omitempty"`
}

// QueryResponse returns matching keys plus cursor metadata.
type QueryResponse struct {
	Namespace string            `json:"namespace,omitempty"`
	Keys      []string          `json:"keys,omitempty"`
	Cursor    string            `json:"cursor,omitempty"`
	IndexSeq  uint64            `json:"index_seq,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// QueryReturn enumerates the supported payload shapes for /v1/query responses.
type QueryReturn string

const (
	// QueryReturnKeys streams an object containing a key slice (default behaviour).
	QueryReturnKeys QueryReturn = "keys"
	// QueryReturnDocuments streams newline-delimited JSON rows containing full documents.
	QueryReturnDocuments QueryReturn = "documents"
)

// Selector is a type alias for the shared LQL selector AST.
type Selector = lql.Selector

// Term is a type alias for the shared LQL term.
type Term = lql.Term

// RangeTerm is a type alias for the shared LQL range term.
type RangeTerm = lql.RangeTerm

// InTerm is a type alias for the shared LQL in-term.
type InTerm = lql.InTerm
