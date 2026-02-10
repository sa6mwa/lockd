package api

import "pkt.systems/lql"

// QueryRequest expresses a selector-based search within a namespace.
type QueryRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Selector is the LQL selector AST used to match keys/documents.
	Selector lql.Selector `json:"selector,omitempty" swaggertype:"object" extensions:"x-example={\"eq\":{\"field\":\"type\",\"value\":\"alpha\"}}"`
	// Limit caps the number of query results returned.
	Limit int `json:"limit,omitempty"`
	// Cursor is the pagination cursor returned by the query engine.
	Cursor string `json:"cursor,omitempty"`
	// Fields maps to the "fields" JSON field.
	Fields map[string]any `json:"fields,omitempty"`
	// Return controls whether query responses return keys or full documents.
	Return QueryReturn `json:"return,omitempty"`
}

// QueryResponse returns matching keys plus cursor metadata.
type QueryResponse struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Keys contains matching key identifiers for key-mode query responses.
	Keys []string `json:"keys,omitempty"`
	// Cursor is the pagination cursor returned by the query engine.
	Cursor string `json:"cursor,omitempty"`
	// IndexSeq is the index sequence observed by the query execution.
	IndexSeq uint64 `json:"index_seq,omitempty"`
	// Metadata carries metadata values returned by the server for this object.
	Metadata map[string]string `json:"metadata,omitempty"`
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
