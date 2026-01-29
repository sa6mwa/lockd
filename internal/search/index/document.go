package index

// Document captures normalized postings for a single key.
type Document struct {
	Key    string
	Fields map[string][]string // field -> terms
	Meta   *DocumentMetadata
}

// DocumentMetadata captures per-document metadata stored alongside index entries.
type DocumentMetadata struct {
	StateETag           string
	StatePlaintextBytes int64
	StateDescriptor     []byte
	PublishedVersion    int64
	QueryExcluded       bool
}

// AddTerm appends a term to the document field.
func (d *Document) AddTerm(field, term string) {
	if term == "" {
		return
	}
	if field == "" {
		field = "_"
	}
	if d.Fields == nil {
		d.Fields = make(map[string][]string)
	}
	d.Fields[field] = append(d.Fields[field], term)
}
