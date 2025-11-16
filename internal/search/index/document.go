package index

// Document captures normalized postings for a single key.
type Document struct {
	Key    string
	Fields map[string][]string // field -> terms
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
