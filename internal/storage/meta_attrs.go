package storage

import "strings"

const (
	// MetaAttributeQueryExclude marks a key as hidden from /v1/query results when set to a truthy value.
	MetaAttributeQueryExclude = "lockd.query.exclude"
)

func (m *Meta) ensureAttributes() {
	if m.Attributes == nil {
		m.Attributes = make(map[string]string)
	}
}

// SetAttribute assigns a string attribute on the metadata.
func (m *Meta) SetAttribute(key, value string) {
	if m == nil || key == "" {
		return
	}
	m.ensureAttributes()
	m.Attributes[key] = value
}

// ClearAttribute removes key from metadata attributes.
func (m *Meta) ClearAttribute(key string) {
	if m == nil || key == "" || m.Attributes == nil {
		return
	}
	delete(m.Attributes, key)
	if len(m.Attributes) == 0 {
		m.Attributes = nil
	}
}

// GetAttribute returns the attribute value and whether it exists.
func (m *Meta) GetAttribute(key string) (string, bool) {
	if m == nil || key == "" || m.Attributes == nil {
		return "", false
	}
	val, ok := m.Attributes[key]
	return val, ok
}

// MarkQueryExcluded flags the metadata so scan queries skip the key.
func (m *Meta) MarkQueryExcluded() {
	m.SetAttribute(MetaAttributeQueryExclude, "true")
}

// ClearQueryExcluded clears the hidden flag from metadata.
func (m *Meta) ClearQueryExcluded() {
	m.ClearAttribute(MetaAttributeQueryExclude)
}

// SetQueryHidden sets or clears the query-hidden flag.
func (m *Meta) SetQueryHidden(hidden bool) {
	if m == nil {
		return
	}
	if hidden {
		m.MarkQueryExcluded()
		return
	}
	// Persist the preference explicitly so future defaults know the user opted in.
	m.SetAttribute(MetaAttributeQueryExclude, "false")
}

// HasQueryHiddenPreference reports whether the metadata carries an explicit
// query-hidden attribute (true or false).
func (m *Meta) HasQueryHiddenPreference() bool {
	if m == nil || m.Attributes == nil {
		return false
	}
	_, ok := m.Attributes[MetaAttributeQueryExclude]
	return ok
}

// QueryExcluded reports whether this key should be hidden from queries.
func (m *Meta) QueryExcluded() bool {
	if m == nil {
		return false
	}
	val, ok := m.GetAttribute(MetaAttributeQueryExclude)
	if !ok {
		return false
	}
	switch strings.ToLower(val) {
	case "true", "1", "yes", "y", "on":
		return true
	default:
		return false
	}
}
