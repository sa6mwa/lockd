package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"pkt.systems/lockd/lql"
)

// Document models a lockd state document with helper methods for JSON-pointer
// mutations and streaming interop.
type Document struct {
	Namespace string
	Key       string
	Version   string
	ETag      string
	Metadata  map[string]string

	Body map[string]any
}

// NewDocument initialises a mutable document for namespace/key.
func NewDocument(namespace, key string) *Document {
	return &Document{Namespace: namespace, Key: key, Body: make(map[string]any)}
}

func (d *Document) ensureDecoded() error {
	if d == nil {
		return fmt.Errorf("lockd: document nil")
	}
	if d.Body == nil {
		d.Body = make(map[string]any)
	}
	return nil
}

// Mutate applies LQL mutations using the current time.
func (d *Document) Mutate(exprs ...string) error {
	if err := d.ensureDecoded(); err != nil {
		return err
	}
	if err := lql.Mutate(d.Body, exprs...); err != nil {
		return err
	}
	return nil
}

// MutateWithTime applies LQL mutations using the supplied timestamp.
func (d *Document) MutateWithTime(now time.Time, exprs ...string) error {
	if err := d.ensureDecoded(); err != nil {
		return err
	}
	if err := lql.MutateWithTime(d.Body, now, exprs...); err != nil {
		return err
	}
	return nil
}

// Bytes returns the compact JSON representation of the document body.
func (d *Document) Bytes() ([]byte, error) {
	if d == nil {
		return nil, fmt.Errorf("lockd: document nil")
	}
	if err := d.ensureDecoded(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(d.Body)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Reader returns a fresh reader over the document JSON.
func (d *Document) Reader() (io.Reader, error) {
	data, err := d.Bytes()
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(data), nil
}

// LoadFrom replaces the document contents with data streamed from r.
func (d *Document) LoadFrom(r io.Reader) error {
	if d == nil {
		return fmt.Errorf("lockd: document nil")
	}
	dec := json.NewDecoder(r)
	var body map[string]any
	if err := dec.Decode(&body); err != nil {
		return err
	}
	d.Body = body
	return nil
}

// Write implements io.Writer so documents can be used with io.Copy.
func (d *Document) Write(p []byte) (int, error) {
	if d == nil {
		return 0, fmt.Errorf("lockd: document nil")
	}
	if err := d.LoadFrom(bytes.NewReader(p)); err != nil {
		return 0, err
	}
	return len(p), nil
}

// LoadInto decodes the document body into target.
func (d *Document) LoadInto(target any) error {
	if d == nil {
		return fmt.Errorf("lockd: document nil")
	}
	if err := d.ensureDecoded(); err != nil {
		return err
	}
	buf, err := json.Marshal(d.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, target)
}
