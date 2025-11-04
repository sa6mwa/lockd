package testlog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"pkt.systems/pslog"
)

// Entry represents a structured log entry captured from the test logger.
type Entry struct {
	Timestamp time.Time
	Level     string
	Message   string
	Fields    map[string]any
	Raw       string
}

// Recorder collects structured log entries emitted through the returned logger.
type Recorder struct {
	mu      sync.Mutex
	entries []Entry
}

// NewRecorder returns a logger that records every structured log line and mirrors
// it to testing.TB (when non-nil). The recorder is safe for concurrent use.
func NewRecorder(t testing.TB, level pslog.Level) (pslog.Logger, *Recorder) {
	rec := &Recorder{}
	writer := &recordingWriter{t: t, recorder: rec}
	logger := pslog.NewStructured(writer)
	if level != pslog.NoLevel {
		logger = logger.LogLevel(level)
	}
	return logger, rec
}

// Events returns a copy of all recorded entries.
func (r *Recorder) Events() []Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Entry, len(r.entries))
	copy(out, r.entries)
	return out
}

// First returns the first entry that matches pred.
func (r *Recorder) First(pred func(Entry) bool) (Entry, bool) {
	for _, entry := range r.Events() {
		if pred(entry) {
			return entry, true
		}
	}
	return Entry{}, false
}

// FirstAfter returns the first entry at or after ts that matches pred.
func (r *Recorder) FirstAfter(ts time.Time, pred func(Entry) bool) (Entry, bool) {
	for _, entry := range r.Events() {
		if entry.Timestamp.Before(ts) {
			continue
		}
		if pred(entry) {
			return entry, true
		}
	}
	return Entry{}, false
}

// Summary returns a human-readable view of the most recent entries.
func (r *Recorder) Summary() string {
	entries := r.Events()
	if len(entries) == 0 {
		return "<no log entries recorded>"
	}
	const max = 15
	start := 0
	if len(entries) > max {
		start = len(entries) - max
	}
	var b strings.Builder
	fmt.Fprintf(&b, "last %d/%d log entries:\n", len(entries)-start, len(entries))
	for _, entry := range entries[start:] {
		fmt.Fprintf(&b, "%s [%s] %s %v\n", entry.Timestamp.Format(time.RFC3339Nano), entry.Level, entry.Message, entry.Fields)
	}
	return b.String()
}

func (r *Recorder) add(entry Entry) {
	r.mu.Lock()
	r.entries = append(r.entries, entry)
	r.mu.Unlock()
}

type recordingWriter struct {
	t        testing.TB
	recorder *Recorder
}

func (w *recordingWriter) Write(p []byte) (int, error) {
	lines := bytes.SplitSeq(p, []byte{'\n'})
	for line := range lines {
		if len(line) == 0 {
			continue
		}
		entry := parseLogEntry(line)
		w.recorder.add(entry)
		if w.t != nil {
			w.t.Helper()
			w.t.Log(string(line))
		}
	}
	return len(p), nil
}

func parseLogEntry(line []byte) Entry {
	var payload map[string]any
	if err := json.Unmarshal(line, &payload); err != nil {
		return Entry{
			Timestamp: time.Now(),
			Level:     "unknown",
			Message:   "unparsed",
			Fields: map[string]any{
				"error": err.Error(),
			},
			Raw: string(line),
		}
	}
	tsStr, _ := payload["ts"].(string)
	lvl, _ := payload["lvl"].(string)
	msg, _ := payload["msg"].(string)
	timestamp, err := time.Parse(time.RFC3339Nano, tsStr)
	if err != nil {
		timestamp = time.Now()
	}
	fields := make(map[string]any, len(payload))
	for k, v := range payload {
		if k == "ts" || k == "lvl" || k == "msg" {
			continue
		}
		fields[k] = v
	}
	return Entry{
		Timestamp: timestamp,
		Level:     lvl,
		Message:   msg,
		Fields:    fields,
		Raw:       string(line),
	}
}

// GetStringField returns the string value for key if present.
func GetStringField(entry Entry, key string) string {
	if value, ok := entry.Fields[key]; ok {
		if str, ok := value.(string); ok {
			return str
		}
	}
	return ""
}

// GetIntField returns the integer value for key if present.
func GetIntField(entry Entry, key string) (int, bool) {
	if value, ok := entry.Fields[key]; ok {
		switch v := value.(type) {
		case float64:
			return int(v), true
		case float32:
			return int(v), true
		case int:
			return v, true
		case int64:
			return int(v), true
		case json.Number:
			if parsed, err := v.Int64(); err == nil {
				return int(parsed), true
			}
		}
	}
	return 0, false
}
