package httpapi

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestPayloadSpoolInMemory(t *testing.T) {
	spool := newPayloadSpool(1 << 20) // 1 MiB threshold
	data := []byte("hello world")
	if _, err := spool.Write(data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if spool.file != nil {
		t.Fatalf("expected in-memory buffer, found spill file %q", spool.file.Name())
	}
	reader, err := spool.Reader()
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("mismatch: got %q, want %q", got, data)
	}
	pos, err := reader.Seek(6, io.SeekStart)
	if err != nil || pos != 6 {
		t.Fatalf("seek start: pos=%d err=%v", pos, err)
	}
	buf := make([]byte, 5)
	if _, err := reader.Read(buf); err != nil {
		t.Fatalf("second read: %v", err)
	}
	if string(buf) != "world" {
		t.Fatalf("seek read mismatch: %q", buf)
	}
	if err := spool.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestPayloadSpoolSpillToDisk(t *testing.T) {
	spool := newPayloadSpool(8) // very small threshold
	chunks := []string{"0123", "4567", "89ab", "cdef"}
	for _, chunk := range chunks {
		if _, err := spool.Write([]byte(chunk)); err != nil {
			t.Fatalf("write chunk: %v", err)
		}
	}
	if spool.file == nil {
		t.Fatal("expected spill to disk, file is nil")
	}
	fileName := spool.file.Name()
	if _, err := os.Stat(fileName); err != nil {
		t.Fatalf("spill file missing: %v", err)
	}
	reader, err := spool.Reader()
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	full, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if string(full) != "0123456789abcdef" {
		t.Fatalf("unexpected content: %q", full)
	}
	if _, err := reader.Seek(4, io.SeekStart); err != nil {
		t.Fatalf("seek start: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := reader.Read(buf); err != nil {
		t.Fatalf("read after seek: %v", err)
	}
	if string(buf) != "4567" {
		t.Fatalf("seeked read mismatch: %q", buf)
	}

	if err := spool.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := os.Stat(fileName); !os.IsNotExist(err) {
		t.Fatalf("spill file should be removed, stat err=%v", err)
	}
}

func TestPayloadSpoolMultipleWritesAfterSpill(t *testing.T) {
	spool := newPayloadSpool(5)
	if _, err := spool.Write([]byte("hello")); err != nil {
		t.Fatalf("write1: %v", err)
	}
	if _, err := spool.Write([]byte(" ")); err != nil {
		t.Fatalf("write2: %v", err)
	}
	if _, err := spool.Write([]byte("world!")); err != nil {
		t.Fatalf("write3: %v", err)
	}
	reader, err := spool.Reader()
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "hello world!" {
		t.Fatalf("unexpected payload: %q", got)
	}
	if err := spool.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestPayloadSpoolReaderBeforeWrite(t *testing.T) {
	spool := newPayloadSpool(16)
	reader, err := spool.Reader()
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("expected empty buffer, got %q", data)
	}
	if err := spool.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
