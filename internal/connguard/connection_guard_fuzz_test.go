package connguard

import (
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"pkt.systems/pslog"
)

func FuzzConnectionGuardPortRotationStillBlocks(f *testing.F) {
	f.Add("127.0.0.1", uint16(20001), uint16(20002))
	f.Add("10.0.0.5", uint16(443), uint16(65000))
	f.Add("192.168.1.9", uint16(1), uint16(65535))

	f.Fuzz(func(t *testing.T, host string, portA, portB uint16) {
		host = sanitizeFuzzHost(host)
		now := time.Now()
		guard := NewConnectionGuard(ConnectionGuardConfig{
			Enabled:          true,
			FailureThreshold: 3,
			FailureWindow:    time.Second,
			BlockDuration:    250 * time.Millisecond,
			ProbeTimeout:     25 * time.Millisecond,
		}, pslog.NoopLogger())
		guard.now = func() time.Time { return now }

		remoteA := fmt.Sprintf("%s:%d", host, portA)
		remoteB := fmt.Sprintf("%s:%d", host, portB)
		if guard.classifyFailure(remoteA, "zero_connect") {
			t.Fatalf("first failure must not block remote=%q", remoteA)
		}
		now = now.Add(5 * time.Millisecond)
		if guard.classifyFailure(remoteA, "tls_handshake") {
			t.Fatalf("second failure must not block remote=%q", remoteA)
		}
		now = now.Add(5 * time.Millisecond)
		if !guard.classifyFailure(remoteB, "zero_connect") {
			t.Fatalf("third failure from same host should block despite port rotation remoteA=%q remoteB=%q", remoteA, remoteB)
		}
		if !guard.isBlocked(remoteA) {
			t.Fatalf("expected remoteA blocked after threshold")
		}
		if !guard.isBlocked(remoteB) {
			t.Fatalf("expected remoteB blocked after threshold")
		}
	})
}

func FuzzConnectionGuardExpiryAndIsolation(f *testing.F) {
	f.Add("127.0.0.1:1111", "127.0.0.2:2222", uint16(20))
	f.Add(" 127.0.0.1:1 ", "127.0.0.1:2", uint16(300))
	f.Add("bad", "also-bad", uint16(60))

	f.Fuzz(func(t *testing.T, remoteA, remoteB string, advanceMS uint16) {
		now := time.Now()
		guard := NewConnectionGuard(ConnectionGuardConfig{
			Enabled:          true,
			FailureThreshold: 2,
			FailureWindow:    100 * time.Millisecond,
			BlockDuration:    50 * time.Millisecond,
			ProbeTimeout:     10 * time.Millisecond,
		}, pslog.NoopLogger())
		guard.now = func() time.Time { return now }

		_ = guard.classifyFailure(remoteA, "zero_connect")
		_ = guard.classifyFailure(remoteA, "tls_handshake")

		normA := normalizeRemoteAddr(remoteA)
		normB := normalizeRemoteAddr(remoteB)
		if normA != "" && normB != "" && normA != normB && guard.isBlocked(remoteB) {
			t.Fatalf("distinct remote unexpectedly blocked remoteA=%q remoteB=%q", remoteA, remoteB)
		}

		now = now.Add(time.Duration(advanceMS+100) * time.Millisecond)
		if guard.isBlocked(remoteA) {
			t.Fatalf("expected block expiry remote=%q", remoteA)
		}
		if guard.classifyFailure("", "zero_connect") {
			t.Fatalf("empty remote must never block")
		}
	})
}

func FuzzPrefixedConnReadConsistency(f *testing.F) {
	f.Add([]byte("a"), []byte("b"), uint8(0), uint8(0), uint8(0))
	f.Add([]byte(""), []byte("tail"), uint8(1), uint8(1), uint8(1))
	f.Add([]byte("prefix"), []byte(""), uint8(2), uint8(2), uint8(2))

	f.Fuzz(func(t *testing.T, rawPrefix, rawTail []byte, prefixSizeSel, tailSizeSel, ioSizeSel uint8) {
		prefix := cgFuzzResizedBytes(rawPrefix, cgFuzzBoundaryInt(prefixSizeSel, []int{
			0, 1, 2, 3, 7, 15, 31, 32, 63, 64, 127, 128, 255,
		}))
		tail := cgFuzzResizedBytes(rawTail, cgFuzzBoundaryInt(tailSizeSel, []int{
			0, 1, 2, 3, 7, 15, 31, 32, 63, 64, 127, 128, 255, 256, 257, 1024,
		}))
		connChunk := cgFuzzBoundaryInt(ioSizeSel, []int{1, 2, 3, 7, 16, 31, 32, 63, 64})
		readChunk := cgFuzzBoundaryInt(ioSizeSel+1, []int{1, 2, 3, 7, 16, 31, 32, 63, 64, 127})

		pc := &prefixedConn{
			Conn:   &fuzzConn{data: tail, chunk: connChunk},
			prefix: prefix,
		}

		buf := make([]byte, readChunk)
		got := make([]byte, 0, len(prefix)+len(tail))
		for {
			n, err := pc.Read(buf)
			if n > 0 {
				got = append(got, buf[:n]...)
			}
			if err == nil {
				continue
			}
			if err == io.EOF {
				break
			}
			t.Fatalf("unexpected read error: %v", err)
		}

		want := append(append([]byte{}, prefix...), tail...)
		if string(got) != string(want) {
			t.Fatalf("prefixed read mismatch\ngot=%q\nwant=%q", got, want)
		}
	})
}

func sanitizeFuzzHost(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return "127.0.0.1"
	}
	var b strings.Builder
	for i := 0; i < len(host); i++ {
		ch := host[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '.' || ch == '-' {
			b.WriteByte(ch)
		}
	}
	clean := strings.Trim(b.String(), ".-")
	if clean == "" || strings.Contains(clean, "..") {
		return "127.0.0.1"
	}
	return clean
}

func cgFuzzBoundaryInt(sel uint8, values []int) int {
	if len(values) == 0 {
		return 0
	}
	return values[int(sel)%len(values)]
}

func cgFuzzResizedBytes(in []byte, target int) []byte {
	if target <= 0 {
		return []byte{}
	}
	if len(in) == 0 {
		in = []byte("x")
	}
	out := make([]byte, target)
	for i := 0; i < target; i++ {
		out[i] = in[i%len(in)]
	}
	return out
}

type fuzzConn struct {
	data  []byte
	chunk int
	off   int
}

func (c *fuzzConn) Read(p []byte) (int, error) {
	if c.off >= len(c.data) {
		return 0, io.EOF
	}
	n := len(p)
	if c.chunk > 0 && n > c.chunk {
		n = c.chunk
	}
	remaining := len(c.data) - c.off
	if n > remaining {
		n = remaining
	}
	copy(p[:n], c.data[c.off:c.off+n])
	c.off += n
	return n, nil
}

func (c *fuzzConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *fuzzConn) Close() error                     { return nil }
func (c *fuzzConn) LocalAddr() net.Addr              { return fuzzAddr("local") }
func (c *fuzzConn) RemoteAddr() net.Addr             { return fuzzAddr("remote") }
func (c *fuzzConn) SetDeadline(time.Time) error      { return nil }
func (c *fuzzConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fuzzConn) SetWriteDeadline(time.Time) error { return nil }

type fuzzAddr string

func (a fuzzAddr) Network() string { return "tcp" }
func (a fuzzAddr) String() string  { return string(a) }
