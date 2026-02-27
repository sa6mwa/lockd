package client

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"sync"
	"unicode/utf16"
	"unicode/utf8"
)

type queryStream struct {
	body    io.ReadCloser
	reader  *bufio.Reader
	closed  bool
	onClose func()
}

var queryStreamReaderPool = sync.Pool{
	New: func() any {
		return &bufio.Reader{}
	},
}

func takeQueryStreamReader(body io.Reader) *bufio.Reader {
	pooled, _ := queryStreamReaderPool.Get().(*bufio.Reader)
	if pooled == nil {
		pooled = &bufio.Reader{}
	}
	pooled.Reset(body)
	return pooled
}

func putQueryStreamReader(reader *bufio.Reader) {
	if reader == nil {
		return
	}
	reader.Reset(nil)
	queryStreamReaderPool.Put(reader)
}

func newQueryStream(body io.ReadCloser, onClose ...func()) *queryStream {
	var closeFn func()
	if len(onClose) > 0 {
		closeFn = onClose[0]
	}
	return &queryStream{body: body, reader: takeQueryStreamReader(body), onClose: closeFn}
}

func (qs *queryStream) Close() error {
	if qs.closed {
		return nil
	}
	qs.closed = true
	if qs.onClose != nil {
		qs.onClose()
	}
	putQueryStreamReader(qs.reader)
	qs.reader = nil
	if qs.body != nil {
		return qs.body.Close()
	}
	return nil
}

func (qs *queryStream) Next() (*streamRow, error) {
	if qs.closed {
		return nil, io.EOF
	}
	if err := qs.skipWhitespace(); err != nil {
		if err == io.EOF {
			qs.Close()
			return nil, io.EOF
		}
		return nil, err
	}
	b, err := qs.reader.ReadByte()
	if err != nil {
		if err == io.EOF {
			qs.Close()
			return nil, io.EOF
		}
		return nil, err
	}
	if b != '{' {
		return nil, fmt.Errorf("lockd: expected '{' at row start, got %q", b)
	}
	row := &streamRow{}
	// ns
	name, err := qs.readFieldName()
	if err != nil {
		return nil, err
	}
	if name != "ns" {
		return nil, fmt.Errorf("lockd: expected field 'ns', got %q", name)
	}
	val, err := qs.readStringLiteral()
	if err != nil {
		return nil, err
	}
	row.Namespace = val
	if err := qs.expectComma(); err != nil {
		return nil, err
	}
	// key
	name, err = qs.readFieldName()
	if err != nil {
		return nil, err
	}
	if name != "key" {
		return nil, fmt.Errorf("lockd: expected field 'key', got %q", name)
	}
	val, err = qs.readStringLiteral()
	if err != nil {
		return nil, err
	}
	row.Key = val
	if err := qs.expectComma(); err != nil {
		return nil, err
	}
	// optional ver
	name, err = qs.readFieldName()
	if err != nil {
		return nil, err
	}
	if name == "ver" {
		num, err := qs.readNumberLiteral()
		if err != nil {
			return nil, err
		}
		row.Version = num
		if err := qs.expectComma(); err != nil {
			return nil, err
		}
		name, err = qs.readFieldName()
		if err != nil {
			return nil, err
		}
	}
	if name != "doc" {
		return nil, fmt.Errorf("lockd: expected field 'doc', got %q", name)
	}
	if err := qs.skipWhitespace(); err != nil {
		return nil, err
	}
	docReader, err := newJSONValueReader(qs.reader)
	if err != nil {
		return nil, err
	}
	row.doc = &streamDocumentHandle{stream: &documentStream{reader: docReader, parent: qs}}
	return row, nil
}

func (qs *queryStream) skipWhitespace() error {
	for {
		b, err := qs.reader.ReadByte()
		if err != nil {
			return err
		}
		if b == ' ' || b == '\t' || b == '\r' || b == '\n' {
			continue
		}
		return qs.reader.UnreadByte()
	}
}

func (qs *queryStream) readFieldName() (string, error) {
	if err := qs.skipWhitespace(); err != nil {
		return "", err
	}
	name, err := qs.readStringLiteral()
	if err != nil {
		return "", err
	}
	if err := qs.expectColon(); err != nil {
		return "", err
	}
	return name, nil
}

func (qs *queryStream) readStringLiteral() (string, error) {
	if err := qs.skipWhitespace(); err != nil {
		return "", err
	}
	return readJSONString(qs.reader)
}

func readJSONString(r *bufio.Reader) (string, error) {
	quote, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	if quote != '"' {
		return "", fmt.Errorf("lockd: expected string literal, got %q", quote)
	}

	var raw []byte
	for {
		fragment, readErr := r.ReadSlice('"')
		if readErr == nil {
			chunk := fragment[:len(fragment)-1]
			if len(raw) == 0 && bytes.IndexByte(chunk, '\\') < 0 {
				return string(chunk), nil
			}
			raw = append(raw, chunk...)
			if trailingBackslashes(chunk)%2 == 0 {
				return unescapeJSONString(raw)
			}
			// Escaped quote inside the string literal.
			raw = append(raw, '"')
			continue
		}
		if readErr == bufio.ErrBufferFull {
			raw = append(raw, fragment...)
			continue
		}
		return "", readErr
	}
}

func trailingBackslashes(chunk []byte) int {
	count := 0
	for i := len(chunk) - 1; i >= 0; i-- {
		if chunk[i] != '\\' {
			break
		}
		count++
	}
	return count
}

func unescapeJSONString(raw []byte) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	if bytes.IndexByte(raw, '\\') < 0 {
		return string(raw), nil
	}
	out := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if b != '\\' {
			out = append(out, b)
			continue
		}
		i++
		if i >= len(raw) {
			return "", io.ErrUnexpectedEOF
		}
		esc := raw[i]
		switch esc {
		case '"', '\\', '/':
			out = append(out, esc)
		case 'b':
			out = append(out, byte('\b'))
		case 'f':
			out = append(out, byte('\f'))
		case 'n':
			out = append(out, byte('\n'))
		case 'r':
			out = append(out, byte('\r'))
		case 't':
			out = append(out, byte('\t'))
		case 'u':
			if i+4 >= len(raw) {
				return "", io.ErrUnexpectedEOF
			}
			rn, err := decodeHexRune(raw[i+1 : i+5])
			if err != nil {
				return "", err
			}
			out = utf8.AppendRune(out, rn)
			i += 4
		default:
			return "", fmt.Errorf("lockd: invalid escape %q", esc)
		}
	}
	return string(out), nil
}

func decodeHexRune(b []byte) (rune, error) {
	var decoded [2]byte
	if _, err := hex.Decode(decoded[:], b); err != nil {
		return 0, err
	}
	r := rune(decoded[0])<<8 | rune(decoded[1])
	if utf16.IsSurrogate(r) {
		return utf16.DecodeRune(r, 0), nil
	}
	return r, nil
}

func (qs *queryStream) expectColon() error {
	if err := qs.skipWhitespace(); err != nil {
		return err
	}
	b, err := qs.reader.ReadByte()
	if err != nil {
		return err
	}
	if b != ':' {
		return fmt.Errorf("lockd: expected ':' got %q", b)
	}
	return nil
}

func (qs *queryStream) expectComma() error {
	if err := qs.skipWhitespace(); err != nil {
		return err
	}
	b, err := qs.reader.ReadByte()
	if err != nil {
		return err
	}
	if b != ',' {
		return fmt.Errorf("lockd: expected ',' got %q", b)
	}
	return nil
}

func (qs *queryStream) readNumberLiteral() (int64, error) {
	if err := qs.skipWhitespace(); err != nil {
		return 0, err
	}
	var buf []byte
	for {
		b, err := qs.reader.ReadByte()
		if err != nil {
			return 0, err
		}
		if (b >= '0' && b <= '9') || b == '-' {
			buf = append(buf, b)
			continue
		}
		qs.reader.UnreadByte()
		break
	}
	if len(buf) == 0 {
		return 0, fmt.Errorf("lockd: expected number literal")
	}
	return strconv.ParseInt(string(buf), 10, 64)
}

func (qs *queryStream) finishRow() error {
	if err := qs.skipWhitespace(); err != nil {
		if err == io.EOF {
			qs.Close()
			return nil
		}
		return err
	}
	b, err := qs.reader.ReadByte()
	if err != nil {
		return err
	}
	if b != '}' {
		return fmt.Errorf("lockd: expected row terminator, got %q", b)
	}
	// consume trailing whitespace/newline
	for {
		b, err = qs.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				qs.Close()
				return nil
			}
			return err
		}
		if b == '\n' {
			return nil
		}
		if b == ' ' || b == '\t' || b == '\r' {
			continue
		}
		qs.reader.UnreadByte()
		return nil
	}
}

type streamRow struct {
	Namespace string
	Key       string
	Version   int64
	doc       *streamDocumentHandle
}

type streamDocumentHandle struct {
	stream   *documentStream
	consumed bool
	closed   bool
}

type documentStream struct {
	reader   *jsonValueReader
	parent   *queryStream
	finished bool
}

func (h *streamDocumentHandle) acquireReader() (io.ReadCloser, error) {
	if h == nil {
		return nil, nil
	}
	if h.consumed {
		return nil, fmt.Errorf("lockd: document already consumed")
	}
	h.consumed = true
	return &documentCloser{handle: h}, nil
}

func (h *streamDocumentHandle) closeSilently() error {
	if h == nil || h.closed {
		return nil
	}
	h.consumed = true
	h.closed = true
	if h.stream == nil {
		return nil
	}
	return h.stream.Close()
}

type documentCloser struct {
	handle *streamDocumentHandle
}

func (dc *documentCloser) Read(p []byte) (int, error) {
	return dc.handle.stream.Read(p)
}

func (dc *documentCloser) Close() error {
	if dc.handle.closed {
		return nil
	}
	dc.handle.closed = true
	return dc.handle.stream.Close()
}

func (ds *documentStream) Read(p []byte) (int, error) {
	if ds.reader == nil {
		return 0, io.EOF
	}
	n, err := ds.reader.Read(p)
	if err == io.EOF {
		ds.reader = nil
		if err := ds.finish(); err != nil {
			return n, err
		}
		return n, io.EOF
	}
	return n, err
}

func (ds *documentStream) Close() error {
	if ds.reader == nil {
		return ds.finish()
	}
	buf := make([]byte, 1024)
	for {
		if _, err := ds.reader.Read(buf); err != nil {
			if err == io.EOF {
				ds.reader = nil
				break
			}
			return err
		}
	}
	return ds.finish()
}

func (ds *documentStream) finish() error {
	if ds.finished {
		return nil
	}
	ds.finished = true
	return ds.parent.finishRow()
}

type jsonValueReader struct {
	r        *bufio.Reader
	buf      []byte
	done     bool
	inString bool
	escape   bool
	depth    int
	simple   bool
}

func newJSONValueReader(r *bufio.Reader) (*jsonValueReader, error) {
	if err := skipWhitespace(r); err != nil {
		return nil, err
	}
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	reader := &jsonValueReader{r: r}
	reader.buf = append(reader.buf, b)
	switch b {
	case '{', '[':
		reader.depth = 1
	case '"':
		reader.inString = true
	default:
		reader.simple = true
	}
	return reader, nil
}

func (jr *jsonValueReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	n := 0
	if len(jr.buf) > 0 {
		copied := copy(p, jr.buf)
		n += copied
		jr.buf = jr.buf[copied:]
		if n == len(p) {
			return n, nil
		}
	}
	if jr.done {
		if n > 0 {
			return n, nil
		}
		return 0, io.EOF
	}
	for n < len(p) && !jr.done {
		b, err := jr.r.ReadByte()
		if err != nil {
			if err == io.EOF && n > 0 {
				return n, nil
			}
			return n, err
		}
		emit, err := jr.consumeByte(b)
		if err != nil {
			return n, err
		}
		if !emit {
			continue
		}
		p[n] = b
		n++
	}
	if n > 0 {
		return n, nil
	}
	if jr.done {
		return 0, io.EOF
	}
	return 0, nil
}

func (jr *jsonValueReader) consumeByte(b byte) (bool, error) {
	if jr.done {
		return false, nil
	}
	if jr.inString {
		if jr.escape {
			jr.escape = false
			return true, nil
		}
		if b == '\\' {
			jr.escape = true
			return true, nil
		}
		if b == '"' {
			jr.inString = false
			if jr.depth == 0 {
				jr.done = true
			}
		}
		return true, nil
	}
	if jr.simple {
		if isLiteralChar(b) {
			return true, nil
		}
		jr.done = true
		return false, jr.r.UnreadByte()
	}
	switch b {
	case '{', '[':
		jr.depth++
	case '}', ']':
		jr.depth--
		if jr.depth == 0 {
			jr.done = true
		}
	case '"':
		jr.inString = true
	}
	return true, nil
}

func (jr *jsonValueReader) Close() error {
	if jr.done {
		return nil
	}
	buf := make([]byte, 1024)
	for {
		if _, err := jr.Read(buf); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func isLiteralChar(b byte) bool {
	return (b >= '0' && b <= '9') || b == '-' || b == '+' || b == '.' || b == 'e' || b == 'E' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func skipWhitespace(r *bufio.Reader) error {
	for {
		b, err := r.ReadByte()
		if err != nil {
			return err
		}
		if b == ' ' || b == '\t' || b == '\r' || b == '\n' {
			continue
		}
		return r.UnreadByte()
	}
}
