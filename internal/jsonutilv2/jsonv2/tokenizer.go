package jsonv2

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// Kind enumerates the token categories produced by Tokenizer.
type Kind int

const (
	// TokenEOF marks the end of the input stream.
	TokenEOF Kind = iota
	// TokenBeginObject represents '{'.
	TokenBeginObject
	// TokenEndObject represents '}'.
	TokenEndObject
	// TokenBeginArray represents '['.
	TokenBeginArray
	// TokenEndArray represents ']'.
	TokenEndArray
	// TokenString contains a JSON string literal.
	TokenString
	// TokenNumber contains a JSON number literal.
	TokenNumber
	// TokenLiteral covers true/false/null tokens.
	TokenLiteral
	// TokenColon represents ':'.
	TokenColon
	// TokenComma represents ','.
	TokenComma
)

// Token captures a JSON token and its raw bytes.
type Token struct {
	Kind Kind
	Raw  []byte
}

// Tokenizer incrementally decodes JSON tokens from an io.Reader. It borrows
// the tokenization logic from the Go 1.25 json v2 runtime while avoiding the
// goexperiment build flag.
type Tokenizer struct {
	rd     io.Reader
	max    int64
	offset int64

	buf []byte
	pos int

	eof bool
}

// NewTokenizer constructs a streaming tokenizer that enforces a maximum payload size.
func NewTokenizer(r io.Reader, maxBytes int64) *Tokenizer {
	return &Tokenizer{
		rd:  r,
		max: maxBytes,
		buf: make([]byte, 0, 4096),
	}
}

// Next advances to the next token, returning it alongside any error encountered.
func (t *Tokenizer) Next() (Token, error) {
	for {
		if err := t.ensureBuffered(); err != nil {
			if errors.Is(err, io.EOF) {
				if t.pos < len(t.buf) {
					// if data remains, continue processing; otherwise signal EOF
					t.eof = true
				} else {
					return Token{Kind: TokenEOF}, io.EOF
				}
			} else {
				return Token{Kind: TokenEOF}, err
			}
		}
		if t.pos >= len(t.buf) {
			if t.eof {
				return Token{Kind: TokenEOF}, io.EOF
			}
			continue
		}

		if n := consumeWhitespace(t.buf[t.pos:]); n > 0 {
			t.pos += n
			continue
		}
		if t.pos >= len(t.buf) {
			continue
		}

		switch b := t.buf[t.pos]; b {
		case '{':
			start := t.pos
			t.pos++
			return Token{Kind: TokenBeginObject, Raw: t.buf[start:t.pos]}, nil
		case '}':
			start := t.pos
			t.pos++
			return Token{Kind: TokenEndObject, Raw: t.buf[start:t.pos]}, nil
		case '[':
			start := t.pos
			t.pos++
			return Token{Kind: TokenBeginArray, Raw: t.buf[start:t.pos]}, nil
		case ']':
			start := t.pos
			t.pos++
			return Token{Kind: TokenEndArray, Raw: t.buf[start:t.pos]}, nil
		case ':':
			start := t.pos
			t.pos++
			return Token{Kind: TokenColon, Raw: t.buf[start:t.pos]}, nil
		case ',':
			start := t.pos
			t.pos++
			return Token{Kind: TokenComma, Raw: t.buf[start:t.pos]}, nil
		case '"':
			return t.readString()
		case 't':
			return t.readLiteral("true")
		case 'f':
			return t.readLiteral("false")
		case 'n':
			return t.readLiteral("null")
		case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			return t.readNumber()
		default:
			return Token{Kind: TokenEOF}, newInvalidCharacterError(t.buf[t.pos:], "looking for beginning of value")
		}
	}
}

func (t *Tokenizer) readString() (Token, error) {
	start := t.pos
	var flags valueFlags
	resume := 0

	for {
		slice := t.buf[start:]
		n, err := consumeStringResumable(&flags, slice, resume, true)
		if err == nil {
			end := start + n
			t.pos = end
			return Token{Kind: TokenString, Raw: t.buf[start:end]}, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			resume = n
			if fetchErr := t.fetch(); fetchErr != nil {
				if errors.Is(fetchErr, io.EOF) {
					return Token{Kind: TokenEOF}, errInvalidEOF
				}
				return Token{Kind: TokenEOF}, fetchErr
			}
			start = t.pos
			continue
		}
		return Token{Kind: TokenEOF}, err
	}
}

func (t *Tokenizer) readLiteral(lit string) (Token, error) {
	start := t.pos
	for {
		n, err := consumeLiteral(t.buf[start:], lit)
		if err == nil {
			end := start + n
			if t.needMore(end) {
				if fetchErr := t.fetch(); fetchErr != nil {
					if errors.Is(fetchErr, io.EOF) {
						t.pos = end
						return Token{Kind: TokenLiteral, Raw: t.buf[start:end]}, nil
					}
					return Token{Kind: TokenEOF}, fetchErr
				}
				start = t.pos
				continue
			}
			if end < len(t.buf) {
				if err := validateLiteralTerminator(t.buf[end]); err != nil {
					return Token{Kind: TokenEOF}, err
				}
			}
			t.pos = end
			return Token{Kind: TokenLiteral, Raw: t.buf[start:end]}, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			if fetchErr := t.fetch(); fetchErr != nil {
				if errors.Is(fetchErr, io.EOF) {
					return Token{Kind: TokenEOF}, errInvalidEOF
				}
				return Token{Kind: TokenEOF}, fetchErr
			}
			start = t.pos
			continue
		}
		return Token{Kind: TokenEOF}, err
	}
}

func (t *Tokenizer) readNumber() (Token, error) {
	start := t.pos
	resume := 0
	state := consumeNumberInit

	for {
		slice := t.buf[start:]
		n, newState, err := consumeNumberResumable(slice, resume, state)
		state = newState
		end := start + n
		switch {
		case err == nil && !t.needMore(end):
			if end < len(t.buf) {
				if valErr := validateNumberTerminator(t.buf[end]); valErr != nil {
					return Token{Kind: TokenEOF}, valErr
				}
			}
			t.pos = end
			return Token{Kind: TokenNumber, Raw: t.buf[start:end]}, nil
		case err == nil && t.needMore(end):
			resume = n
			if fetchErr := t.fetch(); fetchErr != nil {
				if errors.Is(fetchErr, io.EOF) {
					t.pos = end
					return Token{Kind: TokenNumber, Raw: t.buf[start:end]}, nil
				}
				return Token{Kind: TokenEOF}, fetchErr
			}
			start = t.pos
			continue
		case errors.Is(err, io.ErrUnexpectedEOF):
			resume = n
			if fetchErr := t.fetch(); fetchErr != nil {
				if errors.Is(fetchErr, io.EOF) {
					return Token{Kind: TokenEOF}, errInvalidEOF
				}
				return Token{Kind: TokenEOF}, fetchErr
			}
			start = t.pos
			continue
		default:
			return Token{Kind: TokenEOF}, err
		}
	}
}

func (t *Tokenizer) ensureBuffered() error {
	if t.pos < len(t.buf) {
		return nil
	}
	if t.eof {
		return io.EOF
	}
	return t.fetch()
}

func (t *Tokenizer) fetch() error {
	if t.rd == nil {
		return io.EOF
	}

	if t.pos > 0 {
		t.offset += int64(t.pos)
		copy(t.buf, t.buf[t.pos:])
		t.buf = t.buf[:len(t.buf)-t.pos]
		t.pos = 0
	}

	if cap(t.buf)-len(t.buf) < 1024 {
		newCap := cap(t.buf) * 2
		if newCap == 0 {
			newCap = 4096
		}
		newBuf := make([]byte, len(t.buf), newCap)
		copy(newBuf, t.buf)
		t.buf = newBuf
	}

	for {
		n, err := t.rd.Read(t.buf[len(t.buf):cap(t.buf)])
		if n > 0 {
			t.buf = t.buf[:len(t.buf)+n]
			if t.max > 0 && t.offset+int64(len(t.buf)) > t.max {
				return fmt.Errorf("json: payload exceeds %d bytes", t.max)
			}
			if err == io.EOF {
				t.eof = true
				return nil
			}
			return err
		}
		if err != nil {
			if err == io.EOF {
				t.eof = true
			}
			return err
		}
	}
}

func (t *Tokenizer) needMore(pos int) bool {
	return pos >= len(t.buf) && !t.eof
}

// Remaining returns unread data for tests.
func (t *Tokenizer) Remaining() []byte {
	return bytes.Clone(t.buf[t.pos:])
}
