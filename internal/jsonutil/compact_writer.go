package jsonutil

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

type containerState struct {
	typ            byte
	objPhase       objPhase
	objCount       int
	arrExpectValue bool
	arrNeedComma   bool
	arrCount       int
}

type objPhase int

const (
	objExpectKey objPhase = iota
	objExpectColon
	objExpectValue
	objExpectComma
)

// CompactWriter streams JSON from r to w, stripping insignificant whitespace.
// maxBytes limits the number of bytes read from r (<=0 disables the limit).
// Inspired by github.com/tdewolff/minify/v2/json (MIT license) with custom
// implementation tailored for lockd.
func CompactWriter(w io.Writer, r io.Reader, maxBytes int64) error {
	c := &compactor{
		br:  bufio.NewReader(r),
		bw:  bufio.NewWriter(w),
		max: maxBytes,
	}
	if err := c.run(); err != nil {
		return err
	}
	return c.bw.Flush()
}

// CompactToBuffer returns the compacted JSON payload in memory.
func CompactToBuffer(r io.Reader, maxBytes int64) ([]byte, error) {
	var buf bytes.Buffer
	if err := CompactWriter(&buf, r, maxBytes); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type compactor struct {
	br           *bufio.Reader
	bw           *bufio.Writer
	max          int64
	read         int64
	stack        []containerState
	topValueSeen bool
	tmpBuf       []byte
}

func (c *compactor) run() error {
	for {
		b, err := c.readNonSpace()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(c.stack) != 0 {
					return fmt.Errorf("json: unexpected end of input")
				}
				if !c.topValueSeen {
					return fmt.Errorf("json: empty input")
				}
				return nil
			}
			return err
		}

		if len(c.stack) == 0 {
			if c.topValueSeen {
				return fmt.Errorf("json: multiple top-level values")
			}
			if err := c.handleValue(b); err != nil {
				return err
			}
			continue
		}

		frame := &c.stack[len(c.stack)-1]
		if frame.typ == '{' {
			switch frame.objPhase {
			case objExpectKey:
				if b == '}' {
					if err := c.writeByte('}'); err != nil {
						return err
					}
					c.stack = c.stack[:len(c.stack)-1]
					if err := c.valueComplete(); err != nil {
						return err
					}
					continue
				}
				if b != '"' {
					return fmt.Errorf("json: expected object key")
				}
				if err := c.writeString(); err != nil {
					return err
				}
				frame.objPhase = objExpectColon
			case objExpectColon:
				if b != ':' {
					return fmt.Errorf("json: expected colon after object key")
				}
				if err := c.writeByte(':'); err != nil {
					return err
				}
				frame.objPhase = objExpectValue
			case objExpectValue:
				if err := c.handleValue(b); err != nil {
					return err
				}
				// valueComplete adjusts objPhase when primitive; containers
				// will update it when they close.
				if frame.objPhase == objExpectValue {
					// if valueComplete already set phase, skip.
					// Otherwise we're entering a container; keep phase until completion.
				}
			case objExpectComma:
				if b == ',' {
					if err := c.writeByte(','); err != nil {
						return err
					}
					frame.objPhase = objExpectKey
					continue
				}
				if b == '}' {
					if err := c.writeByte('}'); err != nil {
						return err
					}
					c.stack = c.stack[:len(c.stack)-1]
					if err := c.valueComplete(); err != nil {
						return err
					}
					continue
				}
				return fmt.Errorf("json: expected ',' or '}'")
			}
			continue
		}

		// array
		if frame.arrExpectValue {
			if b == ']' {
				if frame.arrCount != 0 {
					return fmt.Errorf("json: expected array value")
				}
				if err := c.writeByte(']'); err != nil {
					return err
				}
				c.stack = c.stack[:len(c.stack)-1]
				if err := c.valueComplete(); err != nil {
					return err
				}
				continue
			}
			if err := c.handleValue(b); err != nil {
				return err
			}
			frame.arrExpectValue = false
			frame.arrNeedComma = true
			frame.arrCount++
			continue
		}

		// expecting comma or closing
		if b == ',' {
			if !frame.arrNeedComma {
				return fmt.Errorf("json: unexpected comma")
			}
			if err := c.writeByte(','); err != nil {
				return err
			}
			frame.arrExpectValue = true
			frame.arrNeedComma = false
			continue
		}
		if b == ']' {
			if err := c.writeByte(']'); err != nil {
				return err
			}
			c.stack = c.stack[:len(c.stack)-1]
			if err := c.valueComplete(); err != nil {
				return err
			}
			continue
		}
		return fmt.Errorf("json: expected ',' or ']' in array")
	}
}

func (c *compactor) handleValue(b byte) error {
	switch b {
	case '{':
		if err := c.writeByte('{'); err != nil {
			return err
		}
		c.stack = append(c.stack, containerState{typ: '{', objPhase: objExpectKey})
		return nil
	case '[':
		if err := c.writeByte('['); err != nil {
			return err
		}
		c.stack = append(c.stack, containerState{typ: '[', arrExpectValue: true})
		return nil
	case '"':
		if err := c.writeString(); err != nil {
			return err
		}
		return c.valueComplete()
	case 't':
		if err := c.writeLiteral("true"); err != nil {
			return err
		}
		return c.valueComplete()
	case 'f':
		if err := c.writeLiteral("false"); err != nil {
			return err
		}
		return c.valueComplete()
	case 'n':
		if err := c.writeLiteral("null"); err != nil {
			return err
		}
		return c.valueComplete()
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		if err := c.writeNumber(b); err != nil {
			return err
		}
		return c.valueComplete()
	case '}':
		if len(c.stack) == 0 || c.stack[len(c.stack)-1].typ != '{' {
			return fmt.Errorf("json: unexpected '}'")
		}
		frame := &c.stack[len(c.stack)-1]
		if frame.objPhase != objExpectKey && frame.objPhase != objExpectComma {
			return fmt.Errorf("json: unexpected '}'")
		}
		if err := c.writeByte('}'); err != nil {
			return err
		}
		c.stack = c.stack[:len(c.stack)-1]
		return c.valueComplete()
	case ']':
		if len(c.stack) == 0 || c.stack[len(c.stack)-1].typ != '[' {
			return fmt.Errorf("json: unexpected ']'")
		}
		frame := &c.stack[len(c.stack)-1]
		if frame.arrExpectValue && frame.arrCount != 0 {
			return fmt.Errorf("json: unexpected ']' (expecting value)")
		}
		if err := c.writeByte(']'); err != nil {
			return err
		}
		c.stack = c.stack[:len(c.stack)-1]
		return c.valueComplete()
	case ',':
		return fmt.Errorf("json: unexpected comma")
	case ':':
		return fmt.Errorf("json: unexpected colon")
	default:
		return fmt.Errorf("json: invalid character '%c'", b)
	}
}

func (c *compactor) valueComplete() error {
	if len(c.stack) == 0 {
		if c.topValueSeen {
			return fmt.Errorf("json: multiple top-level values")
		}
		c.topValueSeen = true
		return nil
	}
	frame := &c.stack[len(c.stack)-1]
	if frame.typ == '{' {
		frame.objPhase = objExpectComma
		frame.objCount++
	} else {
		frame.arrExpectValue = false
		frame.arrNeedComma = true
		frame.arrCount++
	}
	return nil
}

func (c *compactor) writeLiteral(lit string) error {
	for i := 0; i < len(lit); i++ {
		if i > 0 {
			b, err := c.readByte()
			if err != nil {
				return fmt.Errorf("json: unexpected end in literal")
			}
			if b != lit[i] {
				return fmt.Errorf("json: invalid literal")
			}
		}
	}
	_, err := c.bw.WriteString(lit)
	return err
}

func (c *compactor) writeString() error {
	if err := c.writeByte('"'); err != nil {
		return err
	}
	for {
		b, err := c.readByte()
		if err != nil {
			return fmt.Errorf("json: unterminated string")
		}
		if b == '"' {
			if err := c.writeByte('"'); err != nil {
				return err
			}
			return nil
		}
		if b == '\\' {
			if err := c.writeByte('\\'); err != nil {
				return err
			}
			esc, err := c.readByte()
			if err != nil {
				return fmt.Errorf("json: unterminated escape sequence")
			}
			switch esc {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				if err := c.writeByte(esc); err != nil {
					return err
				}
			case 'u':
				if err := c.writeByte('u'); err != nil {
					return err
				}
				for i := 0; i < 4; i++ {
					hx, err := c.readByte()
					if err != nil {
						return fmt.Errorf("json: invalid unicode escape")
					}
					if !isHexDigit(hx) {
						return fmt.Errorf("json: invalid unicode escape")
					}
					if err := c.writeByte(hx); err != nil {
						return err
					}
				}
			default:
				return fmt.Errorf("json: invalid escape character")
			}
			continue
		}
		if b < 0x20 {
			return fmt.Errorf("json: invalid control character in string")
		}
		if b < utf8.RuneSelf {
			if err := c.writeByte(b); err != nil {
				return err
			}
			continue
		}
		size := utf8RuneLen(b)
		if size == 0 {
			return fmt.Errorf("json: invalid utf-8 in string")
		}
		buf := append(c.tmpBuf[:0], b)
		for i := 1; i < size; i++ {
			nb, err := c.readByte()
			if err != nil {
				return fmt.Errorf("json: invalid utf-8 in string")
			}
			if nb&0xC0 != 0x80 {
				return fmt.Errorf("json: invalid utf-8 in string")
			}
			buf = append(buf, nb)
		}
		if _, err := c.bw.Write(buf); err != nil {
			return err
		}
	}
}

func (c *compactor) writeNumber(first byte) error {
	buf := append(c.tmpBuf[:0], first)

	state := numStart

	nextDigit := func() (byte, error) {
		b, err := c.readByte()
		if err != nil {
			return 0, err
		}
		return b, nil
	}

	var b byte
	var err error

	if first == '-' {
		b, err = nextDigit()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return fmt.Errorf("json: invalid number")
			}
			return err
		}
		buf = append(buf, b)
		if b == '0' {
			state = numAfterZero
		} else if isDigit(b) && b != '0' {
			state = numInteger
		} else {
			return fmt.Errorf("json: invalid number")
		}
	} else if first == '0' {
		state = numAfterZero
	} else if isDigit(first) {
		state = numInteger
	} else {
		return fmt.Errorf("json: invalid number")
	}

	for {
		b, err = c.readByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		switch state {
		case numAfterZero:
			if b == '.' {
				state = numFracStart
				buf = append(buf, b)
				continue
			}
			if b == 'e' || b == 'E' {
				state = numExpSign
				buf = append(buf, b)
				continue
			}
			if isDigit(b) {
				return fmt.Errorf("json: leading zeros not allowed")
			}
			c.unread()
			goto DONE
		case numInteger:
			if isDigit(b) {
				buf = append(buf, b)
				continue
			}
			if b == '.' {
				state = numFracStart
				buf = append(buf, b)
				continue
			}
			if b == 'e' || b == 'E' {
				state = numExpSign
				buf = append(buf, b)
				continue
			}
			c.unread()
			goto DONE
		case numFracStart:
			if isDigit(b) {
				state = numFrac
				buf = append(buf, b)
				continue
			}
			return fmt.Errorf("json: invalid fraction")
		case numFrac:
			if isDigit(b) {
				buf = append(buf, b)
				continue
			}
			if b == 'e' || b == 'E' {
				state = numExpSign
				buf = append(buf, b)
				continue
			}
			c.unread()
			goto DONE
		case numExpSign:
			if b == '+' || b == '-' {
				state = numExpDigits
				buf = append(buf, b)
				continue
			}
			if isDigit(b) {
				state = numExpDigits
				buf = append(buf, b)
				continue
			}
			return fmt.Errorf("json: invalid exponent")
		case numExpDigits:
			if isDigit(b) {
				buf = append(buf, b)
				continue
			}
			c.unread()
			goto DONE
		}
	}

DONE:
	switch state {
	case numStart, numFracStart, numExpSign:
		return fmt.Errorf("json: invalid number")
	}
	_, err = c.bw.Write(buf)
	return err
}

const (
	numStart = iota
	numAfterZero
	numInteger
	numFracStart
	numFrac
	numExpSign
	numExpDigits
)

func (c *compactor) writeByte(b byte) error {
	return c.bw.WriteByte(b)
}

func (c *compactor) readByte() (byte, error) {
	b, err := c.br.ReadByte()
	if err != nil {
		return 0, err
	}
	c.read++
	if c.max > 0 && c.read > c.max {
		return 0, fmt.Errorf("json: payload exceeds %d bytes", c.max)
	}
	return b, nil
}

func (c *compactor) unread() {
	_ = c.br.UnreadByte()
	c.read--
}

func (c *compactor) readNonSpace() (byte, error) {
	for {
		b, err := c.readByte()
		if err != nil {
			return 0, err
		}
		if b == ' ' || b == '\n' || b == '\r' || b == '\t' {
			continue
		}
		return b, nil
	}
}

func isHexDigit(b byte) bool {
	return ('0' <= b && b <= '9') || ('a' <= b && b <= 'f') || ('A' <= b && b <= 'F')
}

func isDigit(b byte) bool {
	return '0' <= b && b <= '9'
}

func utf8RuneLen(b byte) int {
	switch {
	case b&0x80 == 0x00:
		return 1
	case b&0xE0 == 0xC0:
		return 2
	case b&0xF0 == 0xE0:
		return 3
	case b&0xF8 == 0xF0:
		return 4
	default:
		return 0
	}
}
