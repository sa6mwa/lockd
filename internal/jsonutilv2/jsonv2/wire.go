// Copyright 2023 The Go Authors.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// The implementation in this file is derived from Go 1.25's
// encoding/json/internal/jsonwire package, adapted for lockd's internal
// tokenizer needs. Significant modifications include trimming unused
// functionality, renaming types, and integrating with the local error
// handling strategy.

package jsonv2

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

type valueFlags uint

const (
	_ valueFlags = (1 << iota) / 2 // start at zero power of two
	stringNonVerbatim
	stringNonCanonical
)

func (f *valueFlags) Join(other valueFlags) { *f |= other }

func consumeWhitespace(b []byte) int {
	i := 0
	for len(b) > i {
		switch b[i] {
		case ' ', '\t', '\r', '\n':
			i++
			continue
		default:
			return i
		}
	}
	return i
}

func consumeLiteral(b []byte, lit string) (int, error) {
	for i := 0; i < len(b) && i < len(lit); i++ {
		if b[i] != lit[i] {
			return i, newInvalidCharacterError(b[i:], "in literal "+lit+" (expecting "+strconv.QuoteRune(rune(lit[i]))+")")
		}
	}
	if len(b) < len(lit) {
		return len(b), io.ErrUnexpectedEOF
	}
	return len(lit), nil
}

func consumeStringResumable(flags *valueFlags, b []byte, resume int, validateUTF8 bool) (int, error) {
	n := resume
	switch {
	case resume > 0:
		// already processed opening quote
	case len(b) == 0:
		return n, io.ErrUnexpectedEOF
	case b[0] == '"':
		n = 1
	default:
		return n, newInvalidCharacterError(b[n:], `at start of string (expecting '"')`)
	}

	for len(b) > n {
		for len(b) > n {
			c := b[n]
			if c < utf8.RuneSelf && c >= ' ' && c != '"' && c != '\\' {
				n++
				continue
			}
			break
		}
		if len(b) <= n {
			return n, io.ErrUnexpectedEOF
		}
		if b[n] == '"' {
			n++
			return n, nil
		}

		switch r, rn := utf8.DecodeRune(b[n:]); {
		case rn > 1:
			n += rn
		case r == '\\':
			flags.Join(stringNonVerbatim)
			resume = n
			if len(b) < n+2 {
				return resume, io.ErrUnexpectedEOF
			}
			switch esc := b[n+1]; esc {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				n += 2
			case 'u':
				if len(b) < n+6 {
					if hasEscapedUTF16Prefix(b[n:], false) {
						return resume, io.ErrUnexpectedEOF
					}
					flags.Join(stringNonCanonical)
					return n, newInvalidEscapeSequenceError(b[n:])
				}
				v1, ok := parseHexUint16(b[n+2 : n+6])
				if !ok {
					flags.Join(stringNonCanonical)
					return n, newInvalidEscapeSequenceError(b[n : n+6])
				}
				n += 6
				r := rune(v1)
				if validateUTF8 && utf16.IsSurrogate(r) {
					if len(b) < n+6 {
						if hasEscapedUTF16Prefix(b[n:], true) {
							return resume, io.ErrUnexpectedEOF
						}
						flags.Join(stringNonCanonical)
						return n - 6, newInvalidEscapeSequenceError(b[n-6:])
					}
					if b[n] != '\\' || b[n+1] != 'u' {
						flags.Join(stringNonCanonical)
						return n - 6, newInvalidEscapeSequenceError(b[n-6 : n+6])
					}
					v2, ok := parseHexUint16(b[n+2 : n+6])
					if !ok {
						flags.Join(stringNonCanonical)
						return n - 6, newInvalidEscapeSequenceError(b[n-6 : n+6])
					}
					if rr := utf16.DecodeRune(rune(v1), rune(v2)); rr == utf8.RuneError {
						flags.Join(stringNonCanonical)
						return n - 6, newInvalidEscapeSequenceError(b[n-6 : n+6])
					}
					n += 6
				}
			default:
				flags.Join(stringNonCanonical)
				return n, newInvalidEscapeSequenceError(b[n : n+2])
			}
		case r == utf8.RuneError:
			if !utf8.FullRune(b[n:]) {
				return n, io.ErrUnexpectedEOF
			}
			flags.Join(stringNonVerbatim | stringNonCanonical)
			if validateUTF8 {
				return n, errInvalidUTF8
			}
			n++
		case r < ' ':
			flags.Join(stringNonVerbatim | stringNonCanonical)
			return n, newInvalidCharacterError(b[n:], "in string (expecting non-control character)")
		default:
			panic("jsonv2: unreachable rune branch")
		}
	}
	return n, io.ErrUnexpectedEOF
}

type consumeNumberState uint

const (
	consumeNumberInit consumeNumberState = iota
	beforeIntegerDigits
	withinIntegerDigits
	beforeFractionalDigits
	withinFractionalDigits
	beforeExponentDigits
	withinExponentDigits
)

func consumeNumberResumable(b []byte, resume int, state consumeNumberState) (int, consumeNumberState, error) {
	n := resume
	if state > consumeNumberInit {
		switch state {
		case withinIntegerDigits, withinFractionalDigits, withinExponentDigits:
			for len(b) > n && '0' <= b[n] && b[n] <= '9' {
				n++
			}
			if len(b) <= n {
				return n, state, nil
			}
			state++
		}
		switch state {
		case beforeIntegerDigits:
			goto beforeInteger
		case beforeFractionalDigits:
			goto beforeFractional
		case beforeExponentDigits:
			goto beforeExponent
		default:
			return n, state, nil
		}
	}

beforeInteger:
	resume = n
	if len(b) > n && b[n] == '-' {
		n++
	}
	switch {
	case len(b) <= n:
		return resume, beforeIntegerDigits, io.ErrUnexpectedEOF
	case b[n] == '0':
		n++
		state = beforeFractionalDigits
	case '1' <= b[n] && b[n] <= '9':
		n++
		for len(b) > n && '0' <= b[n] && b[n] <= '9' {
			n++
		}
		state = withinIntegerDigits
	default:
		return n, state, newInvalidCharacterError(b[n:], "in number (expecting digit)")
	}

beforeFractional:
	if len(b) > n && b[n] == '.' {
		resume = n
		n++
		switch {
		case len(b) <= n:
			return resume, beforeFractionalDigits, io.ErrUnexpectedEOF
		case '0' <= b[n] && b[n] <= '9':
			n++
		default:
			return n, state, newInvalidCharacterError(b[n:], "in number (expecting digit)")
		}
		for len(b) > n && '0' <= b[n] && b[n] <= '9' {
			n++
		}
		state = withinFractionalDigits
	}

beforeExponent:
	if len(b) > n && (b[n] == 'e' || b[n] == 'E') {
		resume = n
		n++
		if len(b) > n && (b[n] == '+' || b[n] == '-') {
			n++
		}
		switch {
		case len(b) <= n:
			return resume, beforeExponentDigits, io.ErrUnexpectedEOF
		case '0' <= b[n] && b[n] <= '9':
			n++
		default:
			return n, state, newInvalidCharacterError(b[n:], "in number (expecting digit)")
		}
		for len(b) > n && '0' <= b[n] && b[n] <= '9' {
			n++
		}
		state = withinExponentDigits
	}

	return n, state, nil
}

var errInvalidEOF = errors.New("unexpected end of JSON input")
var errInvalidUTF8 = errors.New("invalid UTF-8")

func newInvalidCharacterError(prefix []byte, where string) error {
	what := quoteRune(prefix)
	return errors.New("invalid character " + what + " " + where)
}

func newInvalidEscapeSequenceError(what []byte) error {
	label := "escape sequence"
	if len(what) > 6 {
		label = "surrogate pair"
	}
	needEscape := strings.IndexFunc(string(what), func(r rune) bool {
		return r == '`' || r == utf8.RuneError || unicode.IsSpace(r) || !unicode.IsPrint(r)
	}) >= 0
	if needEscape {
		return errors.New("invalid " + label + " " + strconv.Quote(string(what)) + " in string")
	}
	return errors.New("invalid " + label + " `" + string(what) + "` in string")
}

func quoteRune(b []byte) string {
	r, n := utf8.DecodeRune(truncateMaxUTF8(b))
	if r == utf8.RuneError && n == 1 {
		return fmt.Sprintf("'\\x%02x'", b[0])
	}
	return strconv.QuoteRune(r)
}

func truncateMaxUTF8(b []byte) []byte {
	if len(b) > utf8.UTFMax {
		return b[:utf8.UTFMax]
	}
	return b
}

func parseHexUint16(b []byte) (uint16, bool) {
	if len(b) != 4 {
		return 0, false
	}
	var v uint16
	for i := range 4 {
		c := b[i]
		switch {
		case '0' <= c && c <= '9':
			c -= '0'
		case 'a' <= c && c <= 'f':
			c = 10 + c - 'a'
		case 'A' <= c && c <= 'F':
			c = 10 + c - 'A'
		default:
			return 0, false
		}
		v = v*16 + uint16(c)
	}
	return v, true
}

func hasEscapedUTF16Prefix(b []byte, lowerSurrogate bool) bool {
	for i := range b {
		switch c := b[i]; {
		case i == 0 && c != '\\':
			return false
		case i == 1 && c != 'u':
			return false
		case i == 2 && lowerSurrogate && c != 'd' && c != 'D':
			return false
		case i == 3 && lowerSurrogate && !(('c' <= c && c <= 'f') || ('C' <= c && c <= 'F')):
			return false
		case i >= 6:
			return true
		}
	}
	return true
}

func isLiteralTerminator(b byte) bool {
	switch b {
	case ' ', '\t', '\r', '\n', ',', '}', ']':
		return true
	}
	return false
}

func isNumberTerminator(b byte) bool {
	switch b {
	case ' ', '\t', '\r', '\n', ',', '}', ']':
		return true
	}
	return false
}

func validateLiteralTerminator(next byte) error {
	if isLiteralTerminator(next) {
		return nil
	}
	return newInvalidCharacterError([]byte{next}, "after literal")
}

func validateNumberTerminator(next byte) error {
	if isNumberTerminator(next) {
		return nil
	}
	return newInvalidCharacterError([]byte{next}, "after number")
}
