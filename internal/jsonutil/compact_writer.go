package jsonutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"pkt.systems/jpact"
)

const smallJSONThreshold = 2048

// CompactWriter streams JSON from r to w, stripping insignificant whitespace.
// maxBytes limits the number of bytes read from r (<=0 disables the limit).
func CompactWriter(w io.Writer, r io.Reader, maxBytes int64) error {
	threshold := smallJSONThreshold
	if maxBytes > 0 && maxBytes < int64(threshold) {
		threshold = int(maxBytes)
	}
	if threshold <= 0 {
		return jpact.CompactWriter(w, r, maxBytes)
	}

	limit := threshold + 1
	var stack [smallJSONThreshold + 1]byte
	buf := stack[:limit]
	total := 0
	hasSpace := false

	for total < limit {
		n, err := r.Read(buf[total:limit])
		if n > 0 {
			if !hasSpace {
			scan:
				for _, b := range buf[total : total+n] {
					switch b {
					case ' ', '\n', '\t', '\r':
						hasSpace = true
						break scan
					}
				}
			}
		}
		total += n
		if maxBytes > 0 && int64(total) > maxBytes {
			return fmt.Errorf("json: payload exceeds %d bytes", maxBytes)
		}
		if err != nil {
			if err == io.EOF {
				if total <= threshold {
					payload := buf[:total]
					if !json.Valid(payload) {
						return fmt.Errorf("json: invalid input")
					}
					if !hasSpace {
						_, err = w.Write(payload)
						return err
					}
					return jpact.CompactWriter(w, bytes.NewReader(payload), maxBytes)
				}
				break
			}
			return err
		}
	}

	data := make([]byte, total)
	copy(data, buf[:total])
	return jpact.CompactWriter(w, io.MultiReader(bytes.NewReader(data), r), maxBytes)
}

// CompactToBuffer returns the compacted JSON payload in memory.
func CompactToBuffer(r io.Reader, maxBytes int64) ([]byte, error) {
	return jpact.CompactToBuffer(r, maxBytes)
}
