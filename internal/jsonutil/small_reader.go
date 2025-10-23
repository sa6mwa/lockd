package jsonutil

import "io"

type smallReader struct {
	prefix []byte
	reader io.Reader
	offset int
}

func (sr *smallReader) Read(p []byte) (int, error) {
	if sr.offset < len(sr.prefix) {
		n := copy(p, sr.prefix[sr.offset:])
		sr.offset += n
		if n < len(p) {
			m, err := sr.reader.Read(p[n:])
			return n + m, err
		}
		return n, nil
	}
	return sr.reader.Read(p)
}
