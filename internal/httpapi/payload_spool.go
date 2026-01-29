package httpapi

import (
	"bytes"
	"io"
	"os"
	"sync"
)

// payloadSpool buffers payloads in memory up to a threshold, then spills to a temp file.
type payloadSpool struct {
	threshold int64
	buf       []byte
	file      *os.File
	pooled    bool
}

var payloadBufferPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, defaultPayloadSpoolMemoryThreshold)
	},
}

func newPayloadSpool(threshold int64) *payloadSpool {
	ps := &payloadSpool{threshold: threshold}
	if threshold <= 0 {
		return ps
	}
	maxInt := int64(^uint(0) >> 1)
	if threshold > maxInt {
		threshold = maxInt
	}
	bufCap := int(threshold)
	if threshold == defaultPayloadSpoolMemoryThreshold {
		if buf, ok := payloadBufferPool.Get().([]byte); ok {
			if cap(buf) < bufCap {
				buf = make([]byte, 0, bufCap)
			} else {
				buf = buf[:0]
			}
			ps.buf = buf
			ps.pooled = true
			return ps
		}
	}
	if bufCap > 0 {
		ps.buf = make([]byte, 0, bufCap)
	}
	return ps
}

func (p *payloadSpool) Write(data []byte) (int, error) {
	if p.file != nil {
		return p.file.Write(data)
	}
	if int64(len(p.buf))+int64(len(data)) <= p.threshold {
		p.buf = append(p.buf, data...)
		return len(data), nil
	}
	f, err := os.CreateTemp("", "lockd-json-")
	if err != nil {
		return 0, err
	}
	if len(p.buf) > 0 {
		if _, err := f.Write(p.buf); err != nil {
			f.Close()
			_ = os.Remove(f.Name())
			return 0, err
		}
	}
	if p.pooled && p.buf != nil {
		payloadBufferPool.Put(p.buf[:0]) //nolint:staticcheck // avoid extra allocation by pooling value slice
		p.pooled = false
	}
	n, err := f.Write(data)
	if err != nil {
		f.Close()
		_ = os.Remove(f.Name())
		return n, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		_ = os.Remove(f.Name())
		return n, err
	}
	p.file = f
	p.buf = nil
	return n, nil
}

func (p *payloadSpool) Reader() (io.ReadSeeker, error) {
	if p.file != nil {
		if _, err := p.file.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		return p.file, nil
	}
	return bytes.NewReader(p.buf), nil
}

func (p *payloadSpool) Close() error {
	if p.file != nil {
		name := p.file.Name()
		err := p.file.Close()
		_ = os.Remove(name)
		p.file = nil
		return err
	}
	if p.pooled && p.buf != nil {
		payloadBufferPool.Put(p.buf[:0]) //nolint:staticcheck // avoid extra allocation by pooling value slice
		p.pooled = false
	}
	p.buf = nil
	return nil
}
