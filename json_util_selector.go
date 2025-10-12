package lockd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"pkt.systems/lockd/internal/jsonutil"
	"pkt.systems/lockd/internal/jsonutilv2"
)

type jsonUtilImpl struct {
	name          string
	compactWriter func(io.Writer, io.Reader, int64) error
}

func selectJSONUtil(name string) (jsonUtilImpl, error) {
	switch name {
	case JSONUtilLockd:
		return jsonUtilImpl{
			name:          JSONUtilLockd,
			compactWriter: jsonutil.CompactWriter,
		}, nil
	case JSONUtilJSONV2:
		return jsonUtilImpl{
			name:          JSONUtilJSONV2,
			compactWriter: jsonutilv2.CompactWriter,
		}, nil
	case JSONUtilStdlib:
		return jsonUtilImpl{
			name:          JSONUtilStdlib,
			compactWriter: stdlibCompactWriter,
		}, nil
	default:
		return jsonUtilImpl{}, fmt.Errorf("unknown json util %q", name)
	}
}

func stdlibCompactWriter(w io.Writer, r io.Reader, maxBytes int64) error {
	data, err := readAllWithLimit(r, maxBytes)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, data); err != nil {
		return err
	}
	_, err = io.Copy(w, &buf)
	return err
}

func readAllWithLimit(r io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return io.ReadAll(r)
	}
	lr := io.LimitReader(r, maxBytes+1)
	data, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("json: payload exceeds %d bytes", maxBytes)
	}
	return data, nil
}
