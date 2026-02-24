package mcp

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func FuzzDecodeWriteStreamChunk(f *testing.F) {
	f.Add(base64.StdEncoding.EncodeToString([]byte("hello")), int64(8))
	f.Add("not-base64", int64(1024))
	f.Add(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("a"), 128)), int64(16))
	f.Add("   "+base64.StdEncoding.EncodeToString([]byte("{}"))+"   ", int64(2*1024*1024))

	f.Fuzz(func(t *testing.T, chunkBase64 string, maxBytes int64) {
		chunkBase64 = fuzzClampString(chunkBase64, 16*1024)
		if maxBytes < -8*1024*1024 {
			maxBytes = -8 * 1024 * 1024
		}
		if maxBytes > 8*1024*1024 {
			maxBytes = 8 * 1024 * 1024
		}

		decoded, err := decodeWriteStreamChunk(chunkBase64, maxBytes)
		if err != nil {
			return
		}

		limit := normalizedInlineMaxBytes(maxBytes)
		if int64(len(decoded)) > limit {
			t.Fatalf("decoded length %d exceeds limit %d", len(decoded), limit)
		}
		expected, decodeErr := base64.StdEncoding.DecodeString(strings.TrimSpace(chunkBase64))
		if decodeErr != nil {
			t.Fatalf("decodeWriteStreamChunk succeeded while std decode failed: %v", decodeErr)
		}
		if !bytes.Equal(decoded, expected) {
			t.Fatalf("decoded payload mismatch")
		}
	})
}

func FuzzApplyJSONMergePatch(f *testing.F) {
	f.Add([]byte(`{"a":1}`), []byte(`{"b":2}`))
	f.Add([]byte(`{"nested":{"x":1}}`), []byte(`{"nested":{"x":null,"y":2}}`))
	f.Add([]byte(``), []byte(`{"ok":true}`))
	f.Add([]byte(`{"array":[1,2,3]}`), []byte(`[1,2]`))

	f.Fuzz(func(t *testing.T, current, patch []byte) {
		current = fuzzClampBytes(current, 32*1024)
		patch = fuzzClampBytes(patch, 32*1024)

		merged, err := applyJSONMergePatch(current, patch)
		if err != nil {
			return
		}
		if !json.Valid(merged) {
			t.Fatalf("merged output is not valid JSON: %q", string(merged))
		}
	})
}

func FuzzDecodePatchPayload(f *testing.F) {
	f.Add("", "")
	f.Add(`{"k":"v"}`, "")
	f.Add("", base64.StdEncoding.EncodeToString([]byte(`{"k":"v"}`)))
	f.Add("  ", "%%invalid%%")

	f.Fuzz(func(t *testing.T, patchText, patchBase64 string) {
		patchText = fuzzClampString(patchText, 64*1024)
		patchBase64 = fuzzClampString(patchBase64, 64*1024)
		input := statePatchToolInput{
			PatchText:   patchText,
			PatchBase64: patchBase64,
		}

		payload, err := decodePatchPayload(input)
		if err != nil {
			return
		}
		if strings.TrimSpace(patchBase64) != "" {
			expected, decodeErr := base64.StdEncoding.DecodeString(strings.TrimSpace(patchBase64))
			if decodeErr != nil {
				t.Fatalf("decodePatchPayload succeeded while base64 decode failed: %v", decodeErr)
			}
			if !bytes.Equal(payload, expected) {
				t.Fatalf("base64 patch payload mismatch")
			}
			return
		}
		if strings.TrimSpace(patchText) != "" && string(payload) != patchText {
			t.Fatalf("text patch payload mismatch")
		}
	})
}

func fuzzClampBytes(in []byte, max int) []byte {
	if max <= 0 || len(in) <= max {
		return in
	}
	return in[:max]
}

func fuzzClampString(in string, max int) string {
	if max <= 0 || len(in) <= max {
		return in
	}
	return in[:max]
}
