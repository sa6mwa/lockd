package mcp

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func FuzzParsePayloadMode(f *testing.F) {
	f.Add("auto")
	f.Add("inline")
	f.Add("stream")
	f.Add("none")
	f.Add("  INLINE  ")
	f.Add("invalid")

	f.Fuzz(func(t *testing.T, raw string) {
		raw = fuzzClampString(raw, 256)
		mode, err := parsePayloadMode(raw, payloadModeAuto)
		if err != nil {
			return
		}
		switch mode {
		case payloadModeAuto, payloadModeInline, payloadModeStream, payloadModeNone:
		default:
			t.Fatalf("unexpected parsed mode %q", mode)
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
