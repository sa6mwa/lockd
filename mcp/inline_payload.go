package mcp

import (
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

const defaultMCPInlineMaxBytes int64 = 2 * 1024 * 1024

func normalizedInlineMaxBytes(raw int64) int64 {
	if raw <= 0 {
		return defaultMCPInlineMaxBytes
	}
	return raw
}

func validateInlinePayloadBytes(sizeBytes int64, configuredLimit int64, inlineTool, streamBeginTool string) error {
	limit := normalizedInlineMaxBytes(configuredLimit)
	if sizeBytes <= limit {
		return nil
	}
	return inlinePayloadTooLargeError(inlineTool, sizeBytes, limit, streamBeginTool)
}

func inlinePayloadTooLargeError(tool string, sizeBytes, limit int64, streamTools ...string) error {
	tool = strings.TrimSpace(tool)
	if tool == "" {
		tool = "operation"
	}
	hint := "Call `lockd.hint` to read `inline_max_payload_bytes` and choose inline or streaming mode."
	if len(streamTools) == 0 {
		return fmt.Errorf("%s inline payload denied: payload is %d bytes and exceeds mcp.inline_max_bytes=%d. %s", tool, sizeBytes, limit, hint)
	}
	for i := range streamTools {
		streamTools[i] = strings.TrimSpace(streamTools[i])
	}
	return fmt.Errorf(
		"%s inline payload denied: payload is %d bytes and exceeds mcp.inline_max_bytes=%d. Use streaming tools: %s. %s",
		tool,
		sizeBytes,
		limit,
		strings.Join(streamTools, ", "),
		hint,
	)
}

type inlinePayloadContent struct {
	Bytes   int64
	Text    string
	Base64  string
	IsText  bool
	Present bool
}

func encodeInlinePayload(payload []byte) inlinePayloadContent {
	if len(payload) == 0 {
		return inlinePayloadContent{Present: true}
	}
	out := inlinePayloadContent{
		Bytes:   int64(len(payload)),
		Present: true,
	}
	if utf8.Valid(payload) {
		out.Text = string(payload)
		out.IsText = true
		return out
	}
	out.Base64 = base64.StdEncoding.EncodeToString(payload)
	return out
}

func readInlinePayloadStrict(reader io.Reader, configuredLimit int64, tool string, streamTools ...string) (inlinePayloadContent, error) {
	if reader == nil {
		return inlinePayloadContent{}, nil
	}
	limit := normalizedInlineMaxBytes(configuredLimit)
	lr := io.LimitReader(reader, limit+1)
	buf, err := io.ReadAll(lr)
	if err != nil {
		return inlinePayloadContent{}, err
	}
	if int64(len(buf)) > limit {
		return inlinePayloadContent{}, inlinePayloadTooLargeError(tool, int64(len(buf)), limit, streamTools...)
	}
	return encodeInlinePayload(buf), nil
}

func readInlinePayloadAuto(reader io.Reader, configuredLimit int64) (inlinePayloadContent, bool, error) {
	if reader == nil {
		return inlinePayloadContent{}, false, nil
	}
	limit := normalizedInlineMaxBytes(configuredLimit)
	lr := io.LimitReader(reader, limit+1)
	buf, err := io.ReadAll(lr)
	if err != nil {
		return inlinePayloadContent{}, false, err
	}
	if int64(len(buf)) > limit {
		return inlinePayloadContent{}, true, nil
	}
	return encodeInlinePayload(buf), false, nil
}
