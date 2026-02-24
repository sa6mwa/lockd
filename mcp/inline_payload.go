package mcp

import "fmt"

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
	return fmt.Errorf("%s payload %d bytes exceeds mcp.inline_max_bytes=%d; use %s for larger payloads", inlineTool, sizeBytes, limit, streamBeginTool)
}
