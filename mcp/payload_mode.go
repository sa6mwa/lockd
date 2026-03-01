package mcp

import (
	"fmt"
	"strings"
)

type payloadMode string

const (
	payloadModeAuto   payloadMode = "auto"
	payloadModeInline payloadMode = "inline"
	payloadModeStream payloadMode = "stream"
	payloadModeNone   payloadMode = "none"
)

func parsePayloadMode(raw string, defaultMode payloadMode) (payloadMode, error) {
	return parsePayloadModeField(raw, defaultMode, "payload_mode")
}

func parsePayloadModeField(raw string, defaultMode payloadMode, fieldName string) (payloadMode, error) {
	mode := payloadMode(strings.ToLower(strings.TrimSpace(raw)))
	if mode == "" {
		mode = defaultMode
	}
	switch mode {
	case payloadModeAuto, payloadModeInline, payloadModeStream, payloadModeNone:
		return mode, nil
	default:
		fieldName = strings.TrimSpace(fieldName)
		if fieldName == "" {
			fieldName = "payload_mode"
		}
		return "", fmt.Errorf("invalid %s %q (expected auto|inline|stream|none)", fieldName, raw)
	}
}
