package mcp

import "pkt.systems/lockd/mcp/preset"

var defaultPresetOperations = append([]PresetOperation(nil), preset.DefaultOperations...)

// Preset aliases the normalized MCP preset definition model.
type Preset = preset.Definition

// PresetKind aliases one schema-bound preset document kind.
type PresetKind = preset.Kind

// PresetGeneratedTool aliases the generated preset tool-name set.
type PresetGeneratedTool = preset.GeneratedTool

// PresetOperation aliases a supported preset kind operation.
type PresetOperation = preset.Operation

// PresetSchema aliases the normalized preset JSON schema subset.
type PresetSchema = preset.Schema

// AttachmentMetadata aliases preset attachment metadata surfaced to tools.
type AttachmentMetadata = preset.AttachmentMetadata

const (
	// PresetOperationQuery exposes keys-only preset queries.
	PresetOperationQuery = preset.OperationQuery
	// PresetOperationStatePut writes a schema-backed preset document.
	PresetOperationStatePut = preset.OperationStatePut
	// PresetOperationStateGet reads a schema-backed preset document.
	PresetOperationStateGet = preset.OperationStateGet
	// PresetOperationStateDelete removes a schema-backed preset document.
	PresetOperationStateDelete = preset.OperationStateDelete
	// PresetOperationQueueEnqueue enqueues a schema-backed preset payload.
	PresetOperationQueueEnqueue = preset.OperationQueueEnqueue
	// PresetOperationAttachmentsGet retrieves preset document attachments.
	PresetOperationAttachmentsGet = preset.OperationAttachmentsGet
)

// ParsePresetYAML parses and normalizes one or more preset YAML documents.
func ParsePresetYAML(data []byte) ([]Preset, error) {
	return preset.ParseYAML(data)
}

// LoadPresetsFile reads, parses, and normalizes one preset YAML file.
func LoadPresetsFile(path string) ([]Preset, error) {
	return preset.LoadFile(path)
}
