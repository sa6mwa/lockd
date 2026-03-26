package mcp

import "pkt.systems/lockd/mcp/preset"

const (
	builtInLockdPresetName = preset.ReservedLockdName
	lockdReservedFieldPref = preset.ReservedFieldPrefix
)

var defaultPresetOperations = append([]PresetOperation(nil), preset.DefaultOperations...)

type Preset = preset.Definition
type PresetKind = preset.Kind
type PresetGeneratedTool = preset.GeneratedTool
type PresetOperation = preset.Operation
type PresetSchema = preset.Schema
type AttachmentMetadata = preset.AttachmentMetadata

const (
	PresetOperationQuery          = preset.OperationQuery
	PresetOperationStatePut       = preset.OperationStatePut
	PresetOperationStateGet       = preset.OperationStateGet
	PresetOperationStateDelete    = preset.OperationStateDelete
	PresetOperationQueueEnqueue   = preset.OperationQueueEnqueue
	PresetOperationAttachmentsGet = preset.OperationAttachmentsGet
)

func ParsePresetYAML(data []byte) ([]Preset, error) {
	return preset.ParseYAML(data)
}

func LoadPresetsFile(path string) ([]Preset, error) {
	return preset.LoadFile(path)
}
