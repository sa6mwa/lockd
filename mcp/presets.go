package mcp

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"pkt.systems/lockd/namespaces"
)

const (
	builtInLockdPresetName = "lockd"
	lockdReservedFieldPref = "_lockd_"
	maxPresetSchemaDepth   = 1
)

var defaultPresetOperations = []PresetOperation{
	PresetOperationQuery,
	PresetOperationStatePut,
	PresetOperationStateGet,
	PresetOperationStateDelete,
	PresetOperationQueueEnqueue,
	PresetOperationAttachmentsGet,
}

// Preset describes one custom MCP tool surface.
type Preset struct {
	Name        string       `json:"name" yaml:"preset"`
	Description string       `json:"description,omitempty" yaml:"description,omitempty"`
	Kinds       []PresetKind `json:"kinds" yaml:"kinds"`
}

// PresetKind binds one document schema to one hidden namespace.
type PresetKind struct {
	Name        string              `json:"name" yaml:"name"`
	Description string              `json:"description,omitempty" yaml:"description,omitempty"`
	Namespace   string              `json:"namespace" yaml:"namespace"`
	Operations  []PresetOperation   `json:"operations,omitempty" yaml:"operations,omitempty"`
	Schema      PresetSchema        `json:"schema" yaml:"schema"`
	Tools       PresetGeneratedTool `json:"tools" yaml:"-"`
}

// PresetGeneratedTool contains canonical generated tool names for one kind.
type PresetGeneratedTool struct {
	Query          string `json:"query"`
	StatePut       string `json:"state_put"`
	StateGet       string `json:"state_get"`
	StateDelete    string `json:"state_delete"`
	QueueEnqueue   string `json:"queue_enqueue"`
	AttachmentsGet string `json:"attachments_get"`
}

// PresetOperation is a supported preset kind operation.
type PresetOperation string

const (
	PresetOperationQuery          PresetOperation = "query"
	PresetOperationStatePut       PresetOperation = "state.put"
	PresetOperationStateGet       PresetOperation = "state.get"
	PresetOperationStateDelete    PresetOperation = "state.delete"
	PresetOperationQueueEnqueue   PresetOperation = "queue.enqueue"
	PresetOperationAttachmentsGet PresetOperation = "attachments.get"
)

var validPresetOperations = map[PresetOperation]struct{}{
	PresetOperationQuery:          {},
	PresetOperationStatePut:       {},
	PresetOperationStateGet:       {},
	PresetOperationStateDelete:    {},
	PresetOperationQueueEnqueue:   {},
	PresetOperationAttachmentsGet: {},
}

// PresetSchema is the normalized schema subset supported by MCP presets.
type PresetSchema struct {
	Type                 string                  `json:"type" yaml:"type"`
	Description          string                  `json:"description,omitempty" yaml:"description,omitempty"`
	Properties           map[string]PresetSchema `json:"properties,omitempty" yaml:"properties,omitempty"`
	Required             []string                `json:"required,omitempty" yaml:"required,omitempty"`
	Items                *PresetSchema           `json:"items,omitempty" yaml:"items,omitempty"`
	AdditionalProperties bool                    `json:"additional_properties" yaml:"additionalProperties,omitempty"`
}

// AttachmentMetadata is the reserved preset state.get attachment shape.
type AttachmentMetadata struct {
	Name        string `json:"name"`
	ContentType string `json:"content_type,omitempty"`
	SizeBytes   int64  `json:"size_bytes,omitempty"`
	ETag        string `json:"etag,omitempty"`
}

// ParsePresetYAML parses one or more YAML documents into normalized presets.
func ParsePresetYAML(data []byte) ([]Preset, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	presets := make([]Preset, 0)
	seenNames := make(map[string]struct{})
	docIndex := 0
	for {
		var raw Preset
		err := dec.Decode(&raw)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("parse preset yaml document %d: %w", docIndex+1, err)
		}
		if isEmptyPresetDocument(raw) {
			docIndex++
			continue
		}
		normalized, err := normalizePreset(raw)
		if err != nil {
			return nil, fmt.Errorf("normalize preset yaml document %d: %w", docIndex+1, err)
		}
		if _, ok := seenNames[normalized.Name]; ok {
			return nil, fmt.Errorf("duplicate preset %q", normalized.Name)
		}
		seenNames[normalized.Name] = struct{}{}
		presets = append(presets, normalized)
		docIndex++
	}
	if len(presets) == 0 {
		return nil, fmt.Errorf("preset yaml did not contain any presets")
	}
	return presets, nil
}

// LoadPresetsFile reads and parses one preset YAML file.
func LoadPresetsFile(path string) ([]Preset, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read preset file %s: %w", path, err)
	}
	presets, err := ParsePresetYAML(data)
	if err != nil {
		return nil, fmt.Errorf("parse preset file %s: %w", path, err)
	}
	return presets, nil
}

func normalizePreset(in Preset) (Preset, error) {
	name, err := normalizePresetIdentifier(in.Name)
	if err != nil {
		return Preset{}, fmt.Errorf("preset: %w", err)
	}
	if name == builtInLockdPresetName {
		return Preset{}, fmt.Errorf("preset name %q is reserved", builtInLockdPresetName)
	}
	out := Preset{
		Name:        name,
		Description: strings.TrimSpace(in.Description),
		Kinds:       make([]PresetKind, 0, len(in.Kinds)),
	}
	if len(in.Kinds) == 0 {
		return Preset{}, fmt.Errorf("preset %q must contain at least one kind", out.Name)
	}
	seenKinds := make(map[string]struct{}, len(in.Kinds))
	seenTools := map[string]struct{}{
		presetHelpToolName(out.Name): {},
	}
	for idx, kind := range in.Kinds {
		normalizedKind, err := normalizePresetKind(out.Name, kind)
		if err != nil {
			return Preset{}, fmt.Errorf("preset %q kind %d: %w", out.Name, idx+1, err)
		}
		if _, ok := seenKinds[normalizedKind.Name]; ok {
			return Preset{}, fmt.Errorf("preset %q has duplicate kind %q", out.Name, normalizedKind.Name)
		}
		seenKinds[normalizedKind.Name] = struct{}{}
		for _, toolName := range normalizedKind.toolNames() {
			if _, ok := seenTools[toolName]; ok {
				return Preset{}, fmt.Errorf("preset %q generated duplicate tool %q", out.Name, toolName)
			}
			seenTools[toolName] = struct{}{}
		}
		out.Kinds = append(out.Kinds, normalizedKind)
	}
	sort.Slice(out.Kinds, func(i, j int) bool { return out.Kinds[i].Name < out.Kinds[j].Name })
	return out, nil
}

func normalizePresetKind(presetName string, in PresetKind) (PresetKind, error) {
	name, err := normalizePresetIdentifier(in.Name)
	if err != nil {
		return PresetKind{}, fmt.Errorf("kind: %w", err)
	}
	namespace, err := namespaces.Normalize(in.Namespace, "")
	if err != nil {
		return PresetKind{}, fmt.Errorf("namespace: %w", err)
	}
	ops, err := normalizePresetOperations(in.Operations)
	if err != nil {
		return PresetKind{}, err
	}
	schema, err := normalizePresetSchema(in.Schema, name, 0, true)
	if err != nil {
		return PresetKind{}, err
	}
	out := PresetKind{
		Name:        name,
		Description: strings.TrimSpace(in.Description),
		Namespace:   namespace,
		Operations:  ops,
		Schema:      schema,
	}
	out.Tools = PresetGeneratedTool{
		Query:          presetKindToolName(presetName, out.Name, PresetOperationQuery),
		StatePut:       presetKindToolName(presetName, out.Name, PresetOperationStatePut),
		StateGet:       presetKindToolName(presetName, out.Name, PresetOperationStateGet),
		StateDelete:    presetKindToolName(presetName, out.Name, PresetOperationStateDelete),
		QueueEnqueue:   presetKindToolName(presetName, out.Name, PresetOperationQueueEnqueue),
		AttachmentsGet: presetKindToolName(presetName, out.Name, PresetOperationAttachmentsGet),
	}
	return out, nil
}

func normalizePresetOperations(in []PresetOperation) ([]PresetOperation, error) {
	if len(in) == 0 {
		return append([]PresetOperation(nil), defaultPresetOperations...), nil
	}
	seen := make(map[PresetOperation]struct{}, len(in))
	out := make([]PresetOperation, 0, len(in))
	for _, op := range in {
		normalized := PresetOperation(strings.ToLower(strings.TrimSpace(string(op))))
		if _, ok := validPresetOperations[normalized]; !ok {
			return nil, fmt.Errorf("unsupported operation %q", op)
		}
		if _, ok := seen[normalized]; ok {
			return nil, fmt.Errorf("duplicate operation %q", normalized)
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func normalizePresetSchema(in PresetSchema, path string, objectDepth int, isRoot bool) (PresetSchema, error) {
	typeName := strings.ToLower(strings.TrimSpace(in.Type))
	out := PresetSchema{
		Type:                 typeName,
		Description:          strings.TrimSpace(in.Description),
		AdditionalProperties: false,
	}
	switch typeName {
	case "object":
		if !isRoot {
			objectDepth++
		}
		if objectDepth > maxPresetSchemaDepth {
			return PresetSchema{}, fmt.Errorf("schema %q exceeds maximum nested object depth of %d", path, maxPresetSchemaDepth)
		}
		if len(in.Properties) == 0 {
			return PresetSchema{}, fmt.Errorf("schema %q object must define properties", path)
		}
		if in.Items != nil {
			return PresetSchema{}, fmt.Errorf("schema %q object must not define items", path)
		}
		if in.AdditionalProperties {
			return PresetSchema{}, fmt.Errorf("schema %q must set additionalProperties to false", path)
		}
		out.Properties = make(map[string]PresetSchema, len(in.Properties))
		keys := make([]string, 0, len(in.Properties))
		for key := range in.Properties {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if strings.HasPrefix(strings.ToLower(strings.TrimSpace(key)), lockdReservedFieldPref) {
				return PresetSchema{}, fmt.Errorf("schema %q property %q uses reserved %s prefix", path, key, lockdReservedFieldPref)
			}
			normalizedKey, err := normalizePresetIdentifier(key)
			if err != nil {
				return PresetSchema{}, fmt.Errorf("schema %q property %q: %w", path, key, err)
			}
			child, err := normalizePresetSchema(in.Properties[key], path+"."+normalizedKey, objectDepth, false)
			if err != nil {
				return PresetSchema{}, err
			}
			out.Properties[normalizedKey] = child
		}
		required, err := normalizeSchemaRequired(in.Required, out.Properties, path)
		if err != nil {
			return PresetSchema{}, err
		}
		out.Required = required
	case "array":
		if len(in.Properties) > 0 || len(in.Required) > 0 {
			return PresetSchema{}, fmt.Errorf("schema %q array must not define object properties", path)
		}
		if in.AdditionalProperties {
			return PresetSchema{}, fmt.Errorf("schema %q must set additionalProperties to false", path)
		}
		if in.Items == nil {
			return PresetSchema{}, fmt.Errorf("schema %q array must define items", path)
		}
		items, err := normalizePresetSchema(*in.Items, path+"[]", objectDepth, false)
		if err != nil {
			return PresetSchema{}, err
		}
		if items.Type == "array" {
			return PresetSchema{}, fmt.Errorf("schema %q does not support nested arrays", path)
		}
		out.Items = &items
	case "string", "number", "integer", "boolean":
		if len(in.Properties) > 0 || len(in.Required) > 0 || in.Items != nil {
			return PresetSchema{}, fmt.Errorf("schema %q scalar type %q cannot define properties, required, or items", path, typeName)
		}
		if in.AdditionalProperties {
			return PresetSchema{}, fmt.Errorf("schema %q must set additionalProperties to false", path)
		}
	default:
		if typeName == "" {
			return PresetSchema{}, fmt.Errorf("schema %q type is required", path)
		}
		return PresetSchema{}, fmt.Errorf("schema %q type %q is unsupported", path, typeName)
	}
	if isRoot && out.Type != "object" {
		return PresetSchema{}, fmt.Errorf("schema %q root type must be object", path)
	}
	return out, nil
}

func normalizeSchemaRequired(required []string, props map[string]PresetSchema, path string) ([]string, error) {
	if len(required) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(required))
	out := make([]string, 0, len(required))
	for _, field := range required {
		normalized, err := normalizePresetIdentifier(field)
		if err != nil {
			return nil, fmt.Errorf("schema %q required field %q: %w", path, field, err)
		}
		if _, ok := props[normalized]; !ok {
			return nil, fmt.Errorf("schema %q required field %q is not defined in properties", path, normalized)
		}
		if _, ok := seen[normalized]; ok {
			return nil, fmt.Errorf("schema %q duplicate required field %q", path, normalized)
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	sort.Strings(out)
	return out, nil
}

func normalizePresetIdentifier(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("identifier is required")
	}
	var b strings.Builder
	prevUnderscore := false
	for _, r := range strings.ToLower(raw) {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			prevUnderscore = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			prevUnderscore = false
		default:
			if !prevUnderscore {
				b.WriteByte('_')
				prevUnderscore = true
			}
		}
	}
	normalized := strings.Trim(b.String(), "_")
	if normalized == "" {
		return "", fmt.Errorf("identifier %q normalizes to empty", raw)
	}
	if strings.Contains(normalized, "__") {
		normalized = strings.ReplaceAll(normalized, "__", "_")
	}
	return normalized, nil
}

func presetHelpToolName(presetName string) string {
	return presetName + ".help"
}

func presetKindToolName(presetName, kindName string, operation PresetOperation) string {
	return presetName + "." + kindName + "." + string(operation)
}

func (k PresetKind) toolNames() []string {
	return []string{
		k.Tools.Query,
		k.Tools.StatePut,
		k.Tools.StateGet,
		k.Tools.StateDelete,
		k.Tools.QueueEnqueue,
		k.Tools.AttachmentsGet,
	}
}

func isEmptyPresetDocument(in Preset) bool {
	return strings.TrimSpace(in.Name) == "" &&
		strings.TrimSpace(in.Description) == "" &&
		len(in.Kinds) == 0
}
