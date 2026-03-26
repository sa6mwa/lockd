package preset

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
	// ReservedLockdName is the built-in preset name for the native lockd MCP surface.
	ReservedLockdName = "lockd"
	// ReservedFieldPrefix is reserved for preset metadata fields.
	ReservedFieldPrefix  = "_lockd_"
	maxSchemaObjectDepth = 1
)

// DefaultOperations is the default preset kind operation set.
var DefaultOperations = []Operation{
	OperationQuery,
	OperationStatePut,
	OperationStateGet,
	OperationStateDelete,
	OperationQueueEnqueue,
	OperationAttachmentsGet,
}

// Definition describes one custom MCP tool surface.
type Definition struct {
	Name        string `json:"name" yaml:"preset"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
	Kinds       []Kind `json:"kinds" yaml:"kinds"`
}

// Kind binds one document schema to one hidden namespace.
type Kind struct {
	Name        string        `json:"name" yaml:"name"`
	Description string        `json:"description,omitempty" yaml:"description,omitempty"`
	Namespace   string        `json:"namespace" yaml:"namespace"`
	Operations  []Operation   `json:"operations,omitempty" yaml:"operations,omitempty"`
	Schema      Schema        `json:"schema" yaml:"schema"`
	Tools       GeneratedTool `json:"tools" yaml:"-"`
}

// GeneratedTool contains canonical generated tool names for one kind.
type GeneratedTool struct {
	Query          string `json:"query"`
	StatePut       string `json:"state_put"`
	StateGet       string `json:"state_get"`
	StateDelete    string `json:"state_delete"`
	QueueEnqueue   string `json:"queue_enqueue"`
	AttachmentsGet string `json:"attachments_get"`
}

// Operation is a supported preset kind operation.
type Operation string

const (
	// OperationQuery exposes keys-only preset queries.
	OperationQuery Operation = "query"
	// OperationStatePut writes a schema-backed preset document by key.
	OperationStatePut Operation = "state.put"
	// OperationStateGet reads a schema-backed preset document by key.
	OperationStateGet Operation = "state.get"
	// OperationStateDelete removes a schema-backed preset document by key.
	OperationStateDelete Operation = "state.delete"
	// OperationQueueEnqueue enqueues a schema-backed preset payload.
	OperationQueueEnqueue Operation = "queue.enqueue"
	// OperationAttachmentsGet retrieves an attachment for a preset document.
	OperationAttachmentsGet Operation = "attachments.get"
)

var validOperations = map[Operation]struct{}{
	OperationQuery:          {},
	OperationStatePut:       {},
	OperationStateGet:       {},
	OperationStateDelete:    {},
	OperationQueueEnqueue:   {},
	OperationAttachmentsGet: {},
}

// Schema is the normalized schema subset supported by MCP presets.
type Schema struct {
	Type                 string            `json:"type" yaml:"type"`
	Description          string            `json:"description,omitempty" yaml:"description,omitempty"`
	Properties           map[string]Schema `json:"properties,omitempty" yaml:"properties,omitempty"`
	Required             []string          `json:"required,omitempty" yaml:"required,omitempty"`
	Items                *Schema           `json:"items,omitempty" yaml:"items,omitempty"`
	AdditionalProperties bool              `json:"additional_properties" yaml:"additionalProperties,omitempty"`
}

// AttachmentMetadata is the reserved preset state.get attachment shape.
type AttachmentMetadata struct {
	Name        string `json:"name"`
	ContentType string `json:"content_type,omitempty"`
	SizeBytes   int64  `json:"size_bytes,omitempty"`
	ETag        string `json:"etag,omitempty"`
}

// ParseYAML parses one or more YAML documents into normalized presets.
func ParseYAML(data []byte) ([]Definition, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	presets := make([]Definition, 0)
	docIndex := 0
	for {
		var raw Definition
		err := dec.Decode(&raw)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("parse preset yaml document %d: %w", docIndex+1, err)
		}
		if isEmptyDefinition(raw) {
			docIndex++
			continue
		}
		presets = append(presets, raw)
		docIndex++
	}
	if len(presets) == 0 {
		return nil, fmt.Errorf("preset yaml did not contain any presets")
	}
	return NormalizeCollection(presets)
}

// LoadFile reads and parses one preset YAML file.
func LoadFile(path string) ([]Definition, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read preset file %s: %w", path, err)
	}
	presets, err := ParseYAML(data)
	if err != nil {
		return nil, fmt.Errorf("parse preset file %s: %w", path, err)
	}
	return presets, nil
}

// NormalizeCollection validates and normalizes already structured presets.
func NormalizeCollection(in []Definition) ([]Definition, error) {
	if len(in) == 0 {
		return nil, fmt.Errorf("at least one preset is required")
	}
	out := make([]Definition, 0, len(in))
	seenNames := make(map[string]struct{}, len(in))
	for idx, item := range in {
		normalized, err := normalizeDefinition(item)
		if err != nil {
			return nil, fmt.Errorf("preset %d: %w", idx+1, err)
		}
		if _, ok := seenNames[normalized.Name]; ok {
			return nil, fmt.Errorf("duplicate preset %q", normalized.Name)
		}
		seenNames[normalized.Name] = struct{}{}
		out = append(out, normalized)
	}
	return out, nil
}

func normalizeDefinition(in Definition) (Definition, error) {
	name, err := normalizeIdentifier(in.Name)
	if err != nil {
		return Definition{}, fmt.Errorf("preset: %w", err)
	}
	if name == ReservedLockdName {
		return Definition{}, fmt.Errorf("preset name %q is reserved", ReservedLockdName)
	}
	out := Definition{
		Name:        name,
		Description: strings.TrimSpace(in.Description),
		Kinds:       make([]Kind, 0, len(in.Kinds)),
	}
	if len(in.Kinds) == 0 {
		return Definition{}, fmt.Errorf("preset %q must contain at least one kind", out.Name)
	}
	seenKinds := make(map[string]struct{}, len(in.Kinds))
	seenTools := map[string]struct{}{
		helpToolName(out.Name): {},
	}
	for idx, kind := range in.Kinds {
		normalizedKind, err := normalizeKind(out.Name, kind)
		if err != nil {
			return Definition{}, fmt.Errorf("preset %q kind %d: %w", out.Name, idx+1, err)
		}
		if _, ok := seenKinds[normalizedKind.Name]; ok {
			return Definition{}, fmt.Errorf("preset %q has duplicate kind %q", out.Name, normalizedKind.Name)
		}
		seenKinds[normalizedKind.Name] = struct{}{}
		for _, toolName := range normalizedKind.toolNames() {
			if _, ok := seenTools[toolName]; ok {
				return Definition{}, fmt.Errorf("preset %q generated duplicate tool %q", out.Name, toolName)
			}
			seenTools[toolName] = struct{}{}
		}
		out.Kinds = append(out.Kinds, normalizedKind)
	}
	sort.Slice(out.Kinds, func(i, j int) bool { return out.Kinds[i].Name < out.Kinds[j].Name })
	return out, nil
}

func normalizeKind(presetName string, in Kind) (Kind, error) {
	name, err := normalizeIdentifier(in.Name)
	if err != nil {
		return Kind{}, fmt.Errorf("kind: %w", err)
	}
	namespace, err := namespaces.Normalize(in.Namespace, "")
	if err != nil {
		return Kind{}, fmt.Errorf("namespace: %w", err)
	}
	ops, err := normalizeOperations(in.Operations)
	if err != nil {
		return Kind{}, err
	}
	schema, err := normalizeSchema(in.Schema, name, 0, true)
	if err != nil {
		return Kind{}, err
	}
	out := Kind{
		Name:        name,
		Description: strings.TrimSpace(in.Description),
		Namespace:   namespace,
		Operations:  ops,
		Schema:      schema,
	}
	out.Tools = GeneratedTool{
		Query:          kindToolName(presetName, out.Name, OperationQuery),
		StatePut:       kindToolName(presetName, out.Name, OperationStatePut),
		StateGet:       kindToolName(presetName, out.Name, OperationStateGet),
		StateDelete:    kindToolName(presetName, out.Name, OperationStateDelete),
		QueueEnqueue:   kindToolName(presetName, out.Name, OperationQueueEnqueue),
		AttachmentsGet: kindToolName(presetName, out.Name, OperationAttachmentsGet),
	}
	return out, nil
}

func normalizeOperations(in []Operation) ([]Operation, error) {
	if len(in) == 0 {
		return append([]Operation(nil), DefaultOperations...), nil
	}
	seen := make(map[Operation]struct{}, len(in))
	out := make([]Operation, 0, len(in))
	for _, op := range in {
		normalized := Operation(strings.ToLower(strings.TrimSpace(string(op))))
		if _, ok := validOperations[normalized]; !ok {
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

func normalizeSchema(in Schema, path string, objectDepth int, isRoot bool) (Schema, error) {
	typeName := strings.ToLower(strings.TrimSpace(in.Type))
	out := Schema{
		Type:                 typeName,
		Description:          strings.TrimSpace(in.Description),
		AdditionalProperties: false,
	}
	switch typeName {
	case "object":
		if !isRoot {
			objectDepth++
		}
		if objectDepth > maxSchemaObjectDepth {
			return Schema{}, fmt.Errorf("schema %q exceeds maximum nested object depth of %d", path, maxSchemaObjectDepth)
		}
		if len(in.Properties) == 0 {
			return Schema{}, fmt.Errorf("schema %q object must define properties", path)
		}
		if in.Items != nil {
			return Schema{}, fmt.Errorf("schema %q object must not define items", path)
		}
		if in.AdditionalProperties {
			return Schema{}, fmt.Errorf("schema %q must set additionalProperties to false", path)
		}
		out.Properties = make(map[string]Schema, len(in.Properties))
		keys := make([]string, 0, len(in.Properties))
		for key := range in.Properties {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if strings.HasPrefix(strings.ToLower(strings.TrimSpace(key)), ReservedFieldPrefix) {
				return Schema{}, fmt.Errorf("schema %q property %q uses reserved %s prefix", path, key, ReservedFieldPrefix)
			}
			normalizedKey, err := normalizeIdentifier(key)
			if err != nil {
				return Schema{}, fmt.Errorf("schema %q property %q: %w", path, key, err)
			}
			child, err := normalizeSchema(in.Properties[key], path+"."+normalizedKey, objectDepth, false)
			if err != nil {
				return Schema{}, err
			}
			out.Properties[normalizedKey] = child
		}
		required, err := normalizeRequired(in.Required, out.Properties, path)
		if err != nil {
			return Schema{}, err
		}
		out.Required = required
	case "array":
		if len(in.Properties) > 0 || len(in.Required) > 0 {
			return Schema{}, fmt.Errorf("schema %q array must not define object properties", path)
		}
		if in.AdditionalProperties {
			return Schema{}, fmt.Errorf("schema %q must set additionalProperties to false", path)
		}
		if in.Items == nil {
			return Schema{}, fmt.Errorf("schema %q array must define items", path)
		}
		items, err := normalizeSchema(*in.Items, path+"[]", objectDepth, false)
		if err != nil {
			return Schema{}, err
		}
		if items.Type == "array" {
			return Schema{}, fmt.Errorf("schema %q does not support nested arrays", path)
		}
		out.Items = &items
	case "string", "number", "integer", "boolean":
		if len(in.Properties) > 0 || len(in.Required) > 0 || in.Items != nil {
			return Schema{}, fmt.Errorf("schema %q scalar type %q cannot define properties, required, or items", path, typeName)
		}
		if in.AdditionalProperties {
			return Schema{}, fmt.Errorf("schema %q must set additionalProperties to false", path)
		}
	default:
		if typeName == "" {
			return Schema{}, fmt.Errorf("schema %q type is required", path)
		}
		return Schema{}, fmt.Errorf("schema %q type %q is unsupported", path, typeName)
	}
	if isRoot && out.Type != "object" {
		return Schema{}, fmt.Errorf("schema %q root type must be object", path)
	}
	return out, nil
}

func normalizeRequired(required []string, props map[string]Schema, path string) ([]string, error) {
	if len(required) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(required))
	out := make([]string, 0, len(required))
	for _, field := range required {
		normalized, err := normalizeIdentifier(field)
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

func normalizeIdentifier(raw string) (string, error) {
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

func helpToolName(presetName string) string {
	return presetName + ".help"
}

func kindToolName(presetName, kindName string, operation Operation) string {
	return presetName + "." + kindName + "." + string(operation)
}

func (k Kind) toolNames() []string {
	return []string{
		k.Tools.Query,
		k.Tools.StatePut,
		k.Tools.StateGet,
		k.Tools.StateDelete,
		k.Tools.QueueEnqueue,
		k.Tools.AttachmentsGet,
	}
}

func isEmptyDefinition(in Definition) bool {
	return strings.TrimSpace(in.Name) == "" &&
		strings.TrimSpace(in.Description) == "" &&
		len(in.Kinds) == 0
}
