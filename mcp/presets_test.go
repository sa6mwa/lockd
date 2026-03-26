package mcp

import (
	"reflect"
	"strings"
	"testing"
)

func TestParsePresetYAMLNormalizesMultiDocumentPresetFile(t *testing.T) {
	t.Parallel()

	input := `
preset: Memory Vault
description: Shared memory tools
kinds:
  - name: Notes
    namespace: Team-A
    schema:
      type: object
      properties:
        text:
          type: string
        tags:
          type: array
          items:
            type: string
        meta:
          type: object
          properties:
            source:
              type: string
      required:
        - text
---
preset: crm
kinds:
  - name: contacts
    namespace: crm
    operations:
      - state.get
      - query
    schema:
      type: object
      properties:
        name:
          type: string
`
	presets, err := ParsePresetYAML([]byte(input))
	if err != nil {
		t.Fatalf("ParsePresetYAML: %v", err)
	}
	if len(presets) != 2 {
		t.Fatalf("len(presets)=%d want 2", len(presets))
	}

	first := presets[0]
	if first.Name != "memory_vault" {
		t.Fatalf("first.Name=%q want memory_vault", first.Name)
	}
	if len(first.Kinds) != 1 {
		t.Fatalf("len(first.Kinds)=%d want 1", len(first.Kinds))
	}
	kind := first.Kinds[0]
	if kind.Name != "notes" {
		t.Fatalf("kind.Name=%q want notes", kind.Name)
	}
	if kind.Namespace != "team-a" {
		t.Fatalf("kind.Namespace=%q want team-a", kind.Namespace)
	}
	if kind.Tools.StatePut != "memory_vault.notes.state.put" {
		t.Fatalf("kind.Tools.StatePut=%q", kind.Tools.StatePut)
	}
	if !reflect.DeepEqual(kind.Operations, defaultPresetOperations) {
		t.Fatalf("kind.Operations=%v want %v", kind.Operations, defaultPresetOperations)
	}
	if !reflect.DeepEqual(kind.Schema.Required, []string{"text"}) {
		t.Fatalf("kind.Schema.Required=%v want [text]", kind.Schema.Required)
	}
	if _, ok := kind.Schema.Properties["meta"]; !ok {
		t.Fatalf("expected normalized meta property")
	}

	second := presets[1]
	if second.Name != "crm" {
		t.Fatalf("second.Name=%q want crm", second.Name)
	}
	if !reflect.DeepEqual(second.Kinds[0].Operations, []PresetOperation{
		PresetOperationQuery,
		PresetOperationStateGet,
	}) {
		t.Fatalf("second ops=%v", second.Kinds[0].Operations)
	}
}

func TestParsePresetYAMLRejectsReservedPresetName(t *testing.T) {
	t.Parallel()

	_, err := ParsePresetYAML([]byte(`
preset: lockd
kinds:
  - name: note
    namespace: default
    schema:
      type: object
      properties:
        text:
          type: string
`))
	if err == nil || !strings.Contains(err.Error(), `preset name "lockd" is reserved`) {
		t.Fatalf("err=%v want reserved preset name error", err)
	}
}

func TestParsePresetYAMLRejectsReservedFieldPrefix(t *testing.T) {
	t.Parallel()

	_, err := ParsePresetYAML([]byte(`
preset: memory
kinds:
  - name: note
    namespace: default
    schema:
      type: object
      properties:
        _lockd_key:
          type: string
`))
	if err == nil || !strings.Contains(err.Error(), "uses reserved _lockd_ prefix") {
		t.Fatalf("err=%v want reserved field prefix error", err)
	}
}

func TestParsePresetYAMLRejectsAdditionalPropertiesTrue(t *testing.T) {
	t.Parallel()

	_, err := ParsePresetYAML([]byte(`
preset: memory
kinds:
  - name: note
    namespace: default
    schema:
      type: object
      additionalProperties: true
      properties:
        text:
          type: string
`))
	if err == nil || !strings.Contains(err.Error(), "must set additionalProperties to false") {
		t.Fatalf("err=%v want additionalProperties error", err)
	}
}

func TestParsePresetYAMLRejectsNestedObjectBeyondLimit(t *testing.T) {
	t.Parallel()

	_, err := ParsePresetYAML([]byte(`
preset: memory
kinds:
  - name: note
    namespace: default
    schema:
      type: object
      properties:
        level1:
          type: object
          properties:
            level2:
              type: object
              properties:
                text:
                  type: string
`))
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum nested object depth") {
		t.Fatalf("err=%v want nested depth error", err)
	}
}

func TestParsePresetYAMLRejectsUnsupportedOperation(t *testing.T) {
	t.Parallel()

	_, err := ParsePresetYAML([]byte(`
preset: memory
kinds:
  - name: note
    namespace: default
    operations:
      - state.update
    schema:
      type: object
      properties:
        text:
          type: string
`))
	if err == nil || !strings.Contains(err.Error(), `unsupported operation "state.update"`) {
		t.Fatalf("err=%v want unsupported operation error", err)
	}
}

func TestParsePresetYAMLRejectsRequiredFieldMissingFromProperties(t *testing.T) {
	t.Parallel()

	_, err := ParsePresetYAML([]byte(`
preset: memory
kinds:
  - name: note
    namespace: default
    schema:
      type: object
      properties:
        text:
          type: string
      required:
        - title
`))
	if err == nil || !strings.Contains(err.Error(), `required field "title" is not defined in properties`) {
		t.Fatalf("err=%v want required field error", err)
	}
}

func TestParsePresetYAMLRejectsNestedArray(t *testing.T) {
	t.Parallel()

	_, err := ParsePresetYAML([]byte(`
preset: memory
kinds:
  - name: note
    namespace: default
    schema:
      type: object
      properties:
        tags:
          type: array
          items:
            type: array
            items:
              type: string
`))
	if err == nil || !strings.Contains(err.Error(), "does not support nested arrays") {
		t.Fatalf("err=%v want nested array error", err)
	}
}
