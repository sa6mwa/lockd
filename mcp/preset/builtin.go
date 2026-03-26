package preset

import (
	"bytes"
	"fmt"
	"sort"

	"gopkg.in/yaml.v3"
)

var builtInDefinitions = mustBuiltInDefinitions()

// BuiltInNames returns the sorted built-in preset template names.
func BuiltInNames() []string {
	names := make([]string, 0, len(builtInDefinitions))
	for name := range builtInDefinitions {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// BuiltInDefinition returns one normalized built-in preset template.
func BuiltInDefinition(name string) (Definition, bool) {
	def, ok := builtInDefinitions[name]
	if !ok {
		return Definition{}, false
	}
	out, err := NormalizeCollection([]Definition{def})
	if err != nil {
		panic(err)
	}
	return out[0], true
}

// BuiltInYAML renders one built-in preset as YAML.
func BuiltInYAML(name string) ([]byte, error) {
	def, ok := BuiltInDefinition(name)
	if !ok {
		return nil, fmt.Errorf("unknown built-in preset %q", name)
	}
	out, err := yaml.Marshal(def)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// AllBuiltInYAML renders all built-in presets into a multi-document YAML file.
func AllBuiltInYAML() ([]byte, error) {
	var buf bytes.Buffer
	names := BuiltInNames()
	for i, name := range names {
		out, err := BuiltInYAML(name)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			buf.WriteString("---\n")
		}
		buf.Write(out)
	}
	return buf.Bytes(), nil
}

func mustBuiltInDefinitions() map[string]Definition {
	defs, err := NormalizeCollection([]Definition{
		{
			Name:        "memory",
			Description: "Shared AI memory and recall documents.",
			Kinds: []Kind{
				{
					Name:      "memory",
					Namespace: "memory",
					Schema: Schema{
						Type: "object",
						Properties: map[string]Schema{
							"text": {Type: "string", Description: "Primary memory text."},
							"tags": {
								Type:  "array",
								Items: &Schema{Type: "string"},
							},
							"keywords": {
								Type:  "array",
								Items: &Schema{Type: "string"},
							},
						},
						Required: []string{"text"},
					},
				},
				{
					Name:      "bookmarks",
					Namespace: "memory",
					Schema: Schema{
						Type: "object",
						Properties: map[string]Schema{
							"title":   {Type: "string"},
							"summary": {Type: "string"},
							"url":     {Type: "string"},
							"tags":    {Type: "array", Items: &Schema{Type: "string"}},
						},
						Required: []string{"title", "url"},
					},
				},
				{
					Name:      "contacts",
					Namespace: "memory",
					Schema: Schema{
						Type: "object",
						Properties: map[string]Schema{
							"name":    {Type: "string"},
							"company": {Type: "string"},
							"phone":   {Type: "array", Items: &Schema{Type: "string"}},
							"email":   {Type: "array", Items: &Schema{Type: "string"}},
							"url":     {Type: "array", Items: &Schema{Type: "string"}},
							"tags":    {Type: "array", Items: &Schema{Type: "string"}},
						},
						Required: []string{"name"},
					},
				},
				{
					Name:      "reminders",
					Namespace: "memory",
					Schema: Schema{
						Type: "object",
						Properties: map[string]Schema{
							"title":   {Type: "string"},
							"summary": {Type: "string"},
							"time":    {Type: "string"},
							"tags":    {Type: "array", Items: &Schema{Type: "string"}},
						},
						Required: []string{"title", "time"},
					},
				},
				{
					Name:      "todo",
					Namespace: "memory",
					Schema: Schema{
						Type: "object",
						Properties: map[string]Schema{
							"title":   {Type: "string"},
							"summary": {Type: "string"},
							"tags":    {Type: "array", Items: &Schema{Type: "string"}},
						},
						Required: []string{"title"},
					},
				},
			},
		},
	})
	if err != nil {
		panic(err)
	}
	out := make(map[string]Definition, len(defs))
	for _, def := range defs {
		out[def.Name] = def
	}
	return out
}
