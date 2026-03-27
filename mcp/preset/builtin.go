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
			Description: "Shared AI memory and recall documents for notes, bookmarks, contacts, reminders, and todos.",
			Kinds: []Kind{
				{
					Name:        "memory",
					Description: "Free-form memory entries for recall, facts, notes, and snippets of durable context.",
					Namespace:   "memory",
					Schema: Schema{
						Type: "object",
						Properties: map[string]Schema{
							"text": {Type: "string", Description: "Primary memory text to retain and retrieve later."},
							"tags": {
								Type:        "array",
								Description: "Top-level tags used for grouping and retrieval filters.",
								Items:       &Schema{Type: "string"},
							},
							"keywords": {
								Type:        "array",
								Description: "Short keyword hints that improve recall and targeted lookup.",
								Items:       &Schema{Type: "string"},
							},
						},
						Required: []string{"text"},
					},
				},
				{
					Name:        "bookmarks",
					Description: "Saved URLs and references with titles, summaries, and retrieval tags.",
					Namespace:   "memory",
					Schema: Schema{
						Type: "object",
						Properties: map[string]Schema{
							"title":   {Type: "string", Description: "Human-readable bookmark title."},
							"summary": {Type: "string", Description: "Short note explaining why the bookmark matters."},
							"url":     {Type: "string", Description: "Canonical bookmark URL."},
							"tags": {
								Type:        "array",
								Description: "Top-level tags used for grouping and retrieval filters.",
								Items:       &Schema{Type: "string"},
							},
						},
						Required: []string{"title", "url"},
					},
				},
				{
					Name:        "contacts",
					Description: "People or organization contacts with communication details and tags.",
					Namespace:   "memory",
					Schema: Schema{
						Type: "object",
						Properties: map[string]Schema{
							"name":    {Type: "string", Description: "Primary person or organization name."},
							"company": {Type: "string", Description: "Associated company or organization."},
							"phone": {
								Type:        "array",
								Description: "Known phone numbers.",
								Items:       &Schema{Type: "string"},
							},
							"email": {
								Type:        "array",
								Description: "Known email addresses.",
								Items:       &Schema{Type: "string"},
							},
							"url": {
								Type:        "array",
								Description: "Relevant profile or website URLs.",
								Items:       &Schema{Type: "string"},
							},
							"tags": {
								Type:        "array",
								Description: "Top-level tags used for grouping and retrieval filters.",
								Items:       &Schema{Type: "string"},
							},
						},
						Required: []string{"name"},
					},
				},
				{
					Name:        "reminders",
					Description: "Time-bound reminders and future follow-up items.",
					Namespace:   "memory",
					Schema: Schema{
						Type: "object",
						Properties: map[string]Schema{
							"title":   {Type: "string", Description: "Reminder title."},
							"summary": {Type: "string", Description: "Optional details about the reminder."},
							"time":    {Type: "string", Description: "Reminder time, date, or datetime string."},
							"tags": {
								Type:        "array",
								Description: "Top-level tags used for grouping and retrieval filters.",
								Items:       &Schema{Type: "string"},
							},
						},
						Required: []string{"title", "time"},
					},
				},
				{
					Name:        "todo",
					Description: "Action items and task tracking entries.",
					Namespace:   "memory",
					Schema: Schema{
						Type: "object",
						Properties: map[string]Schema{
							"title":   {Type: "string", Description: "Task title."},
							"summary": {Type: "string", Description: "Optional task details or acceptance notes."},
							"tags": {
								Type:        "array",
								Description: "Top-level tags used for grouping and retrieval filters.",
								Items:       &Schema{Type: "string"},
							},
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
