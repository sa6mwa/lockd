package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"pkt.systems/lql"

	"gopkg.in/yaml.v2"
)

func parseMutations(exprs []string, now time.Time) ([]lql.Mutation, error) {
	return lql.ParseMutations(exprs, now)
}

func applyMutations(doc map[string]any, muts []lql.Mutation) error {
	return lql.ApplyMutations(doc, muts)
}

func parseJSONObject(data []byte) (map[string]any, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return map[string]any{}, nil
	}
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return doc, nil
}

func marshalCompactJSON(doc map[string]any) ([]byte, error) {
	return json.Marshal(doc)
}

func marshalPrettyJSON(doc map[string]any) ([]byte, error) {
	return json.MarshalIndent(doc, "", "  ")
}

func convertYAMLToJSON(path string, data []byte) ([]byte, error) {
	var doc any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse yaml %s: %w", path, err)
	}
	return json.Marshal(yamlToJSON(doc))
}

func convertJSONToYAML(data []byte) ([]byte, error) {
	var doc any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return yaml.Marshal(doc)
}

func readInput(path string) ([]byte, os.FileMode, error) {
	if path == "-" {
		data, err := io.ReadAll(os.Stdin)
		return data, 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, 0, err
	}
	data, err := os.ReadFile(path)
	return data, info.Mode(), err
}

func writeOutputFile(path string, data []byte, mode os.FileMode) error {
	if path == "-" {
		_, err := os.Stdout.Write(data)
		return err
	}
	if mode == 0 {
		mode = 0o600
	}
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, mode); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
}

func formatExt(path string) string {
	return strings.ToLower(filepath.Ext(path))
}

func yamlToJSON(v any) any {
	switch val := v.(type) {
	case map[any]any:
		out := make(map[string]any, len(val))
		for k, v := range val {
			key := fmt.Sprint(k)
			out[key] = yamlToJSON(v)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(val))
		for k, v := range val {
			out[k] = yamlToJSON(v)
		}
		return out
	case []any:
		slice := make([]any, len(val))
		for i, elem := range val {
			slice[i] = yamlToJSON(elem)
		}
		return slice
	default:
		return val
	}
}
