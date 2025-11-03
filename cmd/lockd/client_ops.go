package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

type mutationKind int

const (
	mutationSet mutationKind = iota
	mutationIncrement
	mutationRemove
)

type mutation struct {
	Path  []string
	Kind  mutationKind
	Value any
	Delta float64
}

func parseMutations(exprs []string, now time.Time) ([]mutation, error) {
	if len(exprs) == 0 {
		return nil, fmt.Errorf("no field mutations provided")
	}
	var muts []mutation
	for _, expr := range exprs {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			continue
		}
		mut, err := parseMutation(expr, now)
		if err != nil {
			return nil, err
		}
		muts = append(muts, mut)
	}
	if len(muts) == 0 {
		return nil, fmt.Errorf("no valid field mutations parsed")
	}
	return muts, nil
}

func parseMutation(expr string, now time.Time) (mutation, error) {
	removeMode := false
	switch {
	case strings.HasPrefix(expr, "rm:"):
		removeMode = true
		expr = strings.TrimPrefix(expr, "rm:")
	case strings.HasPrefix(expr, "remove:"):
		removeMode = true
		expr = strings.TrimPrefix(expr, "remove:")
	case strings.HasPrefix(expr, "delete:"):
		removeMode = true
		expr = strings.TrimPrefix(expr, "delete:")
	case strings.HasPrefix(expr, "del:"):
		removeMode = true
		expr = strings.TrimPrefix(expr, "del:")
	}

	timeMode := false
	if strings.HasPrefix(expr, "time:") {
		if removeMode {
			return mutation{}, fmt.Errorf("time-prefixed mutation cannot be combined with delete/remove (%s)", expr)
		}
		timeMode = true
		expr = strings.TrimPrefix(expr, "time:")
	}
	if removeMode {
		path := strings.TrimSpace(expr)
		if path == "" {
			return mutation{}, fmt.Errorf("remove mutation missing key path")
		}
		return mutation{
			Path: splitPath(path),
			Kind: mutationRemove,
		}, nil
	}
	if strings.HasSuffix(expr, "++") {
		if timeMode {
			return mutation{}, fmt.Errorf("time-prefixed mutation does not support ++ (%s)", expr)
		}
		path := strings.TrimSuffix(expr, "++")
		mut, err := buildIncrementMutation(path, 1)
		if err != nil {
			return mutation{}, err
		}
		return mut, nil
	}
	if strings.HasSuffix(expr, "--") {
		if timeMode {
			return mutation{}, fmt.Errorf("time-prefixed mutation does not support -- (%s)", expr)
		}
		path := strings.TrimSuffix(expr, "--")
		mut, err := buildIncrementMutation(path, -1)
		if err != nil {
			return mutation{}, err
		}
		return mut, nil
	}
	parts := strings.SplitN(expr, "=", 2)
	if len(parts) != 2 {
		return mutation{}, fmt.Errorf("invalid mutation %q (expected key=value)", expr)
	}
	path := strings.TrimSpace(parts[0])
	if path == "" {
		return mutation{}, fmt.Errorf("mutation %q missing key path", expr)
	}
	value := strings.TrimSpace(parts[1])
	if !timeMode {
		if len(value) > 0 && (value[0] == '+' || value[0] == '-') {
			if _, err := strconv.ParseFloat(value, 64); err == nil {
				delta, _ := strconv.ParseFloat(value, 64)
				return buildIncrementMutation(path, delta)
			}
		}
	}
	val, err := parseValue(value, timeMode, now)
	if err != nil {
		return mutation{}, err
	}
	return mutation{
		Path:  splitPath(path),
		Kind:  mutationSet,
		Value: val,
	}, nil
}

func buildIncrementMutation(path string, delta float64) (mutation, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return mutation{}, fmt.Errorf("increment mutation missing key path")
	}
	if delta == 0 {
		return mutation{}, fmt.Errorf("increment mutation requires non-zero delta")
	}
	return mutation{
		Path:  splitPath(path),
		Kind:  mutationIncrement,
		Delta: delta,
	}, nil
}

func splitPath(path string) []string {
	parts := strings.Split(path, ".")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func parseValue(lit string, timeMode bool, now time.Time) (any, error) {
	unquoted := strings.Trim(lit, " ")
	if len(unquoted) >= 2 && ((unquoted[0] == '"' && unquoted[len(unquoted)-1] == '"') || (unquoted[0] == '\'' && unquoted[len(unquoted)-1] == '\'')) {
		unquoted = unquoted[1 : len(unquoted)-1]
	}
	if timeMode {
		if strings.EqualFold(unquoted, "NOW") {
			return now.UTC().Format(time.RFC3339Nano), nil
		}
		if t, err := time.Parse(time.RFC3339Nano, unquoted); err == nil {
			return t.UTC().Format(time.RFC3339Nano), nil
		}
		if t, err := time.Parse(time.RFC3339, unquoted); err == nil {
			return t.UTC().Format(time.RFC3339Nano), nil
		}
		return nil, fmt.Errorf("invalid time literal %q", lit)
	}
	if strings.EqualFold(unquoted, "true") {
		return true, nil
	}
	if strings.EqualFold(unquoted, "false") {
		return false, nil
	}
	if strings.EqualFold(unquoted, "null") {
		return nil, nil
	}
	if i, err := strconv.ParseInt(unquoted, 10, 64); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(unquoted, 64); err == nil {
		return f, nil
	}
	return unquoted, nil
}

func applyMutations(doc map[string]any, muts []mutation) error {
	if doc == nil {
		return fmt.Errorf("document must be an object")
	}
	for _, mut := range muts {
		if err := applyMutation(doc, mut); err != nil {
			return err
		}
	}
	return nil
}

func applyMutation(doc map[string]any, mut mutation) error {
	if len(mut.Path) == 0 {
		return fmt.Errorf("mutation has empty path")
	}
	create := mut.Kind != mutationRemove
	parent, key, err := navigate(doc, mut.Path, create)
	if err != nil {
		if mut.Kind == mutationRemove {
			return nil
		}
		return err
	}
	switch mut.Kind {
	case mutationSet:
		parent[key] = mut.Value
	case mutationIncrement:
		existing, ok := parent[key]
		if !ok {
			parent[key] = normalizeNumber(mut.Delta)
			return nil
		}
		num, ok := toFloat(existing)
		if !ok {
			return fmt.Errorf("value at %s is not numeric", strings.Join(mut.Path, "."))
		}
		parent[key] = normalizeNumber(num + mut.Delta)
	case mutationRemove:
		if parent != nil {
			delete(parent, key)
		}
	default:
		return fmt.Errorf("unknown mutation kind")
	}
	return nil
}

func navigate(root map[string]any, path []string, create bool) (map[string]any, string, error) {
	if len(path) == 0 {
		return nil, "", fmt.Errorf("empty path")
	}
	current := root
	for i := 0; i < len(path)-1; i++ {
		key := path[i]
		next, ok := current[key]
		if !ok {
			if !create {
				return nil, "", fmt.Errorf("path %s does not exist", strings.Join(path[:i+1], "."))
			}
			child := make(map[string]any)
			current[key] = child
			current = child
			continue
		}
		child, ok := next.(map[string]any)
		if !ok {
			if !create {
				return nil, "", fmt.Errorf("path %s is not an object", strings.Join(path[:i+1], "."))
			}
			child = make(map[string]any)
			current[key] = child
		}
		current = child
	}
	return current, path[len(path)-1], nil
}

func toFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case float32:
		return float64(val), true
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case json.Number:
		f, err := val.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

func normalizeNumber(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return f
	}
	if math.Abs(f-math.Round(f)) < 1e-9 {
		return int64(math.Round(f))
	}
	return f
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
