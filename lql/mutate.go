package lql

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// MutationKind identifies the operation applied to a JSON path.
type MutationKind int

const (
	// MutationSet assigns a value to the target path.
	MutationSet MutationKind = iota
	// MutationIncrement adds/subtracts a numeric delta to the target path.
	MutationIncrement
	// MutationRemove deletes the target path.
	MutationRemove
)

// Mutation describes a parsed mutation expression.
type Mutation struct {
	Path  []string
	Kind  MutationKind
	Value any
	Delta float64
}

// ParseMutations parses CLI-style mutation expressions into Mutation structs.
// Supports dotted paths, brace shorthand (foo.bar{a=1, b=2}), rm:/time: prefixes,
// and ++/--/+=/-= increment forms.
func ParseMutations(exprs []string, now time.Time) ([]Mutation, error) {
	if len(exprs) == 0 {
		return nil, fmt.Errorf("no field mutations provided")
	}
	var out []Mutation
	for _, raw := range exprs {
		expr := strings.TrimSpace(raw)
		if expr == "" {
			continue
		}
		muts, err := parseMutationExpr(expr, now)
		if err != nil {
			return nil, err
		}
		out = append(out, muts...)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no valid field mutations parsed")
	}
	return out, nil
}

// ParseMutationsString parses a comma/newline separated LQL mutation string.
func ParseMutationsString(expr string, now time.Time) ([]Mutation, error) {
	chunks, err := splitExpressions(expr)
	if err != nil {
		return nil, err
	}
	return ParseMutations(chunks, now)
}

func parseMutationExpr(expr string, now time.Time) ([]Mutation, error) {
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
			return nil, fmt.Errorf("time-prefixed mutation cannot be combined with delete/remove (%s)", expr)
		}
		timeMode = true
		expr = strings.TrimPrefix(expr, "time:")
	}
	if removeMode {
		path := strings.TrimSpace(expr)
		if path == "" {
			return nil, fmt.Errorf("remove mutation missing key path")
		}
		pathParts, err := splitPath(path)
		if err != nil {
			return nil, err
		}
		return []Mutation{{Path: pathParts, Kind: MutationRemove}}, nil
	}
	if strings.HasSuffix(expr, "++") {
		if timeMode {
			return nil, fmt.Errorf("time-prefixed mutation does not support ++ (%s)", expr)
		}
		path := strings.TrimSpace(strings.TrimSuffix(expr, "++"))
		mut, err := buildIncrementMutation(path, 1)
		if err != nil {
			return nil, err
		}
		return []Mutation{mut}, nil
	}
	if strings.HasSuffix(expr, "--") {
		if timeMode {
			return nil, fmt.Errorf("time-prefixed mutation does not support -- (%s)", expr)
		}
		path := strings.TrimSpace(strings.TrimSuffix(expr, "--"))
		mut, err := buildIncrementMutation(path, -1)
		if err != nil {
			return nil, err
		}
		return []Mutation{mut}, nil
	}
	// Brace shorthand (foo.bar{...}) with nested expressions.
	if strings.HasSuffix(expr, "}") {
		if idx := strings.Index(expr, "{"); idx > 0 {
			pathExpr := strings.TrimSpace(expr[:idx])
			if pathExpr != "" && !strings.Contains(pathExpr, "=") {
				content := expr[idx+1 : len(expr)-1]
				subExprs, err := splitExpressions(content)
				if err != nil {
					return nil, err
				}
				if len(subExprs) == 0 {
					return nil, fmt.Errorf("brace mutation %q empty", expr)
				}
				prefix, err := splitPath(pathExpr)
				if err != nil {
					return nil, err
				}
				var muts []Mutation
				for _, sub := range subExprs {
					sub = strings.TrimSpace(sub)
					if sub == "" {
						continue
					}
					subMuts, err := parseMutationExpr(sub, now)
					if err != nil {
						return nil, err
					}
					for _, m := range subMuts {
						m.Path = append(prefix, m.Path...)
						muts = append(muts, m)
					}
				}
				if len(muts) == 0 {
					return nil, fmt.Errorf("brace mutation %q produced no expressions", expr)
				}
				return muts, nil
			}
		}
	}
	parts := strings.SplitN(expr, "=", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid mutation %q (expected key=value)", expr)
	}
	path := strings.TrimSpace(parts[0])
	if path == "" {
		return nil, fmt.Errorf("mutation %q missing key path", expr)
	}
	value := strings.TrimSpace(parts[1])
	if !timeMode {
		if len(value) > 0 && (value[0] == '+' || value[0] == '-') {
			if delta, err := strconv.ParseFloat(value, 64); err == nil {
				mut, err := buildIncrementMutation(path, delta)
				if err != nil {
					return nil, err
				}
				return []Mutation{mut}, nil
			}
		}
	}
	mut, err := buildSetMutation(path, value, timeMode, now)
	if err != nil {
		return nil, err
	}
	return []Mutation{mut}, nil
}

func buildSetMutation(path string, literal string, timeMode bool, now time.Time) (Mutation, error) {
	pathSegments, err := splitPath(path)
	if err != nil {
		return Mutation{}, err
	}
	val, err := parseValue(literal, timeMode, now)
	if err != nil {
		return Mutation{}, err
	}
	return Mutation{Path: pathSegments, Kind: MutationSet, Value: val}, nil
}

func buildIncrementMutation(path string, delta float64) (Mutation, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return Mutation{}, fmt.Errorf("increment mutation missing key path")
	}
	if delta == 0 {
		return Mutation{}, fmt.Errorf("increment mutation requires non-zero delta")
	}
	pathSegments, err := splitPath(path)
	if err != nil {
		return Mutation{}, err
	}
	return Mutation{Path: pathSegments, Kind: MutationIncrement, Delta: delta}, nil
}

func splitPath(path string) ([]string, error) {
	var parts []string
	var token strings.Builder
	inQuotes := false
	var quote rune
	escape := false
	flush := func() {
		if token.Len() == 0 {
			return
		}
		segment := strings.TrimSpace(token.String())
		token.Reset()
		if segment != "" {
			parts = append(parts, segment)
		}
	}
	for _, r := range path {
		switch {
		case escape:
			token.WriteRune(r)
			escape = false
		case inQuotes:
			switch r {
			case '\\':
				escape = true
			case quote:
				inQuotes = false
			default:
				token.WriteRune(r)
			}
		default:
			switch r {
			case '"', '\'':
				inQuotes = true
				quote = r
			case '.':
				flush()
			default:
				token.WriteRune(r)
			}
		}
	}
	if inQuotes {
		return nil, fmt.Errorf("unterminated quote in path %q", path)
	}
	flush()
	if len(parts) == 0 {
		return nil, fmt.Errorf("mutation path %q empty", path)
	}
	return parts, nil
}

func parseValue(lit string, timeMode bool, now time.Time) (any, error) {
	unquoted := strings.TrimSpace(lit)
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

// ApplyMutations mutates the provided JSON object in-place according to muts.
func ApplyMutations(doc map[string]any, muts []Mutation) error {
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

func applyMutation(doc map[string]any, mut Mutation) error {
	if len(mut.Path) == 0 {
		return fmt.Errorf("mutation has empty path")
	}
	create := mut.Kind != MutationRemove
	parent, key, err := navigate(doc, mut.Path, create)
	if err != nil {
		if mut.Kind == MutationRemove {
			return nil
		}
		return err
	}
	switch mut.Kind {
	case MutationSet:
		parent[key] = mut.Value
	case MutationIncrement:
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
	case MutationRemove:
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
	case json.Number:
		f, err := val.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
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
