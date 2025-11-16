package lql

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"pkt.systems/lockd/api"
)

var selectorRoots = map[string]struct{}{
	"and":    {},
	"or":     {},
	"not":    {},
	"eq":     {},
	"prefix": {},
	"range":  {},
	"in":     {},
	"exists": {},
}

var aggregatorTokens = map[string]struct{}{
	"and": {},
	"or":  {},
}

const maxSelectorIndex = 1 << 20

// ParseSelectorValues scans URL parameters for selector expressions and returns the AST.
func ParseSelectorValues(values url.Values) (api.Selector, error) {
	selector, found, err := parseSelectorValuesInternal(values)
	if err != nil {
		return api.Selector{}, err
	}
	if !found {
		return api.Selector{}, nil
	}
	return selector, nil
}

// ParseSelectorString parses a comma/newline separated selector expression string (LQL).
func ParseSelectorString(expr string) (api.Selector, error) {
	tokens, err := splitExpressions(expr)
	if err != nil {
		return api.Selector{}, err
	}
	if len(tokens) == 0 {
		return api.Selector{}, nil
	}
	values := url.Values{}
	for _, token := range tokens {
		rewritten, ok, err := rewriteShorthandExpression(token)
		if err != nil {
			return api.Selector{}, err
		}
		if ok {
			values.Add(rewritten, "")
			continue
		}
		values.Add(token, "")
	}
	return ParseSelectorValues(values)
}

func rewriteShorthandExpression(expr string) (string, bool, error) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return "", false, nil
	}
	if firstSelectorToken(trimmed) != "" {
		return "", false, nil
	}
	field, op, value, ok, err := parseShorthandComponents(trimmed)
	if err != nil || !ok {
		return "", ok && err == nil, err
	}
	forceNumeric := op == ">" || op == ">=" || op == "<" || op == "<="
	literal, err := normalizeShorthandValue(value, forceNumeric)
	if err != nil {
		return "", false, err
	}
	switch op {
	case "=", "==":
		return fmt.Sprintf("eq{field=%s,value=%s}", field, literal), true, nil
	case "!=":
		return fmt.Sprintf("not.eq{field=%s,value=%s}", field, literal), true, nil
	case ">":
		return fmt.Sprintf("range{field=%s,gt=%s}", field, literal), true, nil
	case ">=":
		return fmt.Sprintf("range{field=%s,gte=%s}", field, literal), true, nil
	case "<":
		return fmt.Sprintf("range{field=%s,lt=%s}", field, literal), true, nil
	case "<=":
		return fmt.Sprintf("range{field=%s,lte=%s}", field, literal), true, nil
	default:
		return "", false, nil
	}
}

func parseShorthandComponents(expr string) (field, op, value string, matched bool, err error) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return "", "", "", false, nil
	}
	inQuotes := false
	var quote rune
	for i, r := range trimmed {
		switch r {
		case '\'', '"':
			if !inQuotes {
				inQuotes = true
				quote = r
			} else if quote == r {
				inQuotes = false
				quote = 0
			}
		default:
		}
		if inQuotes {
			continue
		}
		if i+1 < len(trimmed) {
			candidate := trimmed[i : i+2]
			if isShorthandOperator(candidate) {
				fieldPart := strings.TrimSpace(trimmed[:i])
				valuePart := trimmed[i+2:]
				return fieldPart, candidate, valuePart, true, validateShorthandParts(expr, fieldPart, valuePart)
			}
		}
		if isSingleOperator(r) {
			fieldPart := strings.TrimSpace(trimmed[:i])
			valuePart := trimmed[i+1:]
			return fieldPart, string(r), valuePart, true, validateShorthandParts(expr, fieldPart, valuePart)
		}
	}
	return "", "", "", false, nil
}

func validateShorthandParts(expr, field, value string) error {
	field = strings.TrimSpace(field)
	value = strings.TrimSpace(value)
	if field == "" {
		return fmt.Errorf("selector shorthand %q missing field", expr)
	}
	if !strings.HasPrefix(field, "/") {
		return fmt.Errorf("selector shorthand %q requires JSON Pointer field", expr)
	}
	if value == "" {
		return fmt.Errorf("selector shorthand %q missing value", expr)
	}
	return nil
}

func isShorthandOperator(op string) bool {
	switch op {
	case "!=", ">=", "<=", "==":
		return true
	default:
		return false
	}
}

func isSingleOperator(r rune) bool {
	switch r {
	case '=', '>', '<':
		return true
	default:
		return false
	}
}

func normalizeShorthandValue(raw string, numericOnly bool) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("selector shorthand missing value")
	}
	if _, quoted := stripQuotes(trimmed); quoted {
		if numericOnly {
			return "", fmt.Errorf("range comparisons require numeric literals")
		}
		return trimmed, nil
	}
	if _, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return trimmed, nil
	}
	lowered := strings.ToLower(trimmed)
	if lowered == "true" || lowered == "false" {
		if numericOnly {
			return "", fmt.Errorf("range comparisons require numeric literals")
		}
		return lowered, nil
	}
	if numericOnly {
		return "", fmt.Errorf("range comparisons require numeric literals")
	}
	buf, err := json.Marshal(trimmed)
	if err != nil {
		return fmt.Sprintf("\"%s\"", trimmed), nil
	}
	return string(buf), nil
}

func parseSelectorValuesInternal(values url.Values) (api.Selector, bool, error) {
	builder := newSelectorBuilder()
	found := false
	for rawKey, vals := range values {
		key := strings.TrimSpace(rawKey)
		if key == "" {
			continue
		}
		first := firstSelectorToken(key)
		if first == "" {
			continue
		}
		tokens, inline, err := parseSelectorKey(key)
		if err != nil {
			return api.Selector{}, false, err
		}
		if len(tokens) == 0 {
			continue
		}
		found = true
		if inline != nil {
			if err := builder.assignInline(tokens, inline); err != nil {
				return api.Selector{}, false, err
			}
			continue
		}
		value := ""
		if len(vals) > 0 {
			value = vals[len(vals)-1]
		}
		if err := builder.assign(tokens, nil, value); err != nil {
			return api.Selector{}, false, err
		}
	}
	if !found {
		return api.Selector{}, false, nil
	}
	return builder.build()
}

func firstSelectorToken(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	token := trimmed
	if idx := strings.IndexAny(trimmed, ".{"); idx >= 0 {
		token = trimmed[:idx]
	}
	token = strings.ToLower(strings.TrimSpace(token))
	if _, ok := selectorRoots[token]; ok {
		return token
	}
	return ""
}

func parseSelectorKey(key string) ([]string, map[string]string, error) {
	raw := strings.TrimSpace(key)
	var inline map[string]string
	if start := strings.Index(raw, "{"); start >= 0 {
		if !strings.HasSuffix(raw, "}") || start == len(raw)-1 {
			return nil, nil, fmt.Errorf("selector %q missing closing brace", key)
		}
		content := raw[start+1 : len(raw)-1]
		assignments, err := parseInlineAssignments(content)
		if err != nil {
			return nil, nil, err
		}
		inline = assignments
		raw = strings.TrimSpace(raw[:start])
	}
	tokens := splitTokens(raw)
	if len(tokens) == 0 {
		return nil, nil, fmt.Errorf("selector path %q empty", key)
	}
	return tokens, inline, nil
}

func splitTokens(raw string) []string {
	parts := strings.Split(raw, ".")
	tokens := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		lower := strings.ToLower(trimmed)
		if _, ok := selectorRoots[lower]; ok {
			tokens = append(tokens, lower)
		} else {
			tokens = append(tokens, trimmed)
		}
	}
	return tokens
}

func splitExpressions(input string) ([]string, error) {
	var expressions []string
	var chunk strings.Builder
	depth := 0
	inQuotes := false
	escape := false
	flush := func() {
		if chunk.Len() == 0 {
			return
		}
		expressions = append(expressions, strings.TrimSpace(chunk.String()))
		chunk.Reset()
	}
	for _, r := range input {
		switch {
		case escape:
			chunk.WriteRune(r)
			escape = false
		case r == '\\':
			escape = true
			chunk.WriteRune(r)
		case r == '"':
			inQuotes = !inQuotes
			chunk.WriteRune(r)
		case r == '{' && !inQuotes:
			depth++
			chunk.WriteRune(r)
		case r == '}' && !inQuotes:
			if depth == 0 {
				return nil, fmt.Errorf("unexpected closing brace")
			}
			depth--
			chunk.WriteRune(r)
		case (r == ',' || r == '\n') && !inQuotes && depth == 0:
			flush()
		default:
			chunk.WriteRune(r)
		}
	}
	if inQuotes {
		return nil, fmt.Errorf("unterminated quote in selector expression")
	}
	if depth != 0 {
		return nil, fmt.Errorf("unterminated brace in selector expression")
	}
	flush()
	cleaned := make([]string, 0, len(expressions))
	for _, expr := range expressions {
		if expr != "" {
			cleaned = append(cleaned, expr)
		}
	}
	return cleaned, nil
}

func parseInlineAssignments(raw string) (map[string]string, error) {
	tokens, err := splitAssignmentTokens(raw)
	if err != nil {
		return nil, err
	}
	assignments := make(map[string]string, len(tokens))
	for _, token := range tokens {
		parts := strings.SplitN(token, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid selector expression %q", token)
		}
		key := strings.TrimSpace(parts[0])
		if unquoted, ok := stripQuotes(key); ok {
			key = unquoted
		}
		if key == "" {
			return nil, fmt.Errorf("selector expression %q missing key", token)
		}
		value := strings.TrimSpace(parts[1])
		assignments[key] = value
	}
	if len(assignments) == 0 {
		return nil, fmt.Errorf("empty selector expression")
	}
	return assignments, nil
}

func splitAssignmentTokens(input string) ([]string, error) {
	var tokens []string
	var chunk strings.Builder
	inQuotes := false
	escape := false
	seenEqual := false
	valueStarted := false
	flush := func() error {
		if chunk.Len() == 0 {
			return nil
		}
		if !seenEqual {
			return fmt.Errorf("invalid selector expression %q", chunk.String())
		}
		token := strings.TrimSpace(chunk.String())
		if token != "" {
			tokens = append(tokens, token)
		}
		chunk.Reset()
		seenEqual = false
		valueStarted = false
		return nil
	}
	for _, r := range input {
		switch {
		case escape:
			chunk.WriteRune(r)
			escape = false
		case r == '\\':
			escape = true
			chunk.WriteRune(r)
		case r == '"':
			inQuotes = !inQuotes
			if seenEqual {
				valueStarted = true
			}
			chunk.WriteRune(r)
		case !inQuotes && (r == ',' || r == '\n' || r == '\r'):
			if err := flush(); err != nil {
				return nil, err
			}
		case !inQuotes && (r == ' ' || r == '\t'):
			if seenEqual && valueStarted {
				if err := flush(); err != nil {
					return nil, err
				}
			} else if chunk.Len() > 0 {
				chunk.WriteRune(r)
			}
		default:
			if r == '=' && !inQuotes {
				seenEqual = true
				valueStarted = false
			} else if seenEqual && !valueStarted && !unicode.IsSpace(r) {
				valueStarted = true
			}
			chunk.WriteRune(r)
		}
	}
	if inQuotes {
		return nil, fmt.Errorf("unterminated quote in selector expression")
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return tokens, nil
}

// selectorBuilder routes assignments to the base clause or dedicated OR clauses.
type selectorBuilder struct {
	base        *clauseBuilder
	orClauses   []*clauseBuilder
	orKeyed     map[string]*clauseBuilder
	activeOrKey string
	activeOr    *clauseBuilder
}

func newSelectorBuilder() *selectorBuilder {
	return &selectorBuilder{base: newClauseBuilder(), orKeyed: make(map[string]*clauseBuilder)}
}

func (b *selectorBuilder) assignInline(tokens []string, fields map[string]string) error {
	if len(fields) == 0 {
		return fmt.Errorf("empty selector expression")
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(tokens) > 0 && tokens[0] == "or" {
		clauseTokens, clauseKey := b.normalizeOrPath(tokens)
		if len(clauseTokens) == 0 {
			return fmt.Errorf("selector or requires child")
		}
		clause := b.orClauseFor(clauseKey)
		if sticky := aggregatorKey(clauseTokens); sticky != "" {
			clause.beginSticky(sticky)
			defer clause.endSticky()
		}
		for _, field := range keys {
			path := append(clauseTokens, field)
			if err := clause.assign(path, convertSelectorValue(fields[field])); err != nil {
				return err
			}
		}
		return nil
	}
	if sticky := aggregatorKey(tokens); sticky != "" {
		b.base.beginSticky(sticky)
		defer b.base.endSticky()
	}
	for _, field := range keys {
		path := append(tokens, field)
		if err := b.base.assign(path, convertSelectorValue(fields[field])); err != nil {
			return err
		}
	}
	return nil
}

func (b *selectorBuilder) assign(tokens []string, tail []string, rawValue string) error {
	fullPath := append(append([]string(nil), tokens...), tail...)
	if len(fullPath) == 0 {
		return fmt.Errorf("selector path empty")
	}
	value := convertSelectorValue(rawValue)
	if fullPath[0] == "or" {
		clauseTokens, clauseKey := b.normalizeOrPath(fullPath)
		if len(clauseTokens) == 0 {
			return fmt.Errorf("selector or requires child")
		}
		clause := b.orClauseFor(clauseKey)
		return clause.assign(clauseTokens, value)
	}
	b.activeOr = nil
	b.activeOrKey = ""
	return b.base.assign(fullPath, value)
}

func (b *selectorBuilder) normalizeOrPath(path []string) ([]string, string) {
	tokens := append([]string(nil), path[1:]...)
	clauseKey := "or"
	if len(tokens) > 0 && looksLikeIndex(tokens[0]) {
		clauseKey = clauseKey + "." + tokens[0]
		tokens = tokens[1:]
	}
	return tokens, clauseKey
}

func (b *selectorBuilder) orClauseFor(key string) *clauseBuilder {
	if key != "or" {
		if clause, ok := b.orKeyed[key]; ok {
			return clause
		}
		clause := newClauseBuilder()
		b.orKeyed[key] = clause
		b.orClauses = append(b.orClauses, clause)
		return clause
	}
	if b.activeOr != nil && b.activeOrKey == key {
		return b.activeOr
	}
	clause := newClauseBuilder()
	b.orClauses = append(b.orClauses, clause)
	b.activeOr = clause
	b.activeOrKey = key
	return clause
}

func (b *selectorBuilder) build() (api.Selector, bool, error) {
	nodes := make([]any, 0, len(b.orClauses)+1)
	if !isEmptyMap(b.base.root) {
		nodes = append(nodes, b.base.root)
	}
	for _, clause := range b.orClauses {
		if !isEmptyMap(clause.root) {
			nodes = append(nodes, clause.root)
		}
	}
	var payload []byte
	var err error
	switch len(nodes) {
	case 0:
		payload, err = json.Marshal(b.base.root)
	case 1:
		payload, err = json.Marshal(nodes[0])
	default:
		payload, err = json.Marshal(map[string]any{"or": nodes})
	}
	if err != nil {
		return api.Selector{}, false, err
	}
	if len(payload) == 0 || string(payload) == "null" {
		return api.Selector{}, false, nil
	}
	var selector api.Selector
	if err := json.Unmarshal(payload, &selector); err != nil {
		return api.Selector{}, false, err
	}
	return selector, true, nil
}

func aggregatorKey(tokens []string) string {
	parts := make([]string, 0, len(tokens))
	for _, token := range tokens {
		trimmed := strings.TrimSpace(token)
		if trimmed == "" {
			continue
		}
		lower := strings.ToLower(trimmed)
		if looksLikeIndex(lower) {
			continue
		}
		parts = append(parts, lower)
		if _, ok := aggregatorTokens[lower]; ok {
			return strings.Join(parts, ".")
		}
	}
	return ""
}

func isEmptyMap(node any) bool {
	if node == nil {
		return true
	}
	m, ok := node.(map[string]any)
	if !ok {
		return false
	}
	return len(m) == 0
}

// clauseBuilder mirrors the previous map-based builder for a single clause.
type clauseBuilder struct {
	root         any
	aggCounters  map[string]int
	stickyKey    string
	stickyIndex  string
	stickyActive bool
}

func newClauseBuilder() *clauseBuilder {
	return &clauseBuilder{root: map[string]any{}, aggCounters: make(map[string]int)}
}

func (c *clauseBuilder) beginSticky(key string) {
	if key == "" {
		return
	}
	c.stickyKey = key
	c.stickyActive = true
	c.stickyIndex = ""
}

func (c *clauseBuilder) endSticky() {
	c.stickyActive = false
	c.stickyKey = ""
	c.stickyIndex = ""
}

func (c *clauseBuilder) assign(tokens []string, value any) error {
	normalized := c.normalize(tokens)
	node, err := assignRecursive(c.root, normalized, value)
	if err != nil {
		return err
	}
	c.root = node
	return nil
}

func (c *clauseBuilder) normalize(tokens []string) []string {
	normalized := make([]string, 0, len(tokens)+2)
	var pathParts []string
	for i, token := range tokens {
		if token == "" {
			continue
		}
		lower := strings.ToLower(token)
		if _, ok := selectorRoots[lower]; ok {
			token = lower
		}
		normalized = append(normalized, token)
		pathParts = append(pathParts, token)
		if _, isAgg := aggregatorTokens[token]; isAgg {
			nextIsIndex := i+1 < len(tokens) && looksLikeIndex(tokens[i+1])
			if nextIsIndex {
				continue
			}
			key := strings.Join(pathParts, ".")
			if c.stickyActive && c.stickyKey == key {
				if c.stickyIndex == "" {
					idx := c.aggCounters[key]
					c.aggCounters[key] = idx + 1
					c.stickyIndex = strconv.Itoa(idx)
				}
				normalized = append(normalized, c.stickyIndex)
				pathParts = append(pathParts, c.stickyIndex)
				continue
			}
			idx := c.aggCounters[key]
			c.aggCounters[key] = idx + 1
			indexStr := strconv.Itoa(idx)
			normalized = append(normalized, indexStr)
			pathParts = append(pathParts, indexStr)
			if c.stickyActive && c.stickyKey == key {
				c.stickyIndex = indexStr
			}
		}
	}
	return normalized
}

func assignRecursive(node any, path []string, value any) (any, error) {
	if len(path) == 0 {
		return value, nil
	}
	token := path[0]
	last := len(path) == 1
	if idx, err := strconv.Atoi(token); err == nil {
		if idx < 0 || idx > maxSelectorIndex {
			return nil, fmt.Errorf("selector index %d invalid", idx)
		}
		var arr []any
		switch v := node.(type) {
		case nil:
			arr = make([]any, idx+1)
		case []any:
			arr = v
			if idx >= len(arr) {
				grow := idx - len(arr) + 1
				if grow > maxSelectorIndex {
					return nil, fmt.Errorf("selector index %d invalid", idx)
				}
				padding := make([]any, grow)
				arr = append(arr, padding...)
			}
		default:
			return nil, fmt.Errorf("selector path conflict at %s", token)
		}
		var err2 error
		if last {
			if arr[idx] != nil {
				return nil, fmt.Errorf("selector path conflict at %s", token)
			}
			arr[idx] = value
		} else {
			arr[idx], err2 = assignRecursive(arr[idx], path[1:], value)
			if err2 != nil {
				return nil, err2
			}
		}
		return arr, nil
	}
	var obj map[string]any
	switch v := node.(type) {
	case nil:
		obj = make(map[string]any)
	case map[string]any:
		obj = v
	default:
		return nil, fmt.Errorf("selector path conflict at %s", token)
	}
	if last {
		if _, exists := obj[token]; exists {
			return nil, fmt.Errorf("selector path conflict at %s", token)
		}
		obj[token] = value
		return obj, nil
	}
	child, err := assignRecursive(obj[token], path[1:], value)
	if err != nil {
		return nil, err
	}
	obj[token] = child
	return obj, nil
}

func looksLikeIndex(token string) bool {
	if token == "" {
		return false
	}
	for _, r := range token {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func convertSelectorValue(v string) any {
	trimmed := strings.TrimSpace(v)
	if trimmed == "" {
		return ""
	}
	if stripped, ok := stripQuotes(trimmed); ok {
		trimmed = stripped
	}
	if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return f
	}
	lower := strings.ToLower(trimmed)
	switch lower {
	case "true":
		return true
	case "false":
		return false
	}
	return trimmed
}

func stripQuotes(v string) (string, bool) {
	if len(v) >= 2 {
		if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
			return v[1 : len(v)-1], true
		}
	}
	return v, false
}
