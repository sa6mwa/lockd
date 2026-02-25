package index

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"pkt.systems/lockd/internal/jsonpointer"
)

const maxTriePatternStates = 250_000

var errFieldPatternStateLimit = fmt.Errorf("field pattern state limit exceeded")

func (r *segmentReader) resolveSelectorFields(ctx context.Context, field string) ([]string, error) {
	field = strings.TrimSpace(field)
	if field == "" {
		return nil, nil
	}
	if !selectorFieldMayNeedExpansion(field) {
		return []string{field}, nil
	}
	if cached, ok := r.cachedFieldResolution(field); ok {
		return cached, nil
	}
	pattern, hasWildcard, err := selectorFieldPatternSegments(field)
	if err != nil {
		return nil, err
	}
	if !hasWildcard {
		return []string{field}, nil
	}
	fields, err := r.allIndexFields(ctx)
	if err != nil {
		return nil, err
	}
	if !shouldUseTriePatternPlanner(pattern, len(fields)) {
		matches := make([]string, 0, 16)
		for _, candidate := range fields {
			if wildcardFieldPathMatch(pattern, r.fieldSegments[candidate]) {
				matches = append(matches, candidate)
			}
		}
		r.rememberFieldResolution(field, matches)
		return matches, nil
	}
	trieMatches, matchErr := r.matchPatternWithTrie(pattern)
	if matchErr == nil {
		r.rememberFieldResolution(field, trieMatches)
		return trieMatches, nil
	}
	// Fallback preserves correctness under pathological recursive expansion.
	matches := make([]string, 0, 16)
	for _, candidate := range fields {
		if wildcardFieldPathMatch(pattern, r.fieldSegments[candidate]) {
			matches = append(matches, candidate)
		}
	}
	r.rememberFieldResolution(field, matches)
	return matches, nil
}

func (r *segmentReader) cachedFieldResolution(field string) ([]string, bool) {
	if r == nil || field == "" {
		return nil, false
	}
	if cached, ok := r.fieldResolutionCached[field]; ok {
		return cached, true
	}
	if r.sharedFieldResolution == nil {
		return nil, false
	}
	cached, ok := r.sharedFieldResolution.get(field)
	if !ok {
		return nil, false
	}
	r.fieldResolutionCached[field] = cached
	return cached, true
}

func (r *segmentReader) rememberFieldResolution(field string, matches []string) {
	if r == nil || field == "" {
		return
	}
	r.fieldResolutionCached[field] = matches
	if r.sharedFieldResolution != nil {
		r.sharedFieldResolution.put(field, matches)
	}
}

func shouldUseTriePatternPlanner(pattern []string, fieldCount int) bool {
	if fieldCount <= 0 || len(pattern) == 0 {
		return true
	}
	wildcards := 0
	recursive := 0
	for _, token := range pattern {
		switch token {
		case "*", "[]":
			wildcards++
		case "**", "...":
			recursive++
		}
	}
	if recursive == 0 && wildcards == 0 {
		return true
	}
	if recursive > 0 && fieldCount > 4_096 {
		return false
	}
	estimatedStates := fieldCount * (1 + (wildcards * 2) + (recursive * 8))
	return estimatedStates <= (maxTriePatternStates / 2)
}

func (r *segmentReader) allIndexFields(ctx context.Context) ([]string, error) {
	if r.fieldListReady {
		return r.fieldList, nil
	}
	set := make(map[string]struct{})
	if err := r.forEachSegment(ctx, func(seg *Segment) error {
		for field := range seg.Fields {
			if strings.HasPrefix(field, containsGramFieldPrefix) {
				continue
			}
			if strings.HasPrefix(field, tokenFieldPrefix) {
				continue
			}
			set[field] = struct{}{}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	fields := make([]string, 0, len(set))
	for field := range set {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	for _, field := range fields {
		segments, err := indexFieldSegments(field)
		if err != nil {
			continue
		}
		r.internField(field)
		r.fieldSegments[field] = segments
	}
	r.fieldList = fields
	r.fieldTrie = buildFieldPathTrie(fields, r.fieldSegments)
	r.fieldListReady = true
	return fields, nil
}

func selectorFieldMayNeedExpansion(field string) bool {
	return strings.Contains(field, "*") || strings.Contains(field, "[]") || strings.Contains(field, "...")
}

func selectorFieldPatternSegments(field string) ([]string, bool, error) {
	segments, err := indexFieldSegments(field)
	if err != nil {
		return nil, false, err
	}
	hasWildcard := false
	for _, segment := range segments {
		switch segment {
		case "[]", "*", "**", "...":
			hasWildcard = true
			return segments, hasWildcard, nil
		}
	}
	return segments, hasWildcard, nil
}

func indexFieldSegments(field string) ([]string, error) {
	segments, err := jsonpointer.Split(field)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, nil
	}
	return expandBracketSugarSegments(segments), nil
}

func expandBracketSugarSegments(segments []string) []string {
	out := make([]string, 0, len(segments))
	for _, segment := range segments {
		if segment == "" || segment == "[]" || segment == "*" || segment == "**" || segment == "..." || !strings.HasSuffix(segment, "[]") {
			out = append(out, segment)
			continue
		}
		base := segment
		count := 0
		for strings.HasSuffix(base, "[]") && base != "[]" {
			base = strings.TrimSuffix(base, "[]")
			count++
		}
		if base == "" {
			out = append(out, segment)
			continue
		}
		out = append(out, base)
		for i := 0; i < count; i++ {
			out = append(out, "[]")
		}
	}
	return out
}

func wildcardFieldPathMatch(pattern, candidate []string) bool {
	return wildcardFieldPathMatchAt(pattern, candidate, 0, 0)
}

func wildcardFieldPathMatchAt(pattern, candidate []string, pi, ci int) bool {
	for {
		if pi >= len(pattern) {
			return ci >= len(candidate)
		}
		token := pattern[pi]
		switch token {
		case "[]":
			pi++
			continue
		case "*":
			if ci >= len(candidate) {
				return false
			}
			pi++
			ci++
			continue
		case "**":
			if wildcardFieldPathMatchAt(pattern, candidate, pi+1, ci) {
				return true
			}
			if ci < len(candidate) {
				return wildcardFieldPathMatchAt(pattern, candidate, pi+1, ci+1)
			}
			return false
		case "...":
			if wildcardFieldPathMatchAt(pattern, candidate, pi+1, ci) {
				return true
			}
			if ci < len(candidate) {
				return wildcardFieldPathMatchAt(pattern, candidate, pi, ci+1)
			}
			return false
		default:
			if ci >= len(candidate) || token != candidate[ci] {
				return false
			}
			pi++
			ci++
		}
	}
}

type fieldPathTrie struct {
	root *fieldPathTrieNode
}

type fieldPathTrieNode struct {
	id        int
	children  map[string]*fieldPathTrieNode
	terminals []string
}

func buildFieldPathTrie(fields []string, segments map[string][]string) *fieldPathTrie {
	root := &fieldPathTrieNode{id: 1, children: make(map[string]*fieldPathTrieNode)}
	nextID := 2
	for _, field := range fields {
		node := root
		parts := segments[field]
		for _, part := range parts {
			child := node.children[part]
			if child == nil {
				child = &fieldPathTrieNode{id: nextID, children: make(map[string]*fieldPathTrieNode)}
				nextID++
				node.children[part] = child
			}
			node = child
		}
		node.terminals = append(node.terminals, field)
	}
	return &fieldPathTrie{root: root}
}

func (r *segmentReader) matchPatternWithTrie(pattern []string) ([]string, error) {
	if r.fieldTrie == nil || r.fieldTrie.root == nil {
		return nil, nil
	}
	type trieState struct {
		nodeID int
		pi     int
	}
	seen := make(map[trieState]struct{}, 512)
	out := make(map[string]struct{}, 128)
	stateCount := 0

	var walk func(node *fieldPathTrieNode, pi int) error
	walk = func(node *fieldPathTrieNode, pi int) error {
		if node == nil {
			return nil
		}
		state := trieState{nodeID: node.id, pi: pi}
		if _, ok := seen[state]; ok {
			return nil
		}
		seen[state] = struct{}{}
		stateCount++
		if stateCount > maxTriePatternStates {
			return errFieldPatternStateLimit
		}
		if pi >= len(pattern) {
			for _, field := range node.terminals {
				out[field] = struct{}{}
			}
			return nil
		}
		token := pattern[pi]
		switch token {
		case "[]":
			return walk(node, pi+1)
		case "*":
			for _, child := range node.children {
				if err := walk(child, pi+1); err != nil {
					return err
				}
			}
			return nil
		case "**":
			if err := walk(node, pi+1); err != nil {
				return err
			}
			for _, child := range node.children {
				if err := walk(child, pi+1); err != nil {
					return err
				}
			}
			return nil
		case "...":
			if err := walk(node, pi+1); err != nil {
				return err
			}
			for _, child := range node.children {
				if err := walk(child, pi); err != nil {
					return err
				}
			}
			return nil
		default:
			child := node.children[token]
			return walk(child, pi+1)
		}
	}
	if err := walk(r.fieldTrie.root, 0); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	matches := make([]string, 0, len(out))
	for field := range out {
		matches = append(matches, field)
	}
	sort.Strings(matches)
	return matches, nil
}
