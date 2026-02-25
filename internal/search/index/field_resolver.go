package index

import (
	"context"
	"sort"
	"strings"

	"pkt.systems/lockd/internal/jsonpointer"
)

func (r *segmentReader) resolveSelectorFields(ctx context.Context, field string) ([]string, error) {
	field = strings.TrimSpace(field)
	if field == "" {
		return nil, nil
	}
	if !selectorFieldMayNeedExpansion(field) {
		return []string{field}, nil
	}
	if cached, ok := r.fieldResolutionCached[field]; ok {
		return cached, nil
	}
	pattern, hasWildcard, hasRecursive, err := selectorFieldPatternSegments(field)
	if err != nil {
		return nil, err
	}
	if hasRecursive {
		return nil, nil
	}
	if !hasWildcard {
		return []string{field}, nil
	}
	fields, err := r.allIndexFields(ctx)
	if err != nil {
		return nil, err
	}
	matches := make([]string, 0, len(fields))
	for _, candidate := range fields {
		if wildcardFieldPathMatch(pattern, r.fieldSegments[candidate]) {
			matches = append(matches, candidate)
		}
	}
	r.fieldResolutionCached[field] = append([]string(nil), matches...)
	return r.fieldResolutionCached[field], nil
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
		r.fieldSegments[field] = segments
	}
	r.fieldList = fields
	r.fieldListReady = true
	return fields, nil
}

func selectorFieldMayNeedExpansion(field string) bool {
	return strings.Contains(field, "*") || strings.Contains(field, "[]") || strings.Contains(field, "...")
}

func selectorFieldPatternSegments(field string) ([]string, bool, bool, error) {
	segments, err := indexFieldSegments(field)
	if err != nil {
		return nil, false, false, err
	}
	pattern := make([]string, 0, len(segments))
	hasWildcard := false
	hasRecursive := false
	for _, segment := range segments {
		switch segment {
		case "[]":
			// Arrays are flattened in index paths, so [] consumes no segment.
			hasWildcard = true
			continue
		case "*":
			hasWildcard = true
			pattern = append(pattern, segment)
		case "**", "...":
			hasWildcard = true
			hasRecursive = true
			pattern = append(pattern, segment)
		default:
			pattern = append(pattern, segment)
		}
	}
	return pattern, hasWildcard, hasRecursive, nil
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
	if len(pattern) != len(candidate) {
		return false
	}
	for i := range pattern {
		if pattern[i] == "*" {
			continue
		}
		if pattern[i] != candidate[i] {
			return false
		}
	}
	return true
}
