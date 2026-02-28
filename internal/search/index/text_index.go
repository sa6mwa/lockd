package index

import (
	"strings"
	"unicode"
)

const containsGramFieldPrefix = "__g3__:"
const tokenFieldPrefix = "__tok__:"
const tokenAllTextField = "__tok__:_all_text"

func containsGramField(field string) string {
	return containsGramFieldPrefix + field
}

func tokenizedField(field string) string {
	return tokenFieldPrefix + field
}

func normalizedTrigrams(value string) []string {
	normalized := normalizeTermValue(value)
	if normalized == "" {
		return nil
	}
	if isASCII(normalized) {
		if len(normalized) < 3 {
			return nil
		}
		out := make([]string, 0, len(normalized)-2)
		for i := 0; i+3 <= len(normalized); i++ {
			// Substring views avoid per-gram allocations for ASCII inputs.
			out = append(out, normalized[i:i+3])
		}
		return out
	}
	runes := []rune(normalized)
	if len(runes) < 3 {
		return nil
	}
	out := make([]string, 0, len(runes)-2)
	for i := 0; i+3 <= len(runes); i++ {
		out = append(out, string(runes[i:i+3]))
	}
	return out
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

// TextFieldMode controls how a string field is indexed for query evaluation.
type TextFieldMode string

const (
	// TextFieldModeRaw keeps exact raw terms and trigram postings.
	TextFieldModeRaw TextFieldMode = "raw"
	// TextFieldModeTokenized indexes analyzer tokens only.
	TextFieldModeTokenized TextFieldMode = "tokenized"
	// TextFieldModeBoth indexes both raw/trigram and tokenized postings.
	TextFieldModeBoth TextFieldMode = "both"
)

// TextAnalyzer tokenizes string values for tokenized full-text indexing.
type TextAnalyzer interface {
	Tokens(value string) []string
}

type simpleTextAnalyzer struct{}

func (simpleTextAnalyzer) Tokens(value string) []string {
	return simpleTextTokens(value, false)
}

func simpleTextTokens(value string, enableStemming bool) []string {
	normalized := normalizeTermValue(value)
	if normalized == "" {
		return nil
	}
	parts := strings.FieldsFunc(normalized, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	if len(parts) == 0 {
		return nil
	}
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		if enableStemming {
			part = stemToken(part)
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}

func stemToken(token string) string {
	if len(token) <= 3 {
		return token
	}
	switch {
	case strings.HasSuffix(token, "ing") && len(token) > 5:
		return strings.TrimSuffix(token, "ing")
	case strings.HasSuffix(token, "ed") && len(token) > 4:
		return strings.TrimSuffix(token, "ed")
	case strings.HasSuffix(token, "es") && len(token) > 4:
		return strings.TrimSuffix(token, "es")
	case strings.HasSuffix(token, "s") && len(token) > 4:
		return strings.TrimSuffix(token, "s")
	default:
		return token
	}
}

// TextIndexPolicy controls per-field text indexing behavior.
type TextIndexPolicy struct {
	// DefaultMode applies when a field is not explicitly configured.
	DefaultMode TextFieldMode
	// FieldModes overrides indexing mode per field path.
	FieldModes map[string]TextFieldMode
	// EnableAllText emits tokens to a synthetic namespace-wide all-text field.
	EnableAllText bool
	// EnableSimpleStemming enables a simple suffix stemmer in the default analyzer.
	EnableSimpleStemming bool
	// Analyzer tokenizes values for tokenized mode.
	Analyzer TextAnalyzer
}

// DefaultTextIndexPolicy returns the default text indexing behavior.
func DefaultTextIndexPolicy() TextIndexPolicy {
	return TextIndexPolicy{
		DefaultMode: TextFieldModeBoth,
	}
}

func (p TextIndexPolicy) normalized() TextIndexPolicy {
	if p.DefaultMode == "" {
		p.DefaultMode = TextFieldModeBoth
	}
	if p.Analyzer == nil {
		p.Analyzer = textAnalyzerFunc(func(value string) []string {
			return simpleTextTokens(value, p.EnableSimpleStemming)
		})
	}
	return p
}

func (p TextIndexPolicy) modeForField(field string) TextFieldMode {
	p = p.normalized()
	if p.FieldModes == nil {
		return p.DefaultMode
	}
	if mode, ok := p.FieldModes[field]; ok && mode != "" {
		return mode
	}
	return p.DefaultMode
}

type textAnalyzerFunc func(value string) []string

func (f textAnalyzerFunc) Tokens(value string) []string {
	return f(value)
}
