package index

const containsGramFieldPrefix = "__g3__:"

func containsGramField(field string) string {
	return containsGramFieldPrefix + field
}

func normalizedTrigrams(value string) []string {
	normalized := normalizeTermValue(value)
	runes := []rune(normalized)
	if len(runes) < 3 {
		return nil
	}
	out := make([]string, 0, len(runes)-2)
	seen := make(map[string]struct{}, len(runes)-2)
	for i := 0; i+3 <= len(runes); i++ {
		gram := string(runes[i : i+3])
		if _, ok := seen[gram]; ok {
			continue
		}
		seen[gram] = struct{}{}
		out = append(out, gram)
	}
	return out
}
