package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"pkt.systems/lockd/api"
)

func makeQueryResponseHeaders(cursor string, indexSeq uint64, metadata map[string]string, mode api.QueryReturn) map[string]string {
	headers := map[string]string{
		headerQueryReturn: string(mode),
	}
	if cursor != "" {
		headers[headerQueryCursor] = cursor
	}
	if indexSeq > 0 {
		headers[headerQueryIndexSeq] = strconv.FormatUint(indexSeq, 10)
	}
	if len(metadata) > 0 {
		if payload, err := json.Marshal(metadata); err == nil {
			headers[headerQueryMetadata] = string(payload)
		}
	}
	return headers
}

func declareQueryDocumentTrailers(headers http.Header) {
	if headers == nil {
		return
	}
	headers.Set("Trailer", strings.Join([]string{
		headerQueryCursor,
		headerQueryError,
		headerQueryIndexSeq,
		headerQueryMetadata,
	}, ", "))
}

func applyQueryDocumentTrailers(headers http.Header, cursor string, indexSeq uint64, metadata map[string]string) {
	if headers == nil {
		return
	}
	if cursor != "" {
		headers.Set(headerQueryCursor, cursor)
	}
	if indexSeq > 0 {
		headers.Set(headerQueryIndexSeq, strconv.FormatUint(indexSeq, 10))
	}
	if len(metadata) > 0 {
		if payload, err := json.Marshal(metadata); err == nil {
			headers.Set(headerQueryMetadata, string(payload))
		}
	}
}

func applyQueryDocumentErrorTrailer(headers http.Header, err error) {
	if headers == nil || err == nil {
		return
	}
	if message := sanitizeTrailerValue(err.Error()); message != "" {
		headers.Set(headerQueryError, message)
	}
}

func sanitizeTrailerValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, value)
}
