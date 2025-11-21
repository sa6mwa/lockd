package httpapi

import (
	"encoding/json"
	"strconv"

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
