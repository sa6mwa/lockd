package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	lockdclient "pkt.systems/lockd/client"
)

type toolErrorEnvelope struct {
	ErrorCode         string `json:"error_code"`
	Detail            string `json:"detail,omitempty"`
	Retryable         bool   `json:"retryable"`
	HTTPStatus        int    `json:"http_status,omitempty"`
	RetryAfterSeconds int64  `json:"retry_after_seconds,omitempty"`
	QRFState          string `json:"qrf_state,omitempty"`
}

func withStructuredToolErrors[In, Out any](h mcpsdk.ToolHandlerFor[In, Out]) mcpsdk.ToolHandlerFor[In, Out] {
	return func(ctx context.Context, req *mcpsdk.CallToolRequest, input In) (*mcpsdk.CallToolResult, Out, error) {
		res, out, err := h(ctx, req, input)
		if err == nil {
			return res, out, nil
		}
		var zero Out
		return nil, zero, toolError{Envelope: classifyToolError(err)}
	}
}

type toolError struct {
	Envelope toolErrorEnvelope
}

func (e toolError) Error() string {
	envelope := map[string]any{"error": e.Envelope}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return `{"error":{"error_code":"tool_error","detail":"failed to encode error envelope"}}`
	}
	return string(encoded)
}

func classifyToolError(err error) toolErrorEnvelope {
	env := toolErrorEnvelope{ErrorCode: "tool_error", Detail: strings.TrimSpace(err.Error())}
	var apiErr *lockdclient.APIError
	if errors.As(err, &apiErr) {
		env.HTTPStatus = apiErr.Status
		env.QRFState = strings.TrimSpace(apiErr.QRFState)
		env.Detail = strings.TrimSpace(apiErr.Response.Detail)
		if env.Detail == "" {
			env.Detail = strings.TrimSpace(err.Error())
		}
		code := strings.TrimSpace(apiErr.Response.ErrorCode)
		if code == "" {
			code = "http_" + strconv.Itoa(apiErr.Status)
		}
		env.ErrorCode = code
		if retryAfter := apiErr.RetryAfterDuration(); retryAfter > 0 {
			env.RetryAfterSeconds = int64(retryAfter.Seconds())
			env.Retryable = true
		}
		switch {
		case apiErr.Status == http.StatusTooManyRequests, apiErr.Status == http.StatusRequestTimeout, apiErr.Status >= 500:
			env.Retryable = true
		}
		switch strings.ToLower(strings.TrimSpace(code)) {
		case "waiting", "node_passive", "cas_mismatch", "qrf_active", "qrf_deny":
			env.Retryable = true
		}
		return env
	}
	lower := strings.ToLower(env.Detail)
	switch {
	case strings.Contains(lower, "required"),
		strings.Contains(lower, "must be"),
		strings.Contains(lower, "invalid"),
		strings.Contains(lower, "mutually exclusive"),
		strings.Contains(lower, "exceed"),
		strings.Contains(lower, "decode "):
		env.ErrorCode = "invalid_argument"
	case strings.Contains(lower, "timeout"), strings.Contains(lower, "deadline"):
		env.ErrorCode = "timeout"
		env.Retryable = true
	case strings.Contains(lower, "temporar"), strings.Contains(lower, "try again"):
		env.ErrorCode = "unavailable"
		env.Retryable = true
	}
	return env
}
