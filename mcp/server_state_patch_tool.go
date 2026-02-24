package mcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	lockdclient "pkt.systems/lockd/client"
)

type statePatchToolInput struct {
	Key          string `json:"key" jsonschema:"State key"`
	Namespace    string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID      string `json:"lease_id" jsonschema:"Active lease id"`
	TxnID        string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	IfETag       string `json:"if_etag,omitempty" jsonschema:"Conditional ETag guard"`
	IfVersion    *int64 `json:"if_version,omitempty" jsonschema:"Conditional version guard"`
	QueryHidden  *bool  `json:"query_hidden,omitempty" jsonschema:"Optional query-hidden metadata mutation"`
	PatchText    string `json:"patch_text,omitempty" jsonschema:"UTF-8 JSON merge patch document"`
	PatchBase64  string `json:"patch_base64,omitempty" jsonschema:"Base64-encoded JSON merge patch document"`
}

type statePatchToolOutput struct {
	NewVersion   int64  `json:"new_version"`
	NewStateETag string `json:"new_state_etag"`
	Bytes        int64  `json:"bytes"`
	QueryHidden  *bool  `json:"query_hidden,omitempty"`
}

func (s *server) handleStatePatchTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input statePatchToolInput) (*mcpsdk.CallToolResult, statePatchToolOutput, error) {
	key := strings.TrimSpace(input.Key)
	leaseID := strings.TrimSpace(input.LeaseID)
	if key == "" || leaseID == "" {
		return nil, statePatchToolOutput{}, fmt.Errorf("key and lease_id are required")
	}
	if strings.TrimSpace(input.PatchBase64) != "" && input.PatchText != "" {
		return nil, statePatchToolOutput{}, fmt.Errorf("patch_text and patch_base64 are mutually exclusive")
	}
	patchBytes, err := decodePatchPayload(input)
	if err != nil {
		return nil, statePatchToolOutput{}, err
	}
	if len(patchBytes) == 0 {
		return nil, statePatchToolOutput{}, fmt.Errorf("patch_text or patch_base64 is required")
	}
	if err := validateInlinePayloadBytes(int64(len(patchBytes)), s.cfg.InlineMaxBytes, toolStatePatch, toolStateWriteStreamBegin); err != nil {
		return nil, statePatchToolOutput{}, err
	}

	currentBytes, err := s.loadStateForPatch(ctx, key, input)
	if err != nil {
		return nil, statePatchToolOutput{}, err
	}
	patched, err := applyJSONMergePatch(currentBytes, patchBytes)
	if err != nil {
		return nil, statePatchToolOutput{}, err
	}
	if err := validateInlinePayloadBytes(int64(len(patched)), s.cfg.InlineMaxBytes, toolStatePatch, toolStateWriteStreamBegin); err != nil {
		return nil, statePatchToolOutput{}, err
	}

	opts := lockdclient.UpdateOptions{
		Namespace:    s.resolveNamespace(input.Namespace),
		TxnID:        strings.TrimSpace(input.TxnID),
		FencingToken: input.FencingToken,
		IfETag:       strings.TrimSpace(input.IfETag),
		IfVersion:    input.IfVersion,
		Metadata: lockdclient.MetadataOptions{
			QueryHidden: input.QueryHidden,
			TxnID:       strings.TrimSpace(input.TxnID),
		},
	}
	resp, err := s.upstream.Update(ctx, key, leaseID, bytes.NewReader(patched), opts)
	if err != nil {
		return nil, statePatchToolOutput{}, err
	}
	return nil, statePatchToolOutput{
		NewVersion:   resp.NewVersion,
		NewStateETag: resp.NewStateETag,
		Bytes:        resp.BytesWritten,
		QueryHidden:  resp.Metadata.QueryHidden,
	}, nil
}

func decodePatchPayload(input statePatchToolInput) ([]byte, error) {
	if encoded := strings.TrimSpace(input.PatchBase64); encoded != "" {
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("decode patch_base64: %w", err)
		}
		return decoded, nil
	}
	if strings.TrimSpace(input.PatchText) != "" {
		return []byte(input.PatchText), nil
	}
	return nil, nil
}

func (s *server) loadStateForPatch(ctx context.Context, key string, input statePatchToolInput) ([]byte, error) {
	namespace := s.resolveNamespace(input.Namespace)
	leaseID := strings.TrimSpace(input.LeaseID)
	resp, err := s.upstream.Get(ctx, key,
		lockdclient.WithGetNamespace(namespace),
		lockdclient.WithGetLeaseID(leaseID),
		lockdclient.WithGetPublicDisabled(true),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Close()
	if !resp.HasState || resp.Reader() == nil {
		return []byte("{}"), nil
	}
	limit := normalizedInlineMaxBytes(s.cfg.InlineMaxBytes)
	limitedReader := io.LimitReader(resp.Reader(), limit+1)
	payload, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, fmt.Errorf("read current state for patch: %w", err)
	}
	if int64(len(payload)) > limit {
		return nil, inlinePayloadTooLargeError(toolStatePatch, int64(len(payload)), limit, toolStateWriteStreamBegin)
	}
	if len(bytes.TrimSpace(payload)) == 0 {
		return []byte("{}"), nil
	}
	return payload, nil
}

func applyJSONMergePatch(current, patch []byte) ([]byte, error) {
	var currentValue any
	if len(bytes.TrimSpace(current)) == 0 {
		currentValue = map[string]any{}
	} else if err := json.Unmarshal(current, &currentValue); err != nil {
		return nil, fmt.Errorf("decode current state json: %w", err)
	}
	var patchValue any
	if err := json.Unmarshal(patch, &patchValue); err != nil {
		return nil, fmt.Errorf("decode patch json: %w", err)
	}
	merged := mergePatchValue(currentValue, patchValue)
	encoded, err := json.Marshal(merged)
	if err != nil {
		return nil, fmt.Errorf("encode patched state json: %w", err)
	}
	return encoded, nil
}

func mergePatchValue(current, patch any) any {
	patchObj, ok := patch.(map[string]any)
	if !ok {
		return patch
	}
	currentObj, ok := current.(map[string]any)
	if !ok {
		currentObj = map[string]any{}
	}
	out := make(map[string]any, len(currentObj))
	for key, value := range currentObj {
		out[key] = value
	}
	for key, patchValue := range patchObj {
		if patchValue == nil {
			delete(out, key)
			continue
		}
		existing, exists := out[key]
		if !exists {
			existing = nil
		}
		out[key] = mergePatchValue(existing, patchValue)
	}
	return out
}
