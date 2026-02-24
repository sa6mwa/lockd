package mcp

import (
	"context"
	"fmt"
	"strings"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	lockdclient "pkt.systems/lockd/client"
)

type stateMutateToolInput struct {
	Key          string   `json:"key" jsonschema:"State key"`
	Namespace    string   `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID      string   `json:"lease_id" jsonschema:"Active lease id"`
	TxnID        string   `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken *int64   `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	IfETag       string   `json:"if_etag,omitempty" jsonschema:"Conditional ETag guard"`
	IfVersion    *int64   `json:"if_version,omitempty" jsonschema:"Conditional version guard"`
	QueryHidden  *bool    `json:"query_hidden,omitempty" jsonschema:"Optional query-hidden metadata mutation"`
	Mutations    []string `json:"mutations" jsonschema:"LQL mutation expressions"`
}

type stateMutateToolOutput struct {
	NewVersion   int64  `json:"new_version"`
	NewStateETag string `json:"new_state_etag"`
	Bytes        int64  `json:"bytes"`
	QueryHidden  *bool  `json:"query_hidden,omitempty"`
}

func (s *server) handleStateMutateTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input stateMutateToolInput) (*mcpsdk.CallToolResult, stateMutateToolOutput, error) {
	key := strings.TrimSpace(input.Key)
	leaseID := strings.TrimSpace(input.LeaseID)
	if key == "" || leaseID == "" {
		return nil, stateMutateToolOutput{}, fmt.Errorf("key and lease_id are required")
	}

	mutationExprs := make([]string, 0, len(input.Mutations))
	for _, raw := range input.Mutations {
		expr := strings.TrimSpace(raw)
		if expr != "" {
			mutationExprs = append(mutationExprs, expr)
		}
	}
	if len(mutationExprs) == 0 {
		return nil, stateMutateToolOutput{}, fmt.Errorf("mutations are required")
	}

	resp, err := s.upstream.Mutate(ctx, lockdclient.MutateRequest{
		Key:       key,
		LeaseID:   leaseID,
		Mutations: mutationExprs,
		Options: lockdclient.UpdateOptions{
			Namespace:    s.resolveNamespace(input.Namespace),
			TxnID:        strings.TrimSpace(input.TxnID),
			FencingToken: input.FencingToken,
			IfETag:       strings.TrimSpace(input.IfETag),
			IfVersion:    input.IfVersion,
			Metadata: lockdclient.MetadataOptions{
				QueryHidden: input.QueryHidden,
				TxnID:       strings.TrimSpace(input.TxnID),
			},
		},
	})
	if err != nil {
		return nil, stateMutateToolOutput{}, err
	}
	return nil, stateMutateToolOutput{
		NewVersion:   resp.NewVersion,
		NewStateETag: resp.NewStateETag,
		Bytes:        resp.BytesWritten,
		QueryHidden:  resp.Metadata.QueryHidden,
	}, nil
}
