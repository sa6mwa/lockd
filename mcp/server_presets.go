package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	lockdclient "pkt.systems/lockd/client"
	presetcfg "pkt.systems/lockd/mcp/preset"
)

type toolSurface struct {
	Lockd   bool
	Presets []presetcfg.Definition
}

func defaultToolSurface() toolSurface {
	return toolSurface{Lockd: true}
}

func (s *server) toolSurfaceForRequest(r *http.Request) toolSurface {
	if s.oauthManager == nil || r == nil {
		return defaultToolSurface()
	}
	tokenInfo := mcpauth.TokenInfoFromContext(r.Context())
	if tokenInfo == nil || strings.TrimSpace(tokenInfo.UserID) == "" {
		return defaultToolSurface()
	}
	snapshot := s.oauthManager.Snapshot()
	if snapshot == nil {
		return defaultToolSurface()
	}
	client, ok := snapshot.Clients[strings.TrimSpace(tokenInfo.UserID)]
	if !ok || client.Revoked {
		return defaultToolSurface()
	}
	return toolSurface{
		Lockd:   client.LockdPreset,
		Presets: client.Presets,
	}
}

func (s *server) newMCPServerForSurface(surface toolSurface) *mcpsdk.Server {
	mcpSrv := mcpsdk.NewServer(&mcpsdk.Implementation{
		Name:    "lockd-mcp-facade",
		Version: "0.1.0",
	}, &mcpsdk.ServerOptions{
		Instructions:       defaultServerInstructions(s.cfg),
		InitializedHandler: s.handleInitialized,
	})
	s.registerResources(mcpSrv)
	s.registerToolsForSurface(mcpSrv, surface)
	return mcpSrv
}

func (s *server) registerToolsForSurface(srv *mcpsdk.Server, surface toolSurface) {
	if surface.Lockd {
		s.registerLockdTools(srv)
	}
	s.registerPresetTools(srv, surface.Presets)
}

func (s *server) registerPresetTools(srv *mcpsdk.Server, presets []presetcfg.Definition) {
	normalized, err := presetcfg.NormalizeCollection(presets)
	switch {
	case len(presets) == 0:
		return
	case err != nil:
		panic(err)
	}
	for _, def := range normalized {
		s.registerPresetHelpTool(srv, def)
		for _, kind := range def.Kinds {
			runtimeKind, err := newPresetRuntimeKind(kind)
			if err != nil {
				panic(err)
			}
			for _, op := range kind.Operations {
				switch op {
				case presetcfg.OperationQuery:
					s.addPresetQueryTool(srv, def, runtimeKind)
				case presetcfg.OperationStatePut:
					s.addPresetStatePutTool(srv, def, runtimeKind)
				case presetcfg.OperationStateGet:
					s.addPresetStateGetTool(srv, def, runtimeKind)
				case presetcfg.OperationStateDelete:
					s.addPresetStateDeleteTool(srv, def, runtimeKind)
				case presetcfg.OperationQueueEnqueue:
					s.addPresetQueueEnqueueTool(srv, def, runtimeKind)
				case presetcfg.OperationAttachmentsGet:
					s.addPresetAttachmentsGetTool(srv, def, runtimeKind)
				}
			}
		}
	}
}

type presetRuntimeKind struct {
	kind              presetcfg.Kind
	documentSchema    *jsonschema.Schema
	documentResolved  *jsonschema.Resolved
	statePutInput     *jsonschema.Schema
	stateGetInput     *jsonschema.Schema
	stateOutput       *jsonschema.Schema
	stateDeleteInput  *jsonschema.Schema
	queryInput        *jsonschema.Schema
	queryOutput       *jsonschema.Schema
	queueEnqueueInput *jsonschema.Schema
	queueOutput       *jsonschema.Schema
	attachmentsInput  *jsonschema.Schema
	attachmentsOutput *jsonschema.Schema
}

func newPresetRuntimeKind(kind presetcfg.Kind) (*presetRuntimeKind, error) {
	documentSchema := presetSchemaToJSONSchema(kind.Schema)
	resolved, err := documentSchema.Resolve(nil)
	if err != nil {
		return nil, fmt.Errorf("resolve preset schema %s: %w", kind.Name, err)
	}
	outSchema := buildPresetStateOutputSchema(kind.Schema)
	return &presetRuntimeKind{
		kind:              kind,
		documentSchema:    documentSchema,
		documentResolved:  resolved,
		statePutInput:     buildPresetStatePutInputSchema(kind.Schema),
		stateGetInput:     buildPresetKeyOnlyInputSchema("Preset document key"),
		stateOutput:       outSchema,
		stateDeleteInput:  buildPresetKeyOnlyInputSchema("Preset document key"),
		queryInput:        buildPresetQueryInputSchema(),
		queryOutput:       buildPresetQueryOutputSchema(),
		queueEnqueueInput: buildPresetQueueInputSchema(kind.Schema),
		queueOutput:       buildPresetQueueOutputSchema(),
		attachmentsInput:  buildPresetAttachmentGetInputSchema(),
		attachmentsOutput: buildPresetAttachmentGetOutputSchema(),
	}, nil
}

func (s *server) registerPresetHelpTool(srv *mcpsdk.Server, def presetcfg.Definition) {
	name := def.Name + ".help"
	tool := &mcpsdk.Tool{
		Name:        name,
		Description: fmt.Sprintf("Summarize the generated %s preset MCP tools.", def.Name),
		Annotations: &mcpsdk.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: boolRef(false)},
		InputSchema: objectSchema(map[string]*jsonschema.Schema{}, nil),
		OutputSchema: objectSchema(map[string]*jsonschema.Schema{
			"preset":      scalarSchema("string", "Preset name"),
			"description": scalarSchema("string", "Preset description"),
			"tools": {
				Type:  "array",
				Items: toolNameSchema(),
			},
		}, []string{"preset", "tools"}),
	}
	mcpsdk.AddTool(srv, tool, withObservedTool(s, name, func(_ context.Context, _ *mcpsdk.CallToolRequest, _ map[string]any) (*mcpsdk.CallToolResult, map[string]any, error) {
		tools := []string{name}
		for _, kind := range def.Kinds {
			for _, op := range kind.Operations {
				tools = append(tools, def.Name+"."+kind.Name+"."+string(op))
			}
		}
		sort.Strings(tools)
		return nil, map[string]any{
			"preset":      def.Name,
			"description": def.Description,
			"tools":       tools,
		}, nil
	}))
}

func (s *server) addPresetQueryTool(srv *mcpsdk.Server, def presetcfg.Definition, rt *presetRuntimeKind) {
	name := rt.kind.Tools.Query
	tool := &mcpsdk.Tool{
		Name:         name,
		Description:  fmt.Sprintf("Query %s %s keys in namespace %s.", def.Name, rt.kind.Name, rt.kind.Namespace),
		Annotations:  &mcpsdk.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: boolRef(false)},
		InputSchema:  rt.queryInput,
		OutputSchema: rt.queryOutput,
	}
	mcpsdk.AddTool(srv, tool, withObservedTool(s, name, func(ctx context.Context, req *mcpsdk.CallToolRequest, input map[string]any) (*mcpsdk.CallToolResult, map[string]any, error) {
		_, out, err := s.handleQueryTool(ctx, req, queryToolInput{
			Query:     mapString(input, "query"),
			Cursor:    mapString(input, "cursor"),
			Engine:    mapString(input, "engine"),
			Refresh:   mapString(input, "refresh"),
			Limit:     mapInt(input, "limit"),
			Fields:    mapAnyMap(input, "fields"),
			Namespace: rt.kind.Namespace,
		})
		if err != nil {
			return nil, nil, err
		}
		return nil, map[string]any{
			"keys":      out.Keys,
			"cursor":    out.Cursor,
			"index_seq": out.IndexSeq,
			"metadata":  out.Metadata,
		}, nil
	}))
}

func (s *server) addPresetStatePutTool(srv *mcpsdk.Server, def presetcfg.Definition, rt *presetRuntimeKind) {
	name := rt.kind.Tools.StatePut
	tool := &mcpsdk.Tool{
		Name:         name,
		Description:  fmt.Sprintf("Create or replace a %s %s document.", def.Name, rt.kind.Name),
		Annotations:  &mcpsdk.ToolAnnotations{OpenWorldHint: boolRef(false)},
		InputSchema:  rt.statePutInput,
		OutputSchema: rt.stateOutput,
	}
	mcpsdk.AddTool(srv, tool, withObservedTool(s, name, func(ctx context.Context, req *mcpsdk.CallToolRequest, input map[string]any) (*mcpsdk.CallToolResult, map[string]any, error) {
		key := mapString(input, "key")
		doc := presetDocumentFromInput(rt.kind.Schema, input)
		payload, err := json.Marshal(doc)
		if err != nil {
			return nil, nil, fmt.Errorf("encode preset document: %w", err)
		}
		if _, _, err := s.handleStatePutTool(ctx, req, statePutToolInput{
			Key:         key,
			Namespace:   rt.kind.Namespace,
			PayloadText: string(payload),
		}); err != nil {
			return nil, nil, err
		}
		out, err := s.loadPresetStateOutput(ctx, req, rt, key)
		return nil, out, err
	}))
}

func (s *server) addPresetStateGetTool(srv *mcpsdk.Server, def presetcfg.Definition, rt *presetRuntimeKind) {
	name := rt.kind.Tools.StateGet
	tool := &mcpsdk.Tool{
		Name:         name,
		Description:  fmt.Sprintf("Read one %s %s document.", def.Name, rt.kind.Name),
		Annotations:  &mcpsdk.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: boolRef(false)},
		InputSchema:  rt.stateGetInput,
		OutputSchema: rt.stateOutput,
	}
	mcpsdk.AddTool(srv, tool, withObservedTool(s, name, func(ctx context.Context, req *mcpsdk.CallToolRequest, input map[string]any) (*mcpsdk.CallToolResult, map[string]any, error) {
		out, err := s.loadPresetStateOutput(ctx, req, rt, mapString(input, "key"))
		return nil, out, err
	}))
}

func (s *server) addPresetStateDeleteTool(srv *mcpsdk.Server, def presetcfg.Definition, rt *presetRuntimeKind) {
	name := rt.kind.Tools.StateDelete
	tool := &mcpsdk.Tool{
		Name:        name,
		Description: fmt.Sprintf("Delete one %s %s document.", def.Name, rt.kind.Name),
		Annotations: &mcpsdk.ToolAnnotations{DestructiveHint: boolRef(true), OpenWorldHint: boolRef(false)},
		InputSchema: rt.stateDeleteInput,
		OutputSchema: objectSchema(map[string]*jsonschema.Schema{
			"_lockd_key": scalarSchema("string", "Document key"),
			"removed":    scalarSchema("boolean", "Whether the document was removed"),
		}, []string{"_lockd_key", "removed"}),
	}
	mcpsdk.AddTool(srv, tool, withObservedTool(s, name, func(ctx context.Context, req *mcpsdk.CallToolRequest, input map[string]any) (*mcpsdk.CallToolResult, map[string]any, error) {
		key := mapString(input, "key")
		_, out, err := s.handleStateDeleteTool(ctx, req, stateDeleteToolInput{
			Key:       key,
			Namespace: rt.kind.Namespace,
		})
		if err != nil {
			return nil, nil, err
		}
		return nil, map[string]any{
			"_lockd_key": key,
			"removed":    out.Removed,
		}, nil
	}))
}

func (s *server) addPresetQueueEnqueueTool(srv *mcpsdk.Server, def presetcfg.Definition, rt *presetRuntimeKind) {
	name := rt.kind.Tools.QueueEnqueue
	tool := &mcpsdk.Tool{
		Name:         name,
		Description:  fmt.Sprintf("Enqueue a %s %s payload into a queue.", def.Name, rt.kind.Name),
		Annotations:  &mcpsdk.ToolAnnotations{OpenWorldHint: boolRef(false)},
		InputSchema:  rt.queueEnqueueInput,
		OutputSchema: rt.queueOutput,
	}
	mcpsdk.AddTool(srv, tool, withObservedTool(s, name, func(ctx context.Context, req *mcpsdk.CallToolRequest, input map[string]any) (*mcpsdk.CallToolResult, map[string]any, error) {
		doc := presetDocumentFromInput(rt.kind.Schema, input)
		payload, err := json.Marshal(doc)
		if err != nil {
			return nil, nil, fmt.Errorf("encode preset queue payload: %w", err)
		}
		_, out, err := s.handleQueueEnqueueTool(ctx, req, queueEnqueueToolInput{
			Queue:             mapString(input, "queue"),
			Namespace:         rt.kind.Namespace,
			PayloadText:       string(payload),
			DelaySeconds:      mapInt64(input, "delay_seconds"),
			VisibilitySeconds: mapInt64(input, "visibility_seconds"),
			TTLSeconds:        mapInt64(input, "ttl_seconds"),
			MaxAttempts:       mapInt(input, "max_attempts"),
			Attributes:        mapAnyMap(input, "attributes"),
		})
		if err != nil {
			return nil, nil, err
		}
		return nil, map[string]any{
			"queue":                      out.Queue,
			"message_id":                 out.MessageID,
			"attempts":                   out.Attempts,
			"max_attempts":               out.MaxAttempts,
			"failure_attempts":           out.FailureAttempts,
			"not_visible_until_unix":     out.NotVisibleUntil,
			"visibility_timeout_seconds": out.VisibilitySeconds,
			"payload_bytes":              out.PayloadBytes,
			"correlation_id":             out.CorrelationID,
		}, nil
	}))
}

func (s *server) addPresetAttachmentsGetTool(srv *mcpsdk.Server, def presetcfg.Definition, rt *presetRuntimeKind) {
	name := rt.kind.Tools.AttachmentsGet
	tool := &mcpsdk.Tool{
		Name:         name,
		Description:  fmt.Sprintf("Retrieve one attachment associated with a %s %s document.", def.Name, rt.kind.Name),
		Annotations:  &mcpsdk.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: boolRef(false)},
		InputSchema:  rt.attachmentsInput,
		OutputSchema: rt.attachmentsOutput,
	}
	mcpsdk.AddTool(srv, tool, withObservedTool(s, name, func(ctx context.Context, req *mcpsdk.CallToolRequest, input map[string]any) (*mcpsdk.CallToolResult, map[string]any, error) {
		_, out, err := s.handleAttachmentGetTool(ctx, req, attachmentGetToolInput{
			Key:         mapString(input, "key"),
			Name:        mapString(input, "name"),
			PayloadMode: mapString(input, "payload_mode"),
			Namespace:   rt.kind.Namespace,
		})
		if err != nil {
			return nil, nil, err
		}
		return nil, map[string]any{
			"key":                      out.Key,
			"attachment":               presetAttachmentInfoMap(out.Attachment),
			"payload_mode":             out.PayloadMode,
			"payload_bytes":            out.PayloadBytes,
			"payload_sha256":           out.PayloadSHA256,
			"payload_text":             out.PayloadText,
			"payload_base64":           out.PayloadBase64,
			"download_url":             out.DownloadURL,
			"download_method":          out.DownloadMethod,
			"download_expires_at_unix": out.DownloadExpiresAtUnix,
		}, nil
	}))
}

func (s *server) loadPresetStateOutput(ctx context.Context, req *mcpsdk.CallToolRequest, rt *presetRuntimeKind, key string) (map[string]any, error) {
	resp, err := s.upstream.Get(ctx, key, lockdclient.WithGetNamespace(rt.kind.Namespace))
	if err != nil {
		return nil, err
	}
	if !resp.HasState {
		return nil, fmt.Errorf("preset state %q has no document", key)
	}
	reader := resp.Reader()
	defer reader.Close()
	var document map[string]any
	dec := json.NewDecoder(reader)
	if err := dec.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode preset state %q: %w", key, err)
	}
	if err := rt.documentResolved.Validate(&document); err != nil {
		return nil, fmt.Errorf("preset state %q does not match schema: %w", key, err)
	}
	out := map[string]any{
		"_lockd_key": key,
	}
	for prop := range rt.kind.Schema.Properties {
		if value, ok := document[prop]; ok {
			out[prop] = value
		}
	}
	list, err := s.upstream.ListAttachments(ctx, lockdclient.ListAttachmentsRequest{
		Namespace: rt.kind.Namespace,
		Key:       key,
		Public:    true,
	})
	if err == nil && len(list.Attachments) > 0 {
		items := make([]map[string]any, 0, len(list.Attachments))
		for _, item := range list.Attachments {
			items = append(items, map[string]any{
				"name":             item.Name,
				"content_type":     item.ContentType,
				"size_bytes":       item.Size,
				"plaintext_sha256": item.PlaintextSHA256,
			})
		}
		out["_lockd_attachments"] = items
	}
	return out, nil
}

func presetSchemaToJSONSchema(schema presetcfg.Schema) *jsonschema.Schema {
	out := &jsonschema.Schema{
		Type:        schema.Type,
		Description: schema.Description,
	}
	switch schema.Type {
	case "object":
		out.Properties = make(map[string]*jsonschema.Schema, len(schema.Properties))
		for key, prop := range schema.Properties {
			out.Properties[key] = presetSchemaToJSONSchema(prop)
			out.PropertyOrder = append(out.PropertyOrder, key)
		}
		sort.Strings(out.PropertyOrder)
		out.Required = append([]string(nil), schema.Required...)
		out.AdditionalProperties = falseSchema()
	case "array":
		out.Items = presetSchemaToJSONSchema(*schema.Items)
	}
	return out
}

func buildPresetStatePutInputSchema(schema presetcfg.Schema) *jsonschema.Schema {
	props := map[string]*jsonschema.Schema{
		"key": scalarSchema("string", "Preset document key"),
	}
	required := []string{"key"}
	for key, prop := range schema.Properties {
		props[key] = presetSchemaToJSONSchema(prop)
	}
	required = append(required, schema.Required...)
	return objectSchema(props, required)
}

func buildPresetStateOutputSchema(schema presetcfg.Schema) *jsonschema.Schema {
	props := map[string]*jsonschema.Schema{
		"_lockd_key": scalarSchema("string", "Document key"),
		"_lockd_attachments": {
			Type: "array",
			Items: objectSchema(map[string]*jsonschema.Schema{
				"name":             scalarSchema("string", "Attachment name"),
				"content_type":     scalarSchema("string", "Attachment content type"),
				"size_bytes":       scalarSchema("integer", "Attachment size in bytes"),
				"plaintext_sha256": scalarSchema("string", "Attachment SHA-256 checksum"),
			}, []string{"name"}),
		},
	}
	required := []string{"_lockd_key"}
	for key, prop := range schema.Properties {
		props[key] = presetSchemaToJSONSchema(prop)
	}
	required = append(required, schema.Required...)
	return objectSchema(props, required)
}

func buildPresetKeyOnlyInputSchema(keyDescription string) *jsonschema.Schema {
	return objectSchema(map[string]*jsonschema.Schema{
		"key": scalarSchema("string", keyDescription),
	}, []string{"key"})
}

func buildPresetQueryInputSchema() *jsonschema.Schema {
	return objectSchema(map[string]*jsonschema.Schema{
		"query":   scalarSchema("string", "LQL query expression"),
		"limit":   scalarSchema("integer", "Maximum rows to return"),
		"cursor":  scalarSchema("string", "Continuation cursor"),
		"engine":  scalarSchema("string", "Query engine override"),
		"refresh": scalarSchema("string", "Refresh policy"),
		"fields": {
			Type:                 "object",
			AdditionalProperties: &jsonschema.Schema{},
		},
	}, []string{"query"})
}

func buildPresetQueryOutputSchema() *jsonschema.Schema {
	return objectSchema(map[string]*jsonschema.Schema{
		"keys": {
			Type:  "array",
			Items: scalarSchema("string", "Matched key"),
		},
		"cursor":    scalarSchema("string", "Continuation cursor"),
		"index_seq": scalarSchema("integer", "Observed index sequence"),
		"metadata": {
			Type:                 "object",
			AdditionalProperties: scalarSchema("string", "Query metadata value"),
		},
	}, nil)
}

func buildPresetQueueInputSchema(schema presetcfg.Schema) *jsonschema.Schema {
	props := map[string]*jsonschema.Schema{
		"queue":              scalarSchema("string", "Queue name"),
		"delay_seconds":      scalarSchema("integer", "Initial invisibility delay"),
		"visibility_seconds": scalarSchema("integer", "Visibility timeout"),
		"ttl_seconds":        scalarSchema("integer", "Message retention TTL"),
		"max_attempts":       scalarSchema("integer", "Maximum failed attempts"),
		"attributes": {
			Type:                 "object",
			AdditionalProperties: &jsonschema.Schema{},
		},
	}
	for key, prop := range schema.Properties {
		props[key] = presetSchemaToJSONSchema(prop)
	}
	return objectSchema(props, schema.Required)
}

func buildPresetQueueOutputSchema() *jsonschema.Schema {
	return objectSchema(map[string]*jsonschema.Schema{
		"queue":                      scalarSchema("string", "Queue name"),
		"message_id":                 scalarSchema("string", "Message ID"),
		"attempts":                   scalarSchema("integer", "Attempt count"),
		"max_attempts":               scalarSchema("integer", "Maximum attempts"),
		"failure_attempts":           scalarSchema("integer", "Failure attempts"),
		"not_visible_until_unix":     scalarSchema("integer", "Visibility deadline"),
		"visibility_timeout_seconds": scalarSchema("integer", "Visibility timeout"),
		"payload_bytes":              scalarSchema("integer", "Payload bytes"),
		"correlation_id":             scalarSchema("string", "Correlation ID"),
	}, []string{"queue", "message_id"})
}

func buildPresetAttachmentGetInputSchema() *jsonschema.Schema {
	return objectSchema(map[string]*jsonschema.Schema{
		"key":          scalarSchema("string", "Document key"),
		"name":         scalarSchema("string", "Attachment name"),
		"payload_mode": scalarSchema("string", "Payload mode: auto, inline, stream, or none"),
	}, []string{"key", "name"})
}

func buildPresetAttachmentGetOutputSchema() *jsonschema.Schema {
	return objectSchema(map[string]*jsonschema.Schema{
		"key": scalarSchema("string", "Document key"),
		"attachment": objectSchema(map[string]*jsonschema.Schema{
			"id":               scalarSchema("string", "Attachment id"),
			"name":             scalarSchema("string", "Attachment name"),
			"size":             scalarSchema("integer", "Attachment size"),
			"plaintext_sha256": scalarSchema("string", "Attachment SHA-256 checksum"),
			"content_type":     scalarSchema("string", "Attachment content type"),
			"created_at_unix":  scalarSchema("integer", "Creation timestamp"),
			"updated_at_unix":  scalarSchema("integer", "Update timestamp"),
		}, []string{"id", "name", "size"}),
		"payload_mode":             scalarSchema("string", "Returned payload mode"),
		"payload_bytes":            scalarSchema("integer", "Payload size"),
		"payload_sha256":           scalarSchema("string", "Payload SHA-256"),
		"payload_text":             scalarSchema("string", "Inline UTF-8 payload"),
		"payload_base64":           scalarSchema("string", "Inline base64 payload"),
		"download_url":             scalarSchema("string", "Download URL"),
		"download_method":          scalarSchema("string", "Download method"),
		"download_expires_at_unix": scalarSchema("integer", "Download URL expiry"),
	}, []string{"key", "attachment", "payload_mode", "payload_bytes"})
}

func objectSchema(props map[string]*jsonschema.Schema, required []string) *jsonschema.Schema {
	order := make([]string, 0, len(props))
	for key := range props {
		order = append(order, key)
	}
	sort.Strings(order)
	return &jsonschema.Schema{
		Type:                 "object",
		Properties:           props,
		Required:             append([]string(nil), required...),
		AdditionalProperties: falseSchema(),
		PropertyOrder:        order,
	}
}

func scalarSchema(typeName, desc string) *jsonschema.Schema {
	return &jsonschema.Schema{Type: typeName, Description: desc}
}

func toolNameSchema() *jsonschema.Schema {
	return &jsonschema.Schema{Type: "string", Description: "Generated preset tool name"}
}

func falseSchema() *jsonschema.Schema {
	return &jsonschema.Schema{Not: &jsonschema.Schema{}}
}

func presetDocumentFromInput(schema presetcfg.Schema, input map[string]any) map[string]any {
	out := make(map[string]any, len(schema.Properties))
	for key := range schema.Properties {
		if value, ok := input[key]; ok {
			out[key] = value
		}
	}
	return out
}

func presetAttachmentInfoMap(in attachmentInfoOutput) map[string]any {
	return map[string]any{
		"id":               in.ID,
		"name":             in.Name,
		"size":             in.Size,
		"plaintext_sha256": in.PlaintextSHA256,
		"content_type":     in.ContentType,
		"created_at_unix":  in.CreatedAtUnix,
		"updated_at_unix":  in.UpdatedAtUnix,
	}
}

func mapString(input map[string]any, key string) string {
	value, ok := input[key]
	if !ok || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func mapAnyMap(input map[string]any, key string) map[string]any {
	value, ok := input[key]
	if !ok || value == nil {
		return nil
	}
	v, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	return cloneAnyMap(v)
}

func mapInt(input map[string]any, key string) int {
	return int(mapInt64(input, key))
}

func mapInt64(input map[string]any, key string) int64 {
	value, ok := input[key]
	if !ok || value == nil {
		return 0
	}
	switch v := value.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case json.Number:
		n, _ := v.Int64()
		return n
	default:
		return 0
	}
}

func decodeJSONObject(reader io.Reader) (map[string]any, error) {
	var out map[string]any
	dec := json.NewDecoder(reader)
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}
