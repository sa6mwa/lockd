package mcpsuite

import (
	"context"
	"errors"
	"iter"
	"slices"
	"sync"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/adk/agent"
	"google.golang.org/adk/artifact"
	adkmemory "google.golang.org/adk/memory"
	adksession "google.golang.org/adk/session"
	adktool "google.golang.org/adk/tool"
	"google.golang.org/adk/tool/mcptoolset"
	"google.golang.org/adk/tool/toolconfirmation"
	"google.golang.org/genai"

	"pkt.systems/lockd/internal/uuidv7"
)

type adkRunnableTool interface {
	Run(ctx adktool.Context, args any) (map[string]any, error)
}

// RunGoogleADKE2E verifies lockd MCP compatibility via Google's ADK MCP toolset.
func RunGoogleADKE2E(t *testing.T, factory ServerFactory) {
	t.Helper()
	if factory == nil {
		t.Fatal("mcp suite requires a server factory")
	}
	ts := factory(t)
	if ts == nil {
		t.Fatal("mcp suite server factory returned nil test server")
	}

	facade := startMCPFacade(t, ts)
	defer facade.stop(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	accessToken := issueClientCredentialsToken(ctx, t, facade, facade.clientID, facade.clientSecret)

	toolset, err := mcptoolset.New(mcptoolset.Config{
		Transport: &mcpsdk.StreamableClientTransport{
			Endpoint:   facade.endpoint,
			HTTPClient: facade.authHTTPClient(accessToken),
		},
	})
	if err != nil {
		t.Fatalf("new adk mcp toolset: %v", err)
	}

	roCtx := newADKReadonlyContext(ctx)
	tools := listADKToolsWithRetry(ctx, t, toolset, roCtx)
	requireADKToolsPresent(t, tools, "lockd.hint", "lockd.lock.acquire", "lockd.lock.release")

	toolCtx := newADKToolContext(ctx)

	hintTool := requireADKRunnableTool(t, tools, "lockd.hint")
	hintRaw, err := hintTool.Run(toolCtx, map[string]any{})
	if err != nil {
		t.Fatalf("run lockd.hint via adk: %v", err)
	}
	hintOut := requireADKToolOutputObject(t, hintRaw)
	if ns := asString(hintOut["default_namespace"]); ns != "mcp" {
		t.Fatalf("expected adk hint default_namespace=mcp, got %q", ns)
	}

	key := "mcp-adk-e2e-" + uuidv7.NewString()
	acquireTool := requireADKRunnableTool(t, tools, "lockd.lock.acquire")
	acquireRaw, err := acquireTool.Run(toolCtx, map[string]any{
		"key":           key,
		"owner":         "mcp-adk-e2e",
		"ttl_seconds":   int64(30),
		"block_seconds": int64(-1),
	})
	if err != nil {
		t.Fatalf("run lockd.lock.acquire via adk: %v", err)
	}
	acquireOut := requireADKToolOutputObject(t, acquireRaw)
	leaseID := requireStringField(t, acquireOut, "lease_id")
	txnID := asString(acquireOut["txn_id"])
	if fencing := asInt64(acquireOut["fencing_token"]); fencing <= 0 {
		t.Fatalf("expected positive fencing_token from adk acquire, got %d", fencing)
	}

	releaseTool := requireADKRunnableTool(t, tools, "lockd.lock.release")
	releaseRaw, err := releaseTool.Run(toolCtx, map[string]any{
		"key":      key,
		"lease_id": leaseID,
		"txn_id":   txnID,
	})
	if err != nil {
		t.Fatalf("run lockd.lock.release via adk: %v", err)
	}
	releaseOut := requireADKToolOutputObject(t, releaseRaw)
	if !asBool(releaseOut["released"]) {
		t.Fatalf("expected released=true from adk release, got %+v", releaseOut)
	}
}

func requireADKToolsPresent(t testing.TB, tools []adktool.Tool, names ...string) {
	t.Helper()
	available := make([]string, 0, len(tools))
	for _, tool := range tools {
		if tool == nil {
			continue
		}
		available = append(available, tool.Name())
	}
	for _, name := range names {
		if !slices.Contains(available, name) {
			t.Fatalf("missing expected adk tool %q in tool list: %v", name, available)
		}
	}
}

func requireADKRunnableTool(t testing.TB, tools []adktool.Tool, name string) adkRunnableTool {
	t.Helper()
	for _, tool := range tools {
		if tool == nil || tool.Name() != name {
			continue
		}
		runner, ok := tool.(adkRunnableTool)
		if !ok {
			t.Fatalf("adk tool %q does not implement Run(tool.Context, any)", name)
		}
		return runner
	}
	t.Fatalf("adk tool %q not found", name)
	return nil
}

func requireADKToolOutputObject(t testing.TB, out map[string]any) map[string]any {
	t.Helper()
	if len(out) == 0 {
		t.Fatalf("expected adk tool output, got empty map")
	}
	output := asObject(out["output"])
	if len(output) == 0 {
		t.Fatalf("expected structured adk output field, got %+v", out)
	}
	return output
}

func listADKToolsWithRetry(ctx context.Context, t testing.TB, toolset adktool.Toolset, roCtx agent.ReadonlyContext) []adktool.Tool {
	t.Helper()
	if toolset == nil {
		t.Fatalf("adk toolset required")
	}
	var (
		tools   []adktool.Tool
		lastErr error
	)
	for attempt := 0; attempt < 50; attempt++ {
		tools, lastErr = toolset.Tools(roCtx)
		if lastErr == nil {
			return tools
		}
		select {
		case <-ctx.Done():
			t.Fatalf("list adk mcp tools canceled: %v (last error: %v)", ctx.Err(), lastErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("list adk mcp tools: %v", lastErr)
	return nil
}

type adkTestReadonlyContext struct {
	context.Context
	invocationID string
	agentName    string
	userID       string
	appName      string
	sessionID    string
	branch       string
	state        *adkTestState
}

func newADKReadonlyContext(ctx context.Context) *adkTestReadonlyContext {
	base := ctx
	if base == nil {
		base = context.Background()
	}
	return &adkTestReadonlyContext{
		Context:      base,
		invocationID: "inv-" + uuidv7.NewString(),
		agentName:    "lockd-mcp-adk-test",
		userID:       "adk-test-user",
		appName:      "lockd-mcp-tests",
		sessionID:    "sess-" + uuidv7.NewString(),
		branch:       "root",
		state:        newADKTestState(),
	}
}

func (c *adkTestReadonlyContext) UserContent() *genai.Content { return nil }
func (c *adkTestReadonlyContext) InvocationID() string        { return c.invocationID }
func (c *adkTestReadonlyContext) AgentName() string           { return c.agentName }
func (c *adkTestReadonlyContext) ReadonlyState() adksession.ReadonlyState {
	return c.state
}
func (c *adkTestReadonlyContext) UserID() string    { return c.userID }
func (c *adkTestReadonlyContext) AppName() string   { return c.appName }
func (c *adkTestReadonlyContext) SessionID() string { return c.sessionID }
func (c *adkTestReadonlyContext) Branch() string    { return c.branch }

type adkTestToolContext struct {
	*adkTestReadonlyContext
	functionCallID string
	actions        *adksession.EventActions
	artifacts      agent.Artifacts
}

func newADKToolContext(ctx context.Context) *adkTestToolContext {
	ro := newADKReadonlyContext(ctx)
	return &adkTestToolContext{
		adkTestReadonlyContext: ro,
		functionCallID:         "fc-" + uuidv7.NewString(),
		actions: &adksession.EventActions{
			StateDelta: make(map[string]any),
		},
		artifacts: adkNoopArtifacts{},
	}
}

func (c *adkTestToolContext) Artifacts() agent.Artifacts { return c.artifacts }
func (c *adkTestToolContext) State() adksession.State    { return c.state }
func (c *adkTestToolContext) FunctionCallID() string     { return c.functionCallID }
func (c *adkTestToolContext) Actions() *adksession.EventActions {
	return c.actions
}
func (c *adkTestToolContext) SearchMemory(context.Context, string) (*adkmemory.SearchResponse, error) {
	return nil, errors.New("search memory not supported in adk mcp test context")
}
func (c *adkTestToolContext) ToolConfirmation() *toolconfirmation.ToolConfirmation { return nil }
func (c *adkTestToolContext) RequestConfirmation(string, any) error                { return nil }

type adkNoopArtifacts struct{}

func (adkNoopArtifacts) Save(context.Context, string, *genai.Part) (*artifact.SaveResponse, error) {
	return nil, errors.New("artifacts save not supported in adk mcp test context")
}

func (adkNoopArtifacts) List(context.Context) (*artifact.ListResponse, error) {
	return nil, errors.New("artifacts list not supported in adk mcp test context")
}

func (adkNoopArtifacts) Load(context.Context, string) (*artifact.LoadResponse, error) {
	return nil, errors.New("artifacts load not supported in adk mcp test context")
}

func (adkNoopArtifacts) LoadVersion(context.Context, string, int) (*artifact.LoadResponse, error) {
	return nil, errors.New("artifacts load version not supported in adk mcp test context")
}

type adkTestState struct {
	mu     sync.RWMutex
	values map[string]any
}

func newADKTestState() *adkTestState {
	return &adkTestState{
		values: make(map[string]any),
	}
}

func (s *adkTestState) Get(key string) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.values[key]
	if !ok {
		return nil, adksession.ErrStateKeyNotExist
	}
	return val, nil
}

func (s *adkTestState) Set(key string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = value
	return nil
}

func (s *adkTestState) All() iter.Seq2[string, any] {
	s.mu.RLock()
	snapshot := make(map[string]any, len(s.values))
	for k, v := range s.values {
		snapshot[k] = v
	}
	s.mu.RUnlock()
	return func(yield func(string, any) bool) {
		for k, v := range snapshot {
			if !yield(k, v) {
				return
			}
		}
	}
}

var (
	_ agent.ReadonlyContext = (*adkTestReadonlyContext)(nil)
	_ adktool.Context       = (*adkTestToolContext)(nil)
	_ agent.Artifacts       = adkNoopArtifacts{}
	_ adksession.State      = (*adkTestState)(nil)
)
