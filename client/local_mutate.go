package client

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"pkt.systems/lql"
)

// MutateLocalRequest describes a client-local streaming LQL mutation flow.
//
// The client loads the lease-visible JSON state, applies mutations locally via
// lql.MutateStream, and streams the mutated JSON back through UpdateStream.
// This is the path that supports file:/textfile:/base64file: mutators.
type MutateLocalRequest struct {
	// Key identifies the state object to mutate.
	Key string
	// LeaseID identifies the active lease required for protected mutations.
	LeaseID string
	// Mutations contains one or more LQL mutation expressions in execution order.
	Mutations []string
	// Options controls namespace, CAS guards, fencing, and txn headers.
	Options UpdateOptions
	// DisableFetchedCAS skips defaulting IfETag/IfVersion from the streamed Get.
	DisableFetchedCAS bool
	// FileValueBaseDir resolves relative file:/textfile:/base64file: paths.
	FileValueBaseDir string
	// FileValueResolver overrides file opening for tests and custom callers.
	FileValueResolver lql.MutationFileValueResolver
}

// MutateLocalOptions customizes session-local streaming mutation helpers.
type MutateLocalOptions struct {
	// Update controls namespace, CAS guards, fencing, and txn headers.
	Update UpdateOptions
	// DisableFetchedCAS skips defaulting IfETag/IfVersion from the streamed Get.
	DisableFetchedCAS bool
	// FileValueBaseDir resolves relative file:/textfile:/base64file: paths.
	FileValueBaseDir string
	// FileValueResolver overrides file opening for tests and custom callers.
	FileValueResolver lql.MutationFileValueResolver
}

// MutateLocal applies LQL mutations client-side without buffering the full JSON
// state in memory. This path supports file-backed LQL mutators.
func (c *Client) MutateLocal(ctx context.Context, req MutateLocalRequest) (*UpdateResult, error) {
	key := strings.TrimSpace(req.Key)
	if key == "" {
		return nil, fmt.Errorf("lockd: key required")
	}
	leaseID := strings.TrimSpace(req.LeaseID)
	if leaseID == "" {
		return nil, fmt.Errorf("lockd: lease id required")
	}
	mutationExprs := trimMutationExpressions(req.Mutations)
	if len(mutationExprs) == 0 {
		return nil, fmt.Errorf("lockd: mutations required")
	}

	opts := req.Options
	namespace, err := c.namespaceFor(opts.Namespace)
	if err != nil {
		return nil, err
	}
	opts.Namespace = namespace
	if opts.TxnID == "" {
		if sess := c.sessionByLease(leaseID); sess != nil {
			opts.TxnID = sess.TxnID
		}
	}
	token, err := c.fencingToken(leaseID, opts.FencingToken)
	if err != nil {
		return nil, err
	}
	opts.FencingToken = Int64(token)
	c.RegisterLeaseToken(leaseID, token)

	mutations, err := lql.ParseMutationsWithOptions(mutationExprs, time.Now(), lql.ParseMutationsOptions{
		EnableFileValues:  true,
		FileValueBaseDir:  strings.TrimSpace(req.FileValueBaseDir),
		FileValueResolver: req.FileValueResolver,
	})
	if err != nil {
		return nil, err
	}
	plan, err := lql.NewMutateStreamPlan(mutations)
	if err != nil {
		return nil, err
	}

	c.logTraceCtx(ctx, "client.mutate_local.start", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "endpoint", c.lastEndpoint, "fencing_token", token)
	current, err := c.Get(ctx, key,
		WithGetNamespace(namespace),
		WithGetLeaseID(leaseID),
		WithGetPublicDisabled(true),
	)
	if err != nil {
		c.logErrorCtx(ctx, "client.mutate_local.get_error", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "endpoint", c.lastEndpoint, "fencing_token", token, "error", err)
		return nil, err
	}

	source := io.NopCloser(strings.NewReader(`{}`))
	if current != nil {
		defer current.Close()
		if !req.DisableFetchedCAS {
			if opts.IfETag == "" {
				opts.IfETag = current.ETag
			}
			if opts.IfVersion == nil {
				if version, parseErr := parseKeyVersionHeader(current.Version); parseErr != nil {
					return nil, parseErr
				} else if version > 0 {
					opts.IfVersion = Int64(version)
				}
			}
		}
		if current.HasState {
			if reader := current.detachReader(); reader != nil {
				source = reader
			}
		}
	}

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	pipeReader, pipeWriter := io.Pipe()
	mutateErrCh := make(chan error, 1)
	go func() {
		defer source.Close()
		_, runErr := lql.MutateStreamWithResult(lql.MutateStreamRequest{
			Ctx:    streamCtx,
			Reader: source,
			Writer: pipeWriter,
			Mode:   lql.MutateSingleObjectOnly,
			Plan:   plan,
		})
		if runErr != nil {
			_ = pipeWriter.CloseWithError(runErr)
			mutateErrCh <- runErr
			return
		}
		_ = pipeWriter.Close()
		mutateErrCh <- nil
	}()

	result, updateErr := c.UpdateStream(streamCtx, key, leaseID, pipeReader, opts)
	if updateErr != nil {
		cancel()
		_ = pipeReader.Close()
		if mutateErr := <-mutateErrCh; mutateErr != nil {
			c.logErrorCtx(ctx, "client.mutate_local.error", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "endpoint", c.lastEndpoint, "fencing_token", token, "error", mutateErr)
			return nil, mutateErr
		}
		c.logErrorCtx(ctx, "client.mutate_local.error", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "endpoint", c.lastEndpoint, "fencing_token", token, "error", updateErr)
		return nil, updateErr
	}
	if mutateErr := <-mutateErrCh; mutateErr != nil {
		c.logErrorCtx(ctx, "client.mutate_local.error", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "endpoint", c.lastEndpoint, "fencing_token", token, "error", mutateErr)
		return nil, mutateErr
	}
	c.logTraceCtx(ctx, "client.mutate_local.success", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "endpoint", c.lastEndpoint, "fencing_token", token, "new_version", result.NewVersion, "new_etag", result.NewStateETag)
	return result, nil
}

// MutateLocal applies client-local streaming LQL mutations while preserving the state lease.
func (s *QueueStateHandle) MutateLocal(ctx context.Context, mutations []string, options MutateLocalOptions) (*UpdateResult, error) {
	cli, namespace, key, leaseID, txnID, corr, _, _, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	ctx = WithCorrelationID(ctx, corr)
	opts := options.Update
	if opts.FencingToken == nil {
		opts.FencingToken = s.syncFencingToken()
	}
	if opts.Namespace == "" {
		opts.Namespace = namespace
	}
	if opts.TxnID == "" {
		opts.TxnID = txnID
	}
	res, err := cli.MutateLocal(ctx, MutateLocalRequest{
		Key:               key,
		LeaseID:           leaseID,
		Mutations:         append([]string(nil), mutations...),
		Options:           opts,
		DisableFetchedCAS: options.DisableFetchedCAS,
		FileValueBaseDir:  options.FileValueBaseDir,
		FileValueResolver: options.FileValueResolver,
	})
	if err != nil {
		return nil, err
	}
	if res != nil {
		s.updateAfterMutation(res.NewStateETag, res.NewVersion)
	}
	s.syncFencingToken()
	return res, nil
}

// MutateLocal applies client-local streaming LQL mutations to the session's key while preserving the lease.
func (s *LeaseSession) MutateLocal(ctx context.Context, mutations []string, options MutateLocalOptions) (*UpdateResult, error) {
	ctx = WithCorrelationID(ctx, s.correlation())
	opts := options.Update
	if opts.FencingToken == nil {
		opts.FencingToken = s.syncFencingToken()
	}
	if opts.Namespace == "" {
		opts.Namespace = s.Namespace
	}
	if opts.TxnID == "" {
		opts.TxnID = s.TxnID
	}
	res, err := s.client.MutateLocal(ctx, MutateLocalRequest{
		Key:               s.Key,
		LeaseID:           s.LeaseID,
		Mutations:         append([]string(nil), mutations...),
		Options:           opts,
		DisableFetchedCAS: options.DisableFetchedCAS,
		FileValueBaseDir:  options.FileValueBaseDir,
		FileValueResolver: options.FileValueResolver,
	})
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.Version = res.NewVersion
	s.StateETag = res.NewStateETag
	if token, tokenErr := s.client.fencingToken(s.LeaseID, nil); tokenErr == nil {
		s.FencingToken = token
	}
	s.mu.Unlock()
	return res, nil
}

// MutateLocal applies client-local streaming LQL mutations while preserving the lease.
func (a *AcquireForUpdateContext) MutateLocal(ctx context.Context, mutations []string, options MutateLocalOptions) (*UpdateResult, error) {
	sess, err := a.session()
	if err != nil {
		return nil, err
	}
	return sess.MutateLocal(ctx, mutations, options)
}

func trimMutationExpressions(exprs []string) []string {
	mutationExprs := make([]string, 0, len(exprs))
	for _, raw := range exprs {
		expr := strings.TrimSpace(raw)
		if expr != "" {
			mutationExprs = append(mutationExprs, expr)
		}
	}
	return mutationExprs
}

func parseKeyVersionHeader(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	version, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse fetched version %q: %w", raw, err)
	}
	return version, nil
}
