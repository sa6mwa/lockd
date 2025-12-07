package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/core/transport"
	"pkt.systems/lockd/internal/correlation"
	"pkt.systems/lockd/internal/jsonpointer"
	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/pslog"
)

// correlationAppliedKey marks log enrichment to avoid duplicate correlation fields.
type correlationAppliedKey struct{}

func logQueueDeliveryInfo(logger pslog.Logger, delivery *core.QueueDelivery) {
	if logger == nil || delivery == nil {
		return
	}
	msg := delivery.Message
	if msg == nil {
		return
	}
	fields := []any{
		"mid", msg.MessageID,
		"attempts", msg.Attempts,
		"max_attempts", msg.MaxAttempts,
		"lease", msg.LeaseID,
		"fencing", msg.FencingToken,
		"visibility_timeout_seconds", msg.VisibilityTimeoutSeconds,
	}
	if msg.StateLeaseID != "" {
		fields = append(fields,
			"state_lease", msg.StateLeaseID,
			"state_fencing", msg.StateFencingToken,
		)
	}
	if delivery.NextCursor != "" {
		fields = append(fields, "cursor", delivery.NextCursor)
	}
	logger.Info("queue.delivery.sent", fields...)
}

func routerSys(operation string) string {
	parts := strings.FieldsFunc(operation, func(r rune) bool {
		switch r {
		case '.', '/', '-', '_':
			return true
		}
		return false
	})
	if len(parts) == 0 {
		return "api.http.router"
	}
	return "api.http.router." + strings.Join(parts, ".")
}

func applyCorrelation(ctx context.Context, logger pslog.Logger, span trace.Span) (context.Context, pslog.Logger) {
	if id := correlation.ID(ctx); id != "" {
		if ctx.Value(correlationAppliedKey{}) == nil {
			logger = logger.With("cid", id)
			ctx = context.WithValue(ctx, correlationAppliedKey{}, struct{}{})
		} else if existing := pslog.LoggerFromContext(ctx); existing != nil {
			logger = existing
		}
		ctx = pslog.ContextWithLogger(ctx, logger)
		if span != nil {
			span.SetAttributes(attribute.String("lockd.correlation_id", id))
		}
	}
	return ctx, logger
}

func clientIdentityFromRequest(r *http.Request) string {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return ""
	}
	cert := r.TLS.PeerCertificates[0]
	serial := ""
	if cert.SerialNumber != nil {
		serial = cert.SerialNumber.Text(16)
	}
	return fmt.Sprintf("%s#%s", cert.Subject.String(), serial)
}

type limitedWriter struct {
	io.Writer
	limit int64
	wrote int64
}

func (lw *limitedWriter) Write(p []byte) (int, error) {
	if lw.limit > 0 && lw.wrote+int64(len(p)) > lw.limit {
		return 0, httpError{Status: http.StatusRequestEntityTooLarge, Code: "document_too_large", Detail: fmt.Sprintf("state exceeds %d bytes", lw.limit)}
	}
	n, err := lw.Writer.Write(p)
	lw.wrote += int64(n)
	return n, err
}

// ndjsonSink adapts core.DocumentSink to the HTTP NDJSON response writer.
type ndjsonSink struct {
	writer  io.Writer
	flusher http.Flusher
	logger  pslog.Logger
	stream  func(w io.Writer, namespace, key string, version int64, doc io.Reader) error
	ns      string
}

func (s *ndjsonSink) OnDocument(ctx context.Context, namespace, key string, version int64, reader io.Reader) error {
	if err := s.stream(s.writer, namespace, key, version, reader); err != nil {
		return err
	}
	if s.flusher != nil {
		s.flusher.Flush()
	}
	return nil
}

func parseEngineHint(raw string) (search.EngineHint, error) {
	if raw == "" {
		return search.EngineAuto, nil
	}
	switch strings.ToLower(raw) {
	case "auto":
		return search.EngineAuto, nil
	case "index":
		return search.EngineIndex, nil
	case "scan":
		return search.EngineScan, nil
	default:
		return search.EngineAuto, httpError{
			Status: http.StatusBadRequest,
			Code:   "invalid_engine",
			Detail: fmt.Sprintf("unsupported query engine %q", raw),
		}
	}
}

type refreshMode string

const (
	refreshImmediate refreshMode = ""
	refreshWaitFor   refreshMode = "wait_for"
)

func parseRefreshMode(raw string) (refreshMode, error) {
	if raw == "" {
		return refreshImmediate, nil
	}
	switch strings.ToLower(raw) {
	case "wait_for", "wait-for":
		return refreshWaitFor, nil
	default:
		return refreshImmediate, httpError{
			Status: http.StatusBadRequest,
			Code:   "invalid_refresh",
			Detail: fmt.Sprintf("unsupported refresh mode %q", raw),
		}
	}
}

func parseQueryReturnMode(raw string) (api.QueryReturn, error) {
	if raw == "" {
		return api.QueryReturnKeys, nil
	}
	switch strings.ToLower(raw) {
	case "keys":
		return api.QueryReturnKeys, nil
	case "documents", "document", "docs":
		return api.QueryReturnDocuments, nil
	default:
		return api.QueryReturnKeys, httpError{
			Status: http.StatusBadRequest,
			Code:   "invalid_return",
			Detail: fmt.Sprintf("unsupported return mode %q", raw),
		}
	}
}

func normalizeSelectorFields(sel *api.Selector) error {
	if sel == nil {
		return nil
	}
	if sel.Eq != nil {
		normalized, err := jsonpointer.Normalize(sel.Eq.Field)
		if err != nil {
			return fmt.Errorf("eq.field: %w", err)
		}
		sel.Eq.Field = normalized
	}
	if sel.Prefix != nil {
		normalized, err := jsonpointer.Normalize(sel.Prefix.Field)
		if err != nil {
			return fmt.Errorf("prefix.field: %w", err)
		}
		sel.Prefix.Field = normalized
	}
	if sel.Range != nil {
		normalized, err := jsonpointer.Normalize(sel.Range.Field)
		if err != nil {
			return fmt.Errorf("range.field: %w", err)
		}
		sel.Range.Field = normalized
	}
	if sel.In != nil {
		normalized, err := jsonpointer.Normalize(sel.In.Field)
		if err != nil {
			return fmt.Errorf("in.field: %w", err)
		}
		sel.In.Field = normalized
	}
	if sel.Exists != "" {
		normalized, err := jsonpointer.Normalize(sel.Exists)
		if err != nil {
			return fmt.Errorf("exists.field: %w", err)
		}
		sel.Exists = normalized
	}
	for i := range sel.And {
		if err := normalizeSelectorFields(&sel.And[i]); err != nil {
			return err
		}
	}
	for i := range sel.Or {
		if err := normalizeSelectorFields(&sel.Or[i]); err != nil {
			return err
		}
	}
	if sel.Not != nil {
		if err := normalizeSelectorFields(sel.Not); err != nil {
			return err
		}
	}
	return nil
}

// convertCoreError maps transport-neutral core failures onto HTTP-aware errors.
func convertCoreError(err error) error {
	switch {
	case errors.Is(err, storage.ErrCASMismatch):
		return httpError{Status: http.StatusConflict, Code: "cas_mismatch", Detail: "storage cas mismatch"}
	case errors.Is(err, storage.ErrNotFound):
		return httpError{Status: http.StatusNotFound, Code: "not_found", Detail: "resource not found"}
	}
	if httpErr, ok := transport.ToHTTP(err); ok {
		return httpError{
			Status:     httpErr.Status,
			Code:       httpErr.Code,
			Detail:     httpErr.Detail,
			Version:    httpErr.Version,
			ETag:       httpErr.ETag,
			RetryAfter: httpErr.RetryAfter,
		}
	}
	return err
}

func writeQueueDeliveryBatch(w http.ResponseWriter, deliveries []*core.QueueDelivery, defaultCursor string) error {
	mw := multipart.NewWriter(w)
	firstCID := ""
	if len(deliveries) > 0 && deliveries[0] != nil && deliveries[0].Message != nil {
		firstCID = deliveries[0].Message.CorrelationID
	}
	contentType := fmt.Sprintf("multipart/related; boundary=%s", mw.Boundary())
	w.Header().Set("Content-Type", contentType)
	if firstCID != "" {
		w.Header().Set(headerCorrelationID, firstCID)
	}
	w.WriteHeader(http.StatusOK)

	if _, err := writeQueueDeliveriesToWriter(mw, deliveries, defaultCursor); err != nil {
		_ = mw.Close()
		return err
	}
	return mw.Close()
}

func writeQueueDeliveriesToWriter(mw *multipart.Writer, deliveries []*core.QueueDelivery, defaultCursor string) (string, error) {
	sink := &multipartQueueSink{mw: mw, defaultCursor: defaultCursor}
	if err := core.WriteDeliveries(deliveries, defaultCursor, sink); err != nil {
		return "", err
	}
	return sink.firstCID, nil
}

type multipartQueueSink struct {
	mw            *multipart.Writer
	defaultCursor string
	firstCID      string
}

func (s *multipartQueueSink) WriteMeta(idx, total int, d *core.QueueDelivery, defaultCursor string) error {
	if s.mw == nil || d == nil {
		return nil
	}
	metaHeader := textproto.MIMEHeader{}
	metaHeader.Set("Content-Type", "application/json")
	if total > 1 {
		metaHeader.Set("Content-Disposition", fmt.Sprintf(`form-data; name="meta"; filename="meta-%d.json"`, idx))
	} else {
		metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
	}
	metaPart, err := s.mw.CreatePart(metaHeader)
	if err != nil {
		return err
	}
	cursor := d.NextCursor
	if cursor == "" {
		cursor = defaultCursor
	}
	meta := api.DequeueResponse{Message: toAPIDeliveryMessage(d.Message), NextCursor: cursor}
	if err := json.NewEncoder(metaPart).Encode(meta); err != nil {
		return err
	}
	if d.Message != nil && s.firstCID == "" {
		s.firstCID = d.Message.CorrelationID
	}
	return nil
}

func (s *multipartQueueSink) WritePayload(idx, total int, d *core.QueueDelivery) error {
	if s.mw == nil || d == nil || d.Payload == nil {
		return nil
	}
	payloadHeader := textproto.MIMEHeader{}
	ctype := strings.TrimSpace(d.PayloadContentType)
	if ctype == "" {
		ctype = "application/octet-stream"
	}
	payloadHeader.Set("Content-Type", ctype)
	if d.PayloadBytes >= 0 {
		payloadHeader.Set("Content-Length", strconv.FormatInt(d.PayloadBytes, 10))
	}
	if total > 1 {
		payloadHeader.Set("Content-Disposition", fmt.Sprintf(`form-data; name=\"payload\"; filename=\"payload-%d\"`, idx))
	} else {
		payloadHeader.Set("Content-Disposition", `form-data; name=\"payload\"`)
	}
	payloadPart, err := s.mw.CreatePart(payloadHeader)
	if err != nil {
		return err
	}
	return core.PayloadCopy(payloadPart, d)
}

func toAPIDeliveryMessage(msg *core.QueueMessage) *api.Message {
	if msg == nil {
		return nil
	}
	return &api.Message{
		Namespace:                msg.Namespace,
		Queue:                    msg.Queue,
		MessageID:                msg.MessageID,
		Attempts:                 msg.Attempts,
		MaxAttempts:              msg.MaxAttempts,
		NotVisibleUntilUnix:      msg.NotVisibleUntilUnix,
		VisibilityTimeoutSeconds: msg.VisibilityTimeoutSeconds,
		Attributes:               msg.Attributes,
		PayloadContentType:       msg.PayloadContentType,
		PayloadBytes:             msg.PayloadBytes,
		CorrelationID:            msg.CorrelationID,
		LeaseID:                  msg.LeaseID,
		LeaseExpiresAtUnix:       msg.LeaseExpiresAtUnix,
		FencingToken:             msg.FencingToken,
		MetaETag:                 msg.MetaETag,
		StateETag:                msg.StateETag,
		StateLeaseID:             msg.StateLeaseID,
		StateLeaseExpiresAtUnix:  msg.StateLeaseExpiresAtUnix,
		StateFencingToken:        msg.StateFencingToken,
	}
}

type acquireBackoff struct {
	next time.Duration
	rand func(int64) int64
}

func newAcquireBackoff() *acquireBackoff {
	return &acquireBackoff{
		next: acquireBackoffStart,
		rand: backoffRandInt63n,
	}
}

func (b *acquireBackoff) Next(limit time.Duration) time.Duration {
	if b.next <= 0 {
		b.next = acquireBackoffStart
	}
	sleep := b.next
	if limit > 0 && limit < sleep {
		sleep = limit
	}
	sleep = applyBackoffJitter(sleep, limit, b.rand)
	if limit > 0 && sleep > limit {
		sleep = limit
	}
	if sleep < acquireBackoffMin {
		if limit > 0 && limit < acquireBackoffMin {
			sleep = limit
		} else {
			sleep = acquireBackoffMin
		}
	}
	if sleep < 0 {
		sleep = 0
	}
	next := time.Duration(float64(b.next)*acquireBackoffMultiplier + 0.5)
	if next <= 0 {
		next = acquireBackoffStart
	}
	if next > acquireBackoffMax {
		next = acquireBackoffMax
	}
	b.next = next
	return sleep
}

func applyBackoffJitter(base, limit time.Duration, randFn func(int64) int64) time.Duration {
	if acquireBackoffJitter <= 0 || randFn == nil {
		return base
	}
	jitter := acquireBackoffJitter
	if base > 0 && base < jitter {
		jitter = base / 2
	}
	if limit > 0 && limit < jitter {
		jitter = limit / 2
	}
	if jitter <= 0 {
		return base
	}
	max := int64(jitter)*2 + 1
	if max <= 0 {
		return base
	}
	val := randFn(max)
	offset := time.Duration(val) - jitter
	return base + offset
}

func backoffRandInt63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	backoffRandMu.Lock()
	v := backoffRandSrc.Int63n(n)
	backoffRandMu.Unlock()
	return v
}

func parseFencingToken(r *http.Request) (int64, error) {
	value := strings.TrimSpace(r.Header.Get(headerFencingToken))
	if value == "" {
		return 0, httpError{Status: http.StatusBadRequest, Code: "missing_fencing_token", Detail: "X-Fencing-Token header required"}
	}
	token, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, httpError{Status: http.StatusBadRequest, Code: "invalid_fencing_token", Detail: "invalid fencing token"}
	}
	return token, nil
}

func parseBoolQuery(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "on", "yes":
		return true
	default:
		return false
	}
}

func zeroSelector(sel api.Selector) bool {
	return len(sel.And) == 0 &&
		len(sel.Or) == 0 &&
		sel.Not == nil &&
		sel.Eq == nil &&
		sel.Prefix == nil &&
		sel.Range == nil &&
		sel.In == nil &&
		sel.Exists == ""
}

func relativeKey(namespace, namespaced string) string {
	if namespace == "" {
		return namespaced
	}
	prefix := namespace + "/"
	return strings.TrimPrefix(namespaced, prefix)
}

func cloneMeta(meta storage.Meta) storage.Meta {
	clone := meta
	if meta.Lease != nil {
		leaseCopy := *meta.Lease
		clone.Lease = &leaseCopy
	}
	if len(meta.StateDescriptor) > 0 {
		clone.StateDescriptor = append([]byte(nil), meta.StateDescriptor...)
	}
	return clone
}

func durationToSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Second - 1) / time.Second)
}
