package mcp

import (
	"fmt"
	"strings"
)

const (
	toolLockAcquire                  = "lockd.lock.acquire"
	toolLockKeepAlive                = "lockd.lock.keepalive"
	toolLockRelease                  = "lockd.lock.release"
	toolGet                          = "lockd.get"
	toolDescribe                     = "lockd.describe"
	toolQuery                        = "lockd.query"
	toolQueryStream                  = "lockd.query.stream"
	toolStateUpdate                  = "lockd.state.update"
	toolStatePatch                   = "lockd.state.patch"
	toolStateWriteStreamBegin        = "lockd.state.write_stream.begin"
	toolStateWriteStreamAppend       = "lockd.state.write_stream.append"
	toolStateWriteStreamCommit       = "lockd.state.write_stream.commit"
	toolStateWriteStreamAbort        = "lockd.state.write_stream.abort"
	toolStateStream                  = "lockd.state.stream"
	toolStateMetadata                = "lockd.state.metadata"
	toolStateRemove                  = "lockd.state.remove"
	toolAttachmentsWriteStreamBegin  = "lockd.attachments.write_stream.begin"
	toolAttachmentsWriteStreamAppend = "lockd.attachments.write_stream.append"
	toolAttachmentsWriteStreamCommit = "lockd.attachments.write_stream.commit"
	toolAttachmentsWriteStreamAbort  = "lockd.attachments.write_stream.abort"
	toolAttachmentsList              = "lockd.attachments.list"
	toolAttachmentsHead              = "lockd.attachments.head"
	toolAttachmentsChecksum          = "lockd.attachments.checksum"
	toolAttachmentsGet               = "lockd.attachments.get"
	toolAttachmentsStream            = "lockd.attachments.stream"
	toolAttachmentsDelete            = "lockd.attachments.delete"
	toolAttachmentsDeleteAll         = "lockd.attachments.delete_all"
	toolNamespaceGet                 = "lockd.namespace.get"
	toolNamespaceUpdate              = "lockd.namespace.update"
	toolIndexFlush                   = "lockd.index.flush"
	toolHint                         = "lockd.hint"
	toolHelp                         = "lockd.help"
	toolQueueEnqueue                 = "lockd.queue.enqueue"
	toolQueueWriteStreamBegin        = "lockd.queue.write_stream.begin"
	toolQueueWriteStreamAppend       = "lockd.queue.write_stream.append"
	toolQueueWriteStreamCommit       = "lockd.queue.write_stream.commit"
	toolQueueWriteStreamAbort        = "lockd.queue.write_stream.abort"
	toolQueueDequeue                 = "lockd.queue.dequeue"
	toolQueueStats                   = "lockd.queue.stats"
	toolQueueWatch                   = "lockd.queue.watch"
	toolQueueAck                     = "lockd.queue.ack"
	toolQueueNack                    = "lockd.queue.nack"
	toolQueueDefer                   = "lockd.queue.defer"
	toolQueueExtend                  = "lockd.queue.extend"
	toolQueueSubscribe               = "lockd.queue.subscribe"
	toolQueueUnsubscribe             = "lockd.queue.unsubscribe"
)

var mcpToolNames = []string{
	toolLockAcquire,
	toolLockKeepAlive,
	toolLockRelease,
	toolGet,
	toolDescribe,
	toolQuery,
	toolQueryStream,
	toolStateUpdate,
	toolStatePatch,
	toolStateWriteStreamBegin,
	toolStateWriteStreamAppend,
	toolStateWriteStreamCommit,
	toolStateWriteStreamAbort,
	toolStateStream,
	toolStateMetadata,
	toolStateRemove,
	toolAttachmentsWriteStreamBegin,
	toolAttachmentsWriteStreamAppend,
	toolAttachmentsWriteStreamCommit,
	toolAttachmentsWriteStreamAbort,
	toolAttachmentsList,
	toolAttachmentsHead,
	toolAttachmentsChecksum,
	toolAttachmentsGet,
	toolAttachmentsStream,
	toolAttachmentsDelete,
	toolAttachmentsDeleteAll,
	toolNamespaceGet,
	toolNamespaceUpdate,
	toolIndexFlush,
	toolHint,
	toolHelp,
	toolQueueEnqueue,
	toolQueueWriteStreamBegin,
	toolQueueWriteStreamAppend,
	toolQueueWriteStreamCommit,
	toolQueueWriteStreamAbort,
	toolQueueDequeue,
	toolQueueStats,
	toolQueueWatch,
	toolQueueAck,
	toolQueueNack,
	toolQueueDefer,
	toolQueueExtend,
	toolQueueSubscribe,
	toolQueueUnsubscribe,
}

type toolContract struct {
	Purpose  string
	UseWhen  string
	Requires string
	Effects  string
	Retry    string
	Next     string
}

func formatToolDescription(spec toolContract) string {
	return strings.Join([]string{
		"Purpose: " + spec.Purpose,
		"Use when: " + spec.UseWhen,
		"Requires: " + spec.Requires,
		"Effects: " + spec.Effects,
		"Retry: " + spec.Retry,
		"Next: " + spec.Next,
	}, "\n")
}

func buildToolDescriptions(cfg Config) map[string]string {
	namespace := strings.TrimSpace(cfg.DefaultNamespace)
	if namespace == "" {
		namespace = "mcp"
	}
	queue := strings.TrimSpace(cfg.AgentBusQueue)
	if queue == "" {
		queue = "lockd.agent.bus"
	}
	inlineMax := cfg.InlineMaxBytes
	if inlineMax <= 0 {
		inlineMax = 2 * 1024 * 1024
	}

	return map[string]string{
		toolLockAcquire: formatToolDescription(toolContract{
			Purpose:  "Acquire an exclusive lock lease and receive the lease identity/fencing material needed for protected writes.",
			UseWhen:  "You need to mutate shared state or attachments for a key, or you need exclusive coordination ownership.",
			Requires: fmt.Sprintf("`key` is required. `namespace` defaults to %q. `owner` defaults to OAuth client id. Optional `txn_id` enlists the lease in XA flow.", namespace),
			Effects:  "Creates or claims a lease and returns `lease_id`, `fencing_token`, expiry, and optional `txn_id` for follow-up tools.",
			Retry:    "Not inherently idempotent. Retry can obtain a different lease or block outcome; use deterministic owner/workflow guards.",
			Next:     "Call `lockd.state.update` or attachment tools, keep lease alive while working, then `lockd.lock.release`.",
		}),
		toolLockKeepAlive: formatToolDescription(toolContract{
			Purpose:  "Extend an active lock lease TTL.",
			UseWhen:  "Long-running work might outlive current lease expiry.",
			Requires: fmt.Sprintf("`key`, `lease_id`, and `ttl_seconds > 0` are required. `namespace` defaults to %q. Optional `txn_id` keeps XA context consistent.", namespace),
			Effects:  "Extends lease expiry and returns the updated expiration timestamp.",
			Retry:    "Safe to retry while lease is still valid. If lease already expired, reacquire with `lockd.lock.acquire`.",
			Next:     "Continue mutation workflow; periodically keepalive until ready to release.",
		}),
		toolLockRelease: formatToolDescription(toolContract{
			Purpose:  "Release a lease and finalize staged changes.",
			UseWhen:  "Work under a lease is complete or must be abandoned.",
			Requires: fmt.Sprintf("`key` and `lease_id` are required. `namespace` defaults to %q. Optional `txn_id` and `rollback=true` control XA/rollback behavior.", namespace),
			Effects:  "Releases lease ownership. Commits staged changes by default; rolls back when `rollback=true`.",
			Retry:    "Usually safe to retry after transport failures; duplicate release may return already-released/lease errors depending on timing.",
			Next:     "If more work is needed later, reacquire with `lockd.lock.acquire`.",
		}),
		toolGet: formatToolDescription(toolContract{
			Purpose:  "Read committed JSON state for one key.",
			UseWhen:  "You need current state for planning, validation, or rendering context.",
			Requires: fmt.Sprintf("`key` is required. `namespace` defaults to %q. `public` defaults to true. If `public=false`, `lease_id` is required. If `public=true`, `lease_id` must be omitted.", namespace),
			Effects:  "Returns metadata only (`found`, ETag, numeric version). Payload is never inlined; use `lockd.state.stream` when content is required.",
			Retry:    "Safe to retry; this is a read operation.",
			Next:     "Call `lockd.state.stream` for payload bytes, `lockd.query` for broader discovery, or acquire a lock before mutation.",
		}),
		toolDescribe: formatToolDescription(toolContract{
			Purpose:  "Read key metadata (lease owner, version, ETag, timestamps) without returning state payload.",
			UseWhen:  "You need lock/version metadata for diagnostics, conflict handling, or coordination checks.",
			Requires: fmt.Sprintf("`key` is required. `namespace` defaults to %q when omitted.", namespace),
			Effects:  "Returns metadata snapshot only; does not mutate state.",
			Retry:    "Safe to retry; this is a read operation.",
			Next:     "Use `lockd.get` to read payload or lock tools for mutation workflows.",
		}),
		toolQuery: formatToolDescription(toolContract{
			Purpose:  "Run LQL queries over namespace data in key mode.",
			UseWhen:  "You need to locate candidate keys before reading or locking.",
			Requires: fmt.Sprintf("`query` expression is required. `namespace` defaults to %q. Optional `limit`, `cursor`, `engine`, `refresh`, and `fields` refine execution. Query output is always keys.", namespace),
			Effects:  "Returns matching keys plus pagination/index metadata (`cursor`, `index_seq`).",
			Retry:    "Safe to retry. Cursor-based pagination should reuse the latest returned cursor.",
			Next:     "Call `lockd.query.stream` for document payload streaming, or `lockd.get` and `lockd.state.stream` for point reads.",
		}),
		toolQueryStream: formatToolDescription(toolContract{
			Purpose:  "Run an LQL query and stream matching documents over MCP progress notifications without buffering full result sets.",
			UseWhen:  "You need query result documents and must keep MCP memory usage bounded.",
			Requires: fmt.Sprintf("Active MCP session is required. `query` expression is required. `namespace` defaults to %q. Optional `limit`, `cursor`, `engine`, `refresh`, `fields`, `chunk_bytes`, `max_bytes`, and `max_documents` bound execution.", namespace),
			Effects:  "Emits `lockd.query.document.chunk` progress notifications per document chunk and returns stream summary (`documents_streamed`, `streamed_bytes`, `chunks`, `truncated`).",
			Retry:    "Safe to retry; each call restarts the query stream from the provided cursor/options.",
			Next:     "Use returned `cursor` for pagination and verify document processing before issuing the next page.",
		}),
		toolStateUpdate: formatToolDescription(toolContract{
			Purpose:  "Write JSON state under lease protection.",
			UseWhen:  "You need to create or update state for a locked key and payload fits inline request limits.",
			Requires: fmt.Sprintf("`key` and `lease_id` are required. `namespace` defaults to %q. Payload comes from exactly one of `payload_text` or `payload_base64` (defaults to `{}`) and must be <= %d bytes after decoding.", namespace, inlineMax),
			Effects:  "Updates state and returns new version/ETag metadata; can also mutate `query_hidden` metadata.",
			Retry:    "Use `if_etag`, `if_version`, and `fencing_token` for safe retries. Without guards, retries can apply duplicate writes.",
			Next:     "For larger payloads use `lockd.state.write_stream.begin` and upload to returned `upload_url`, then `commit`; release lease to commit or rollback.",
		}),
		toolStatePatch: formatToolDescription(toolContract{
			Purpose:  "Apply an RFC 7396 JSON merge patch to current state under lease protection.",
			UseWhen:  "You need partial updates without sending a full replacement document.",
			Requires: fmt.Sprintf("`key` and `lease_id` are required. `namespace` defaults to %q. Provide exactly one of `patch_text` or `patch_base64` and keep patch/result within inline limits; use `state.write_stream` for large documents.", namespace),
			Effects:  "Loads current state (lease-bound read), applies merge patch, then writes updated state and returns new version/ETag metadata.",
			Retry:    "Use `if_etag`, `if_version`, and `fencing_token` for safe retries. Patch retries without guards can duplicate logical effects.",
			Next:     "Use `lockd.get`/`lockd.state.stream` to verify result, then release lease.",
		}),
		toolStateWriteStreamBegin: formatToolDescription(toolContract{
			Purpose:  "Open a session-scoped streaming state writer.",
			UseWhen:  "State payload is too large for inline JSON or you want bounded-memory uploads.",
			Requires: fmt.Sprintf("Active MCP session is required. `key` and `lease_id` are required. `namespace` defaults to %q. Optional CAS/fencing fields mirror `lockd.state.update`.", namespace),
			Effects:  "Creates a stream session and returns `stream_id` plus one-time `upload_url` for direct HTTP PUT upload outside MCP tool payload context.",
			Retry:    "Not idempotent; begin returns a new stream each time. Abort unused streams explicitly.",
			Next:     "Upload bytes to `upload_url`, then call `lockd.state.write_stream.commit` (or `abort`).",
		}),
		toolStateWriteStreamAppend: formatToolDescription(toolContract{
			Purpose:  "Append one base64 chunk into an open state write stream.",
			UseWhen:  "You are actively streaming large state payload content.",
			Requires: fmt.Sprintf("Active MCP session is required. `stream_id` and `chunk_base64` are required. Decoded chunk must be <= %d bytes.", inlineMax),
			Effects:  "Writes chunk bytes directly to the upstream update request stream and returns append/total byte counts.",
			Retry:    "Do not blindly retry the same chunk without stream-position tracking; repeated append can duplicate bytes.",
			Next:     "Continue appending until complete, then `lockd.state.write_stream.commit`.",
		}),
		toolStateWriteStreamCommit: formatToolDescription(toolContract{
			Purpose:  "Finalize a state write stream and commit it to upstream lockd.",
			UseWhen:  "All payload chunks have been appended.",
			Requires: "Active MCP session and `stream_id` are required.",
			Effects:  "Closes the upload stream, waits for upstream completion, and returns state update result metadata.",
			Retry:    "Commit is terminal for a stream_id. On failure, begin a new stream and replay payload.",
			Next:     "Release lease or proceed with additional state/attachment mutations.",
		}),
		toolStateWriteStreamAbort: formatToolDescription(toolContract{
			Purpose:  "Abort an in-flight state write stream.",
			UseWhen:  "Upload should be canceled or discarded before commit.",
			Requires: "Active MCP session and `stream_id` are required. Optional `reason` is advisory.",
			Effects:  "Terminates the stream, cancels upstream request flow, and frees stream resources.",
			Retry:    "Safe to retry; absent streams return not-found style errors.",
			Next:     "Begin a new stream if upload should be retried.",
		}),
		toolStateStream: formatToolDescription(toolContract{
			Purpose:  "Create a one-time download capability URL for state payload bytes.",
			UseWhen:  "You need state content (small or large) without routing payload through MCP tool responses.",
			Requires: fmt.Sprintf("Active MCP session is required. `key` is required. `namespace` defaults to %q. `public` defaults to true; set `public=false` with `lease_id` for lease-bound reads.", namespace),
			Effects:  "Returns metadata plus one-time `download_url` and `download_method` for direct HTTP download.",
			Retry:    "Safe to retry; each call creates a new one-time URL.",
			Next:     "Fetch `download_url` directly with the returned method, then continue lock/query workflow.",
		}),
		toolStateMetadata: formatToolDescription(toolContract{
			Purpose:  "Update state metadata without replacing JSON state payload.",
			UseWhen:  "Only metadata (for example `query_hidden`) needs to change.",
			Requires: fmt.Sprintf("`key`, `lease_id`, and `query_hidden` are required. `namespace` defaults to %q. Optional CAS/fencing fields protect against races.", namespace),
			Effects:  "Persists metadata mutation and returns resulting version/metadata.",
			Retry:    "Use CAS/fencing fields for deterministic retries; otherwise last-writer-wins behavior applies.",
			Next:     "Continue updates under the same lease or release the lease when done.",
		}),
		toolStateRemove: formatToolDescription(toolContract{
			Purpose:  "Delete state for a key under lease protection.",
			UseWhen:  "State should be removed as part of cleanup or workflow transitions.",
			Requires: fmt.Sprintf("`key` and `lease_id` are required. `namespace` defaults to %q. Optional `txn_id` and CAS/fencing fields support XA/concurrency control.", namespace),
			Effects:  "Removes committed state for the key (or stages delete in transaction context).",
			Retry:    "Prefer CAS/fencing guards for deterministic retries; repeated deletes may be no-op or not-found depending on timing.",
			Next:     "Release lease to finalize, or write replacement state/attachments first.",
		}),
		toolAttachmentsWriteStreamBegin: formatToolDescription(toolContract{
			Purpose:  "Open a session-scoped streaming attachment writer.",
			UseWhen:  "You need to upload attachment payloads (all attachment writes are stream-based).",
			Requires: fmt.Sprintf("Active MCP session is required. `key`, `lease_id`, and `name` are required. `namespace` defaults to %q. `mode` supports `create` (default), `upsert`, `replace`.", namespace),
			Effects:  "Creates a stream session and returns one-time `upload_url` for direct HTTP PUT upload into upstream attachment flow.",
			Retry:    "Not idempotent; begin returns a new stream each time. Abort unused streams explicitly.",
			Next:     "Upload bytes to `upload_url`, then call `lockd.attachments.write_stream.commit` (or `abort`).",
		}),
		toolAttachmentsWriteStreamAppend: formatToolDescription(toolContract{
			Purpose:  "Append one base64 chunk into an open attachment write stream.",
			UseWhen:  "You are uploading attachment bytes chunk-by-chunk.",
			Requires: fmt.Sprintf("Active MCP session is required. `stream_id` and `chunk_base64` are required. Decoded chunk must be <= %d bytes.", inlineMax),
			Effects:  "Writes chunk bytes directly to upstream attachment body stream and returns append/total byte counts.",
			Retry:    "Do not blindly retry the same chunk without stream-position tracking; repeated append can duplicate bytes.",
			Next:     "Continue appending until complete, then `lockd.attachments.write_stream.commit`.",
		}),
		toolAttachmentsWriteStreamCommit: formatToolDescription(toolContract{
			Purpose:  "Finalize an attachment write stream.",
			UseWhen:  "All attachment payload chunks have been appended.",
			Requires: "Active MCP session and `stream_id` are required.",
			Effects:  "Closes the upload stream, waits for upstream attach completion, and returns attachment metadata/version.",
			Retry:    "Commit is terminal for a stream_id. On failure, begin a new stream and replay payload.",
			Next:     "List/head/checksum attachments for verification, then release lease.",
		}),
		toolAttachmentsWriteStreamAbort: formatToolDescription(toolContract{
			Purpose:  "Abort an in-flight attachment write stream.",
			UseWhen:  "Attachment upload should be canceled.",
			Requires: "Active MCP session and `stream_id` are required. Optional `reason` is advisory.",
			Effects:  "Terminates upload flow and frees stream resources.",
			Retry:    "Safe to retry; absent streams return not-found style errors.",
			Next:     "Begin a new attachment stream if upload should be retried.",
		}),
		toolAttachmentsList: formatToolDescription(toolContract{
			Purpose:  "List attachment metadata for a key.",
			UseWhen:  "You need attachment ids/names before get/delete workflows.",
			Requires: fmt.Sprintf("`key` is required. `namespace` defaults to %q. `public` defaults to true. If `public=false`, `lease_id` is required. If `public=true`, `lease_id` must be omitted.", namespace),
			Effects:  "Returns attachment metadata only (no payload bytes).",
			Retry:    "Safe to retry; this is a read operation.",
			Next:     "Use `lockd.attachments.head` for metadata-only lookup, `lockd.attachments.get` for payload access, or delete tools for cleanup.",
		}),
		toolAttachmentsHead: formatToolDescription(toolContract{
			Purpose:  "Fetch metadata for one attachment by id or name without reading payload bytes.",
			UseWhen:  "You need to inspect attachment existence/size/type before deciding whether to download content.",
			Requires: fmt.Sprintf("`key` plus either `id` or `name` is required. `namespace` defaults to %q. `public` defaults to true. If `public=false`, `lease_id` is required. If `public=true`, `lease_id` must be omitted.", namespace),
			Effects:  "Returns attachment metadata only (`id`, `name`, `size`, `plaintext_sha256`, timestamps, and content type).",
			Retry:    "Safe to retry; this is a read operation.",
			Next:     "Call `lockd.attachments.stream` when payload content is required.",
		}),
		toolAttachmentsChecksum: formatToolDescription(toolContract{
			Purpose:  "Return persisted plaintext SHA-256 checksum for one attachment without reading attachment payload.",
			UseWhen:  "You need integrity metadata quickly and cannot afford a full payload read.",
			Requires: fmt.Sprintf("`key` plus either `id` or `name` is required. `namespace` defaults to %q. `public` defaults to true. If `public=false`, `lease_id` is required.", namespace),
			Effects:  "Returns metadata-derived `plaintext_sha256` for the selected attachment.",
			Retry:    "Safe to retry; this is a read operation.",
			Next:     "Compare against client-side checksum after `lockd.attachments.stream` if payload verification is needed.",
		}),
		toolAttachmentsGet: formatToolDescription(toolContract{
			Purpose:  "Resolve one attachment by id or name and return metadata for streaming decisions.",
			UseWhen:  "You need attachment metadata and stream hints without loading payload into memory.",
			Requires: fmt.Sprintf("`key` plus either `id` or `name` is required. `namespace` defaults to %q. `public` defaults to true. If `public=false`, `lease_id` is required. If `public=true`, `lease_id` must be omitted.", namespace),
			Effects:  "Returns attachment metadata and persisted checksum only; payload bytes are not returned inline.",
			Retry:    "Safe to retry; this is a read operation.",
			Next:     "Call `lockd.attachments.stream` to get a one-time payload download URL.",
		}),
		toolAttachmentsStream: formatToolDescription(toolContract{
			Purpose:  "Create a one-time download capability URL for attachment payload bytes.",
			UseWhen:  "You need attachment content without pushing payload through MCP tool responses.",
			Requires: fmt.Sprintf("Active MCP session is required. `key` plus either `id` or `name` is required. `namespace` defaults to %q. `public` defaults to true.", namespace),
			Effects:  "Returns attachment metadata plus one-time `download_url` and `download_method` for direct HTTP download.",
			Retry:    "Safe to retry; each call creates a new one-time URL.",
			Next:     "Fetch `download_url` directly and compare with `lockd.attachments.checksum` when integrity checks are needed.",
		}),
		toolAttachmentsDelete: formatToolDescription(toolContract{
			Purpose:  "Delete a single attachment under lease protection.",
			UseWhen:  "One attachment should be removed while keeping others.",
			Requires: fmt.Sprintf("`key` and `lease_id` are required, plus either `id` or `name`. `namespace` defaults to %q. Optional `txn_id` enables XA staging.", namespace),
			Effects:  "Deletes (or stages deletion of) the targeted attachment and returns success state.",
			Retry:    "Generally safe after network errors; repeated deletes may become no-op/not-found depending on prior success.",
			Next:     "List attachments to verify remaining items, then release lease.",
		}),
		toolAttachmentsDeleteAll: formatToolDescription(toolContract{
			Purpose:  "Delete all attachments for a key under lease protection.",
			UseWhen:  "You need full attachment cleanup for a key.",
			Requires: fmt.Sprintf("`key` and `lease_id` are required. `namespace` defaults to %q. Optional `txn_id` allows transactional staging.", namespace),
			Effects:  "Removes (or stages removal of) all attachments for the key.",
			Retry:    "Usually safe to retry after transport failures; later attempts may report already-empty state.",
			Next:     "Optionally write replacement attachments, then release lease.",
		}),
		toolNamespaceGet: formatToolDescription(toolContract{
			Purpose:  "Read namespace query-engine configuration.",
			UseWhen:  "You need to inspect current query engine settings before updates or debugging.",
			Requires: fmt.Sprintf("`namespace` defaults to %q when omitted.", namespace),
			Effects:  "Returns effective namespace query configuration and ETag context.",
			Retry:    "Safe to retry; this is a read operation.",
			Next:     "Use `lockd.namespace.update` to change engine preferences.",
		}),
		toolNamespaceUpdate: formatToolDescription(toolContract{
			Purpose:  "Update namespace query-engine configuration.",
			UseWhen:  "You need to change preferred/fallback query engines for a namespace.",
			Requires: fmt.Sprintf("`preferred_engine` and `fallback_engine` are required. `namespace` defaults to %q. Optional `if_match` enables CAS-safe updates.", namespace),
			Effects:  "Persists namespace query config and returns updated values/ETag.",
			Retry:    "Prefer `if_match` for deterministic retries; otherwise concurrent updates may overwrite each other.",
			Next:     "Optionally call `lockd.index.flush` or rerun queries to validate behavior.",
		}),
		toolIndexFlush: formatToolDescription(toolContract{
			Purpose:  "Trigger search index flush for a namespace.",
			UseWhen:  "You need query index freshness guarantees after heavy writes or before time-sensitive reads.",
			Requires: fmt.Sprintf("`namespace` defaults to %q. `mode` may be `async` (default) or `wait`.", namespace),
			Effects:  "Schedules or waits for index flush and returns flush progress metadata.",
			Retry:    "Safe to retry; repeated flush requests are operationally benign.",
			Next:     "Run `lockd.query` to observe refreshed index results.",
		}),
		toolHint: formatToolDescription(toolContract{
			Purpose:  "Return namespace access hints so agents can choose valid namespaces before doing work.",
			UseWhen:  "Session start, planning phase, or when namespace-forbidden errors occur.",
			Requires: "No required fields. Uses current caller token context plus upstream client-bundle namespace claims when available.",
			Effects:  "Returns advisory namespace access hints, default namespace, client id, and token scope context.",
			Retry:    "Safe to retry; this is a read-only advisory operation.",
			Next:     "Call `lockd.help` then use returned namespace hints when invoking lock/query/queue tools.",
		}),
		toolHelp: formatToolDescription(toolContract{
			Purpose:  "Return curated lockd MCP workflows, invariants, and documentation resource URIs.",
			UseWhen:  "Start of session or when uncertain about correct operation sequence.",
			Requires: "No required fields. Optional `topic` narrows guidance to overview/locks/messaging/sync.",
			Effects:  "Returns guidance only; no lockd state mutation occurs.",
			Retry:    "Safe to retry.",
			Next:     "Follow `next_calls` in the response, typically beginning with lock, queue, or query tools.",
		}),
		toolQueueEnqueue: formatToolDescription(toolContract{
			Purpose:  "Publish a message to a lockd queue for agent coordination.",
			UseWhen:  "You need to signal work/events/context to queue consumers and payload fits inline limits.",
			Requires: fmt.Sprintf("`queue` defaults to %q. `namespace` defaults to %q. Provide payload via exactly one of `payload_text` or `payload_base64`; decoded payload must be <= %d bytes.", queue, namespace, inlineMax),
			Effects:  "Appends a queue message and returns delivery metadata (`message_id`, visibility, attempts). Queue ordering is best-effort under retries/requeues; do not assume strict FIFO across all consumers.",
			Retry:    "Not idempotent by default; retries can enqueue duplicates. Use application-level dedupe keys in payload/attributes.",
			Next:     "For larger payloads use `lockd.queue.write_stream.begin` and upload to returned `upload_url`, then `commit`. Consumers should dequeue then ack/nack/defer.",
		}),
		toolQueueWriteStreamBegin: formatToolDescription(toolContract{
			Purpose:  "Open a session-scoped streaming queue publisher.",
			UseWhen:  "Queue payload is too large for inline enqueue or you need bounded-memory uploads.",
			Requires: fmt.Sprintf("Active MCP session is required. `queue` defaults to %q and `namespace` defaults to %q. Delay/visibility/ttl/attributes/content_type options match `lockd.queue.enqueue`.", queue, namespace),
			Effects:  "Creates a stream session and returns `stream_id` plus one-time `upload_url` for direct HTTP PUT upload.",
			Retry:    "Not idempotent; begin returns a new stream each time. Abort unused streams explicitly.",
			Next:     "Upload bytes to `upload_url`, then call `lockd.queue.write_stream.commit` (or `abort`).",
		}),
		toolQueueWriteStreamAppend: formatToolDescription(toolContract{
			Purpose:  "Append one base64 chunk into an open queue write stream.",
			UseWhen:  "You are streaming large queue payload content.",
			Requires: fmt.Sprintf("Active MCP session is required. `stream_id` and `chunk_base64` are required. Decoded chunk must be <= %d bytes.", inlineMax),
			Effects:  "Writes chunk bytes directly to the upstream enqueue request stream and returns append/total byte counts.",
			Retry:    "Do not blindly retry the same chunk without stream-position tracking; repeated append can duplicate bytes.",
			Next:     "Continue appending until complete, then `lockd.queue.write_stream.commit`.",
		}),
		toolQueueWriteStreamCommit: formatToolDescription(toolContract{
			Purpose:  "Finalize a queue write stream and publish the message.",
			UseWhen:  "All payload chunks for a queued message have been appended.",
			Requires: "Active MCP session and `stream_id` are required.",
			Effects:  "Closes upload stream, waits for upstream enqueue completion, and returns enqueue metadata.",
			Retry:    "Commit is terminal for a stream_id. On failure, begin a new stream and replay payload.",
			Next:     "Use dequeue/ack workflows on consuming agents.",
		}),
		toolQueueWriteStreamAbort: formatToolDescription(toolContract{
			Purpose:  "Abort an in-flight queue write stream.",
			UseWhen:  "Queued payload upload should be canceled.",
			Requires: "Active MCP session and `stream_id` are required. Optional `reason` is advisory.",
			Effects:  "Terminates upload flow and frees stream resources.",
			Retry:    "Safe to retry; absent streams return not-found style errors.",
			Next:     "Begin a new queue stream if publish should be retried.",
		}),
		toolQueueDequeue: formatToolDescription(toolContract{
			Purpose:  "Receive one available message lease from a queue.",
			UseWhen:  "A worker is ready to process the next queue item.",
			Requires: fmt.Sprintf("Active MCP session is required for payload transfer. `queue` defaults to %q. `namespace` defaults to %q. `owner` defaults to OAuth client id. Optional `stateful`, `visibility_seconds`, `cursor`, and `txn_id` tune dequeue behavior.", queue, namespace),
			Effects:  "Returns `found=false` when no message is available, or message metadata/lease fields plus one-time `payload_download_url` for direct payload download. Includes `next_cursor` for continuation.",
			Retry:    "Safe to retry. Duplicate retries may lease different messages when queue state changes.",
			Next:     "Download payload with returned URL/method, then call `lockd.queue.ack`, `lockd.queue.nack`, or `lockd.queue.defer`; use `lockd.queue.extend` for long handlers.",
		}),
		toolQueueStats: formatToolDescription(toolContract{
			Purpose:  "Read side-effect-free queue runtime stats and current visible head snapshot.",
			UseWhen:  "You need queue health/introspection signals before deciding whether to watch/dequeue.",
			Requires: fmt.Sprintf("`queue` defaults to %q and `namespace` defaults to %q.", queue, namespace),
			Effects:  "Returns dispatcher counters (`waiting_consumers`, `pending_candidates`, `total_consumers`), watcher state (`has_active_watcher`), and head availability fields when a visible message exists.",
			Retry:    "Safe to retry; read-only operation.",
			Next:     "Use `lockd.queue.watch` for bounded wakeups and `lockd.queue.dequeue` for actual message leasing/consumption.",
		}),
		toolQueueWatch: formatToolDescription(toolContract{
			Purpose:  "Wait for queue-availability events within a bounded call window.",
			UseWhen:  "Interactive clients need push-like wakeups without maintaining long-lived subscriptions.",
			Requires: fmt.Sprintf("`queue` defaults to %q and `namespace` defaults to %q. Optional `duration_seconds` bounds wait time (default 30). Optional `max_events` bounds result size (default 1).", queue, namespace),
			Effects:  "Streams watch events from upstream lockd and returns a bounded event list with `stop_reason` (`max_events`, `timeout`, or `context_canceled`).",
			Retry:    "Safe to retry; each invocation is independent and bounded.",
			Next:     "After any event, call `lockd.queue.dequeue` and then `ack`/`nack`/`defer`.",
		}),
		toolQueueAck: formatToolDescription(toolContract{
			Purpose:  "Acknowledge successful processing of a dequeued message.",
			UseWhen:  "Worker completed handling of a leased message.",
			Requires: fmt.Sprintf("`queue`, `message_id`, `lease_id`, `fencing_token`, and `meta_etag` are required. `namespace` defaults to %q. Use fields returned by `lockd.queue.dequeue`.", namespace),
			Effects:  "Confirms processing and removes/commits message state according to lockd queue semantics.",
			Retry:    "Generally safe after network failures; duplicate ack may return already-acknowledged/mismatch errors based on lease state.",
			Next:     "Dequeue next message or perform follow-up state updates as needed.",
		}),
		toolQueueNack: formatToolDescription(toolContract{
			Purpose:  "Requeue a message as a failure, consuming failure budget (`max_attempts`).",
			UseWhen:  "Message processing failed and should be retried later with failure semantics.",
			Requires: fmt.Sprintf("`queue`, `message_id`, `lease_id`, `fencing_token`, and `meta_etag` are required. `namespace` defaults to %q. Optional `delay_seconds` and `reason` enrich retry behavior.", namespace),
			Effects:  "Requeues message with `intent=failure`, updates queue metadata ETag, and records optional failure detail.",
			Retry:    "Generally safe after transport failures; repeated nack can alter delay/attempt metadata depending on timing.",
			Next:     "Dequeue next message or inspect retry/dead-letter behavior.",
		}),
		toolQueueDefer: formatToolDescription(toolContract{
			Purpose:  "Requeue a leased message intentionally without counting it as failure.",
			UseWhen:  "Message is valid but should be processed later or by another worker.",
			Requires: fmt.Sprintf("`queue`, `message_id`, `lease_id`, `fencing_token`, and `meta_etag` are required. `namespace` defaults to %q. Optional `delay_seconds` postpones visibility.", namespace),
			Effects:  "Returns message to queue with defer intent and optional delay.",
			Retry:    "Generally safe after transport failures; repeated defer can change next-visibility timing.",
			Next:     "Continue polling/subscription workflow and dequeue again when ready.",
		}),
		toolQueueExtend: formatToolDescription(toolContract{
			Purpose:  "Extend visibility/lease timeout for a currently leased queue message.",
			UseWhen:  "Message processing is still active and lease timeout is approaching.",
			Requires: fmt.Sprintf("`queue`, `message_id`, `lease_id`, `fencing_token`, and `meta_etag` are required. `namespace` defaults to %q. Optional state lease fields support stateful dequeue workflows.", namespace),
			Effects:  "Refreshes message lease expiry and visibility timeout; returns updated lease timing and metadata ETag.",
			Retry:    "Safe to retry while lease remains valid. If lease has expired, dequeue again to reacquire.",
			Next:     "Continue processing, then ack/nack/defer when work is complete.",
		}),
		toolQueueSubscribe: formatToolDescription(toolContract{
			Purpose:  "Subscribe current MCP session to queue-availability notifications over SSE.",
			UseWhen:  "You run a long-lived MCP runtime and want ongoing push wake-ups instead of pure dequeue polling.",
			Requires: fmt.Sprintf("Active MCP session is required. `queue` defaults to %q and `namespace` defaults to %q.", queue, namespace),
			Effects:  "Registers session subscription; future queue activity emits MCP progress notifications.",
			Retry:    "Safe to retry; duplicate subscribe is handled as already subscribed.",
			Next:     "On notification, call `lockd.queue.dequeue` then `ack` or `defer`.",
		}),
		toolQueueUnsubscribe: formatToolDescription(toolContract{
			Purpose:  "Remove current MCP session queue subscription.",
			UseWhen:  "Worker no longer wants notifications for a queue.",
			Requires: fmt.Sprintf("Active MCP session is required. `queue` defaults to %q and `namespace` defaults to %q.", queue, namespace),
			Effects:  "Stops notification forwarding for the targeted session+queue binding.",
			Retry:    "Safe to retry; unsubscribing an absent subscription returns false/unsubscribed state.",
			Next:     "Either resubscribe later or continue with explicit dequeue polling.",
		}),
	}
}
