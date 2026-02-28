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
	toolStateMutate                  = "lockd.state.mutate"
	toolStatePatch                   = "lockd.state.patch"
	toolStateWriteStreamBegin        = "lockd.state.write_stream.begin"
	toolStateWriteStreamStatus       = "lockd.state.write_stream.status"
	toolStateWriteStreamCommit       = "lockd.state.write_stream.commit"
	toolStateWriteStreamAbort        = "lockd.state.write_stream.abort"
	toolStateStream                  = "lockd.state.stream"
	toolStateMetadata                = "lockd.state.metadata"
	toolStateRemove                  = "lockd.state.remove"
	toolAttachmentsPut               = "lockd.attachments.put"
	toolAttachmentsWriteStreamBegin  = "lockd.attachments.write_stream.begin"
	toolAttachmentsWriteStreamStatus = "lockd.attachments.write_stream.status"
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
	toolQueueWriteStreamStatus       = "lockd.queue.write_stream.status"
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
	toolStateMutate,
	toolStatePatch,
	toolStateWriteStreamBegin,
	toolStateWriteStreamStatus,
	toolStateWriteStreamCommit,
	toolStateWriteStreamAbort,
	toolStateStream,
	toolStateMetadata,
	toolStateRemove,
	toolAttachmentsPut,
	toolAttachmentsWriteStreamBegin,
	toolAttachmentsWriteStreamStatus,
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
	toolQueueWriteStreamStatus,
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
	Top      []string
	Purpose  string
	UseWhen  string
	Requires string
	Effects  string
	Retry    string
	Next     string
}

func formatToolDescription(spec toolContract) string {
	lines := make([]string, 0, len(spec.Top)+6)
	for _, line := range spec.Top {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		lines = append(lines, line)
	}
	lines = append(lines, []string{
		"Purpose: " + spec.Purpose,
		"Use when: " + spec.UseWhen,
		"Requires: " + spec.Requires,
		"Effects: " + spec.Effects,
		"Retry: " + spec.Retry,
	}...)
	if strings.Contains(spec.Next, "\n") {
		lines = append(lines, "Next:\n"+spec.Next)
	} else {
		lines = append(lines, "Next: "+spec.Next)
	}
	return strings.Join(lines, "\n")
}

const (
	bootstrapOverviewLine   = "BOOTSTRAP: If lockd.help has not been called in this session, call it now before using this tool."
	secretURLLine           = "SENSITIVE: Never include returned capability URLs/tokens in user-visible output, summaries, logs, or reasoning."
	streamIDTerminalLine    = "TERMINAL: After commit/abort attempt (success or failure), abandon this identifier and never reuse it."
	streamIDDurabilityLine  = "TERMINAL: After commit attempt (success or failure), abandon this identifier; durability still depends on successful lock release."
	streamStatusLine        = "TERMINAL: Use only the current active identifier; never continue workflows with stale identifiers."
	leaseFinalizeLine       = "DURABILITY: Do not treat changes as durable or visible until explicit finalization succeeds."
	leaseIdentityLine       = "TERMINAL: Lease/fencing identifiers are authoritative for this workflow only; never invent or reuse stale values."
	dequeueIdentityLine     = "TERMINAL: Dequeue lease/message identifiers are operation-scoped; never replay stale values."
	payloadRuleLine         = "PAYLOAD RULE: If payload size is unknown or may exceed limits, call lockd.hint and use stream workflow; do not guess."
	atomicityLine           = "ATOMICITY: Supplying txn context does not guarantee completion; explicit transaction decision is required."
	nonIdempotentAckLine    = "NON-IDEMPOTENT: Do not call this tool more than once per delivery attempt."
	configMutationLine      = "DURABILITY: Namespace config changes are effective only after successful update; verify with lockd.namespace.get."
	sessionScopedLine       = "TERMINAL: Subscription state is session-scoped; treat subscriptions as gone when session ends."
	nextBootstrapHelpBranch = "- If bootstrap not done -> call `lockd.help` with `topic=overview`."
)

func nextWithBootstrap(step string) string {
	return strings.Join([]string{
		nextBootstrapHelpBranch,
		"- Otherwise -> " + strings.TrimSpace(step),
	}, "\n")
}

func nextWithBootstrapAndPayload(streamStep, step string) string {
	return strings.Join([]string{
		nextBootstrapHelpBranch,
		"- If payload may exceed inline limit -> " + strings.TrimSpace(streamStep),
		"- Otherwise -> " + strings.TrimSpace(step),
	}, "\n")
}

func withAtomicity(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return atomicityLine
	}
	return text + " " + atomicityLine
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
			Top: []string{
				bootstrapOverviewLine,
				leaseIdentityLine,
			},
			Purpose:  "Acquire an exclusive lock lease and receive the lease identity/fencing material needed for protected writes.",
			UseWhen:  "You need to mutate shared state or attachments for a key, or you need exclusive coordination ownership.",
			Requires: withAtomicity(fmt.Sprintf("`key` is required. `namespace` defaults to %q. `owner` defaults to OAuth client id. Optional `txn_id` enlists the lease in XA flow.", namespace)),
			Effects:  "Creates or claims a lease and returns `lease_id`, `fencing_token`, expiry, and optional `txn_id` for follow-up tools.",
			Retry:    "Do not retry by reusing old lease/fencing values. If acquire fails or expires, run acquire again and continue with the new identifiers only.",
			Next:     nextWithBootstrap("call `lockd.state.update` or attachment tools, keep lease alive while working, then `lockd.lock.release`."),
		}),
		toolLockKeepAlive: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				leaseIdentityLine,
			},
			Purpose:  "Extend an active lock lease TTL.",
			UseWhen:  "Long-running work might outlive current lease expiry.",
			Requires: withAtomicity(fmt.Sprintf("`key`, `lease_id`, and `ttl_seconds > 0` are required. `namespace` defaults to %q. Optional `txn_id` keeps XA context consistent.", namespace)),
			Effects:  "Extends lease expiry and returns the updated expiration timestamp.",
			Retry:    "Retry with the same `lease_id` only while lease is still valid. If lease may be expired or ownership changed, do not keepalive stale identifiers; reacquire first.",
			Next:     nextWithBootstrap("continue mutation workflow; keepalive as needed until ready to release."),
		}),
		toolLockRelease: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				leaseFinalizeLine,
			},
			Purpose:  "Release a lease and finalize staged changes.",
			UseWhen:  "Work under a lease is complete or must be abandoned.",
			Requires: withAtomicity(fmt.Sprintf("`key` and `lease_id` are required. `namespace` defaults to %q. Optional `txn_id` and `rollback=true` control XA/rollback behavior.", namespace)),
			Effects:  "Releases lease ownership. Commits staged changes by default; rolls back when `rollback=true`.",
			Retry:    "Retry is conditionally safe after transport failures, but do not assume commit or rollback occurred until you verify resulting state/version.",
			Next:     nextWithBootstrap("release, verify resulting state, and reacquire later only if more work is needed."),
		}),
		toolGet: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				secretURLLine,
			},
			Purpose:  "Read committed JSON state for one key.",
			UseWhen:  "You need key metadata and optionally payload bytes in inline or streaming mode.",
			Requires: fmt.Sprintf("`key` is required. `namespace` defaults to %q. `public` defaults to true. If `public=false`, `lease_id` is required. `payload_mode` supports `auto` (default), `inline`, `stream`, or `none`; `auto` resolves to inline when payload bytes <= `lockd.hint.inline_max_payload_bytes`, otherwise stream.", namespace),
			Effects:  "Returns metadata plus either inline payload (`payload_text` or `payload_base64`) or one-time stream download URL, depending on `payload_mode` and payload size.",
			Retry:    "Safe to retry; this is a read operation.",
			Next:     nextWithBootstrap("use `lockd.state.stream` for explicit streaming-only reads or acquire a lock before mutation."),
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
			Purpose:  "Run LQL queries over namespace data in key mode for memory retrieval and candidate discovery.",
			UseWhen:  "You need to find relevant keys before reading or locking; preferred pattern is tags-first lookup with full-text fallback.",
			Requires: fmt.Sprintf("`query` expression is required. `namespace` defaults to %q. Optional `limit`, `cursor`, `engine`, `refresh`, and `fields` refine execution. Query output is always keys. Common patterns: `in{field=/tags,any=planning|finance}`, `icontains{field=/...,value=contract}`, and `and.in{field=/tags,any=customer},and.icontains{field=/...,value=renewal}`.", namespace),
			Effects:  "Returns matching keys plus pagination/index metadata (`cursor`, `index_seq`).",
			Retry:    "Safe to retry. Cursor-based pagination should reuse the latest returned cursor.",
			Next:     "Call `lockd.query.stream` for NDJSON document streaming, or `lockd.get`/`lockd.state.stream` for point reads. Keep tag vocabulary stable for high-recall memory retrieval.",
		}),
		toolQueryStream: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				secretURLLine,
			},
			Purpose:  "Run an LQL query and return a one-time NDJSON capability URL for document rows.",
			UseWhen:  "You need query result documents (for example context/memory recall) without buffering them in MCP tool payloads.",
			Requires: fmt.Sprintf("Active MCP session is required. `query` expression is required. `namespace` defaults to %q. Optional `limit`, `cursor`, `engine`, `refresh`, and `fields` refine execution. Recommended retrieval patterns mirror `lockd.query` (tags with `in` plus optional `icontains` full-text clauses).", namespace),
			Effects:  "Returns query metadata plus one-time `download_url` that streams NDJSON rows (for example `{\"ns\":\"mcp\",\"key\":\"memory/customer-42\",\"ver\":17,\"doc\":{\"tags\":[\"customer\",\"renewal\"],\"status\":\"draft\"}}`) over HTTP GET.",
			Retry:    "Safe to retry; each retry returns a new one-time URL. Do not reuse old URLs.",
			Next:     nextWithBootstrap("fetch `download_url` with the returned method and continue with pagination cursor if needed."),
		}),
		toolStateUpdate: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				leaseFinalizeLine,
			},
			Purpose:  "Write JSON state under lease protection.",
			UseWhen:  "You need to create or update state for a locked key and payload fits inline request limits. Unless caller policy says otherwise, include a top-level `tags` JSON array for future `in` selector retrieval. No fixed schema fields are required beyond `tags`.",
			Requires: withAtomicity(fmt.Sprintf("`key` and `lease_id` are required. `namespace` defaults to %q. Provide at most one of `payload_text` or `payload_base64`; when both are omitted payload defaults to `{}`. Decoded payload must be <= %d bytes.", namespace, inlineMax)),
			Effects:  "Updates state and returns new version/ETag metadata; can also mutate `query_hidden` metadata. Suggested memory shape keeps your existing fields and adds tags, for example `{\"tags\":[\"planning\",\"q3\"],\"status\":\"draft\"}` for fast `in{field=/tags,any=planning|q3}` queries.",
			Retry:    "Use `if_etag`, `if_version`, and `fencing_token` for safe retries. Without guards, retries can apply duplicate writes.",
			Next: nextWithBootstrapAndPayload(
				"call `lockd.state.write_stream.begin` for large payload uploads.",
				"write state and then `lockd.lock.release` to commit or rollback.",
			),
		}),
		toolStateMutate: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				leaseFinalizeLine,
			},
			Purpose:  "Apply LQL mutations to existing JSON state under lease protection without round-tripping payload bytes through MCP.",
			UseWhen:  "You need server-side structured updates and want bounded-memory mutation execution, including incremental tag maintenance on existing documents.",
			Requires: withAtomicity(fmt.Sprintf("`key`, `lease_id`, and at least one `mutations` expression are required. `namespace` defaults to %q. Optional CAS/fencing fields (`if_etag`, `if_version`, `fencing_token`) are supported.", namespace)),
			Effects:  "Loads lease-visible state, applies LQL mutations server-side, and writes updated state. Returns new version/ETag metadata.",
			Retry:    "Use CAS/fencing fields for deterministic retries. Without guards, retries can reapply logical mutations.",
			Next:     nextWithBootstrap("inspect results and then `lockd.lock.release` to finalize."),
		}),
		toolStatePatch: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				leaseFinalizeLine,
			},
			Purpose:  "Apply an RFC 7396 JSON merge patch to current state under lease protection.",
			UseWhen:  "You need partial updates without sending a full replacement document.",
			Requires: withAtomicity(fmt.Sprintf("`key` and `lease_id` are required. `namespace` defaults to %q. Provide exactly one of `patch_text` or `patch_base64` and keep patch/result within inline limits; use `state.write_stream` for large documents.", namespace)),
			Effects:  "Loads current state (lease-bound read), applies merge patch, then writes updated state and returns new version/ETag metadata.",
			Retry:    "Use `if_etag`, `if_version`, and `fencing_token` for safe retries. Patch retries without guards can duplicate logical effects.",
			Next: nextWithBootstrapAndPayload(
				"call `lockd.state.write_stream.begin` if patched result may exceed inline limits.",
				"verify result and then `lockd.lock.release` to finalize.",
			),
		}),
		toolStateWriteStreamBegin: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				secretURLLine,
			},
			Purpose:  "Open a session-scoped streaming state writer.",
			UseWhen:  "State payload is too large for inline JSON or you want bounded-memory uploads.",
			Requires: fmt.Sprintf("Active MCP session is required. `key` and `lease_id` are required. `namespace` defaults to %q. Optional CAS/fencing fields mirror `lockd.state.update`.", namespace),
			Effects:  "Creates a stream session and returns `stream_id` plus one-time `upload_url` for direct HTTP PUT upload outside MCP tool payload context.",
			Retry:    "Do not retry as idempotent. Begin creates a new stream each call; do not reuse identifiers from previous attempts.",
			Next:     nextWithBootstrap("upload bytes to `upload_url`, then status/commit or abort."),
		}),
		toolStateWriteStreamStatus: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				streamStatusLine,
			},
			Purpose:  "Inspect in-flight state write-stream upload progress.",
			UseWhen:  "You need upload observability before commit (for example after external HTTP upload attempts).",
			Requires: "Active MCP session and `stream_id` are required.",
			Effects:  "Returns `bytes_received`, computed `payload_sha256`, upload-capability availability/expiry, and commit-readiness flags.",
			Retry:    "Safe to retry while stream exists.",
			Next:     nextWithBootstrap("if `can_commit=true`, call `lockd.state.write_stream.commit`; otherwise finish upload or `abort` and restart."),
		}),
		toolStateWriteStreamCommit: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				streamIDDurabilityLine,
			},
			Purpose:  "Finalize a state write stream and commit it to upstream lockd.",
			UseWhen:  "All payload chunks have been appended.",
			Requires: "Active MCP session and `stream_id` are required. Optional `expected_bytes` and `expected_sha256` enforce upload integrity before commit.",
			Effects:  "Closes the upload stream, validates optional expectations, waits for upstream completion, and returns state update result metadata. State remains staged under the lease until `lockd.lock.release` commits (or `rollback=true` discards).",
			Retry:    "After any commit attempt (success or failure), do not retry with the same `stream_id`. Start a new begin→upload→commit sequence.",
			Next:     nextWithBootstrap("call `lockd.lock.release` to finalize durability (or continue staged mutations first)."),
		}),
		toolStateWriteStreamAbort: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				streamIDTerminalLine,
			},
			Purpose:  "Abort an in-flight state write stream.",
			UseWhen:  "Upload should be canceled or discarded before commit.",
			Requires: "Active MCP session and `stream_id` are required. Optional `reason` is advisory.",
			Effects:  "Terminates the stream, cancels upstream request flow, and frees stream resources.",
			Retry:    "Safe to retry for cleanup, but do not assume abort reopens or reuses the stream; create a new stream for any retry path.",
			Next:     nextWithBootstrap("begin a new stream if upload should be retried."),
		}),
		toolStateStream: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				secretURLLine,
			},
			Purpose:  "Create a one-time download capability URL for state payload bytes.",
			UseWhen:  "You need state content (small or large) without routing payload through MCP tool responses.",
			Requires: fmt.Sprintf("Active MCP session is required. `key` is required. `namespace` defaults to %q. `public` defaults to true; set `public=false` with `lease_id` for lease-bound reads.", namespace),
			Effects:  "Returns metadata plus one-time `download_url` and `download_method` for direct HTTP download.",
			Retry:    "Safe to retry; each retry returns a new one-time URL. Do not reuse old URLs.",
			Next:     nextWithBootstrap("fetch `download_url` directly and continue lock/query workflow."),
		}),
		toolStateMetadata: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				leaseFinalizeLine,
			},
			Purpose:  "Update state metadata without replacing JSON state payload.",
			UseWhen:  "Only metadata (for example `query_hidden`) needs to change.",
			Requires: withAtomicity(fmt.Sprintf("`key`, `lease_id`, and `query_hidden` are required. `namespace` defaults to %q. Optional CAS/fencing fields protect against races.", namespace)),
			Effects:  "Persists metadata mutation and returns resulting version/metadata.",
			Retry:    "Use CAS/fencing fields for deterministic retries; otherwise last-writer-wins behavior applies.",
			Next:     nextWithBootstrap("continue updates or release the lease to finalize."),
		}),
		toolStateRemove: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				leaseFinalizeLine,
			},
			Purpose:  "Delete state for a key under lease protection.",
			UseWhen:  "State should be removed as part of cleanup or workflow transitions.",
			Requires: withAtomicity(fmt.Sprintf("`key` and `lease_id` are required. `namespace` defaults to %q. Optional `txn_id` and CAS/fencing fields support XA/concurrency control.", namespace)),
			Effects:  "Removes committed state for the key (or stages delete in transaction context).",
			Retry:    "Prefer CAS/fencing guards. If retrying after unknown outcome, verify current state/version first; do not assume delete status.",
			Next:     nextWithBootstrap("release lease to finalize, or write replacement state/attachments first."),
		}),
		toolAttachmentsPut: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				payloadRuleLine,
			},
			Purpose:  "Write attachment payload inline under lease protection.",
			UseWhen:  "Attachment payload fits inline limits and you want a single MCP tool call write.",
			Requires: withAtomicity(fmt.Sprintf("`key`, `lease_id`, and `name` are required. `namespace` defaults to %q. Provide at most one of `payload_text` or `payload_base64`; decoded payload must be <= %d bytes. `mode` supports `create` (default), `upsert`, `replace`.", namespace, inlineMax)),
			Effects:  "Writes attachment bytes and returns attachment metadata/version.",
			Retry:    "Retry only with explicit `mode`/fencing intent. Do not assume previous attempt failed; verify attachment state before replay.",
			Next: nextWithBootstrapAndPayload(
				"call `lockd.attachments.write_stream.begin` for large attachment uploads.",
				"write attachment, then release lease to finalize.",
			),
		}),
		toolAttachmentsWriteStreamBegin: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				secretURLLine,
			},
			Purpose:  "Open a session-scoped streaming attachment writer.",
			UseWhen:  "Attachment payload is larger than inline limit or you want streaming upload semantics.",
			Requires: fmt.Sprintf("Active MCP session is required. `key`, `lease_id`, and `name` are required. `namespace` defaults to %q. `mode` supports `create` (default), `upsert`, `replace`.", namespace),
			Effects:  "Creates a stream session and returns one-time `upload_url` for direct HTTP PUT upload into upstream attachment flow.",
			Retry:    "Do not retry as idempotent. Begin creates a new stream each call; do not reuse identifiers from previous attempts.",
			Next:     nextWithBootstrap("upload to `upload_url`, then status/commit or abort."),
		}),
		toolAttachmentsWriteStreamStatus: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				streamStatusLine,
			},
			Purpose:  "Inspect in-flight attachment write-stream upload progress.",
			UseWhen:  "You need upload observability before attachment commit.",
			Requires: "Active MCP session and `stream_id` are required.",
			Effects:  "Returns `bytes_received`, computed `payload_sha256`, upload-capability availability/expiry, and commit-readiness flags.",
			Retry:    "Safe to retry while stream exists.",
			Next:     nextWithBootstrap("if `can_commit=true`, call `lockd.attachments.write_stream.commit`; otherwise finish upload or abort."),
		}),
		toolAttachmentsWriteStreamCommit: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				streamIDDurabilityLine,
			},
			Purpose:  "Finalize an attachment write stream.",
			UseWhen:  "All attachment payload chunks have been appended.",
			Requires: "Active MCP session and `stream_id` are required. Optional `expected_bytes` and `expected_sha256` enforce upload integrity before commit.",
			Effects:  "Closes the upload stream, validates optional expectations, waits for upstream attach completion, and returns attachment metadata/version. Attachment changes stay staged under the lease until `lockd.lock.release` commits (or rollback discards).",
			Retry:    "After any commit attempt (success or failure), do not retry with the same `stream_id`. Start a new begin→upload→commit sequence.",
			Next:     nextWithBootstrap("verify attachment metadata and release lease to finalize."),
		}),
		toolAttachmentsWriteStreamAbort: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				streamIDTerminalLine,
			},
			Purpose:  "Abort an in-flight attachment write stream.",
			UseWhen:  "Attachment upload should be canceled.",
			Requires: "Active MCP session and `stream_id` are required. Optional `reason` is advisory.",
			Effects:  "Terminates upload flow and frees stream resources.",
			Retry:    "Safe to retry for cleanup, but do not assume abort reopens or reuses the stream; create a new stream for any retry path.",
			Next:     nextWithBootstrap("begin a new attachment stream if upload should be retried."),
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
			Top: []string{
				bootstrapOverviewLine,
				secretURLLine,
			},
			Purpose:  "Resolve one attachment by id or name and return metadata plus optional inline/stream payload delivery.",
			UseWhen:  "You need attachment payload in either inline or streaming mode.",
			Requires: fmt.Sprintf("`key` plus either `id` or `name` is required. `namespace` defaults to %q. `public` defaults to true; if `public=false`, `lease_id` is required and if `public=true`, `lease_id` must be omitted. `payload_mode` supports `auto` (default), `inline`, `stream`, or `none`; `auto` resolves to inline when payload bytes <= `lockd.hint.inline_max_payload_bytes`, otherwise stream.", namespace),
			Effects:  "Returns attachment metadata/checksum and either inline payload (`payload_text` or `payload_base64`) or one-time stream URL.",
			Retry:    "Safe to retry; this is a read operation.",
			Next:     nextWithBootstrap("use `lockd.attachments.stream` for explicit streaming-only access and checksum for integrity checks."),
		}),
		toolAttachmentsStream: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				secretURLLine,
			},
			Purpose:  "Create a one-time download capability URL for attachment payload bytes.",
			UseWhen:  "You need attachment content without pushing payload through MCP tool responses.",
			Requires: fmt.Sprintf("Active MCP session is required. `key` plus either `id` or `name` is required. `namespace` defaults to %q. `public` defaults to true; if `public=false`, `lease_id` is required and if `public=true`, `lease_id` must be omitted.", namespace),
			Effects:  "Returns attachment metadata plus one-time `download_url` and `download_method` for direct HTTP download.",
			Retry:    "Safe to retry; each retry returns a new one-time URL. Do not reuse old URLs.",
			Next:     nextWithBootstrap("fetch `download_url` directly and compare with checksum when integrity checks are needed."),
		}),
		toolAttachmentsDelete: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				leaseFinalizeLine,
			},
			Purpose:  "Delete a single attachment under lease protection.",
			UseWhen:  "One attachment should be removed while keeping others.",
			Requires: withAtomicity(fmt.Sprintf("`key` and `lease_id` are required, plus either `id` or `name`. `namespace` defaults to %q. Optional `txn_id` enables XA staging.", namespace)),
			Effects:  "Deletes (or stages deletion of) the targeted attachment and returns success state.",
			Retry:    "If retrying after unknown outcome, verify attachment state first; do not assume delete status.",
			Next:     nextWithBootstrap("verify remaining attachments and release lease to finalize."),
		}),
		toolAttachmentsDeleteAll: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				leaseFinalizeLine,
			},
			Purpose:  "Delete all attachments for a key under lease protection.",
			UseWhen:  "You need full attachment cleanup for a key.",
			Requires: withAtomicity(fmt.Sprintf("`key` and `lease_id` are required. `namespace` defaults to %q. Optional `txn_id` allows transactional staging.", namespace)),
			Effects:  "Removes (or stages removal of) all attachments for the key.",
			Retry:    "If retrying after unknown outcome, verify attachment list first; do not assume delete-all status.",
			Next:     nextWithBootstrap("optionally write replacements and release lease to finalize."),
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
			Top: []string{
				bootstrapOverviewLine,
				configMutationLine,
			},
			Purpose:  "Update namespace query-engine configuration.",
			UseWhen:  "You need to change preferred/fallback query engines for a namespace.",
			Requires: fmt.Sprintf("`preferred_engine` and `fallback_engine` are required. `namespace` defaults to %q. Optional `if_match` enables CAS-safe updates.", namespace),
			Effects:  "Persists namespace query config and returns updated values/ETag.",
			Retry:    "Prefer `if_match` for deterministic retries; otherwise concurrent updates may overwrite each other.",
			Next:     nextWithBootstrap("optionally call `lockd.index.flush` or rerun queries to validate behavior."),
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
			Effects:  "Returns advisory namespace access hints, default namespace, inline payload limit (`inline_max_payload_bytes`), client id, and token scope context.",
			Retry:    "Safe to retry; this is a read-only advisory operation.",
			Next:     "Call `lockd.help` then use returned namespace hints when invoking lock/query/queue tools.",
		}),
		toolHelp: formatToolDescription(toolContract{
			Purpose:  "Return curated lockd MCP workflows, invariants, and documentation resource URIs.",
			UseWhen:  "Start of session or when uncertain about correct operation sequence.",
			Requires: "No required fields. Optional `topic` narrows guidance to overview/locks/messaging/sync/lql.",
			Effects:  "Returns guidance only; no lockd state mutation occurs.",
			Retry:    "Safe to retry.",
			Next:     "Follow `next_calls` in the response, typically beginning with lock, queue, or query tools.",
		}),
		toolQueueEnqueue: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				payloadRuleLine,
			},
			Purpose:  "Publish a message to a lockd queue for agent coordination.",
			UseWhen:  "You need to signal work/events/context to queue consumers and payload fits inline limits.",
			Requires: fmt.Sprintf("`queue` defaults to %q. `namespace` defaults to %q. Provide at most one of `payload_text` or `payload_base64`; decoded payload must be <= %d bytes.", queue, namespace, inlineMax),
			Effects:  "Appends a queue message and returns delivery metadata (`message_id`, visibility, attempts). Queue ordering is best-effort under retries/requeues; do not assume strict FIFO across all consumers.",
			Retry:    "Not idempotent. If retrying, assume duplicates are possible and rely on application dedupe keys.",
			Next: nextWithBootstrapAndPayload(
				"call `lockd.queue.write_stream.begin` for large payloads.",
				"enqueue and continue with dequeue/ack/nack/defer workflows.",
			),
		}),
		toolQueueWriteStreamBegin: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				secretURLLine,
			},
			Purpose:  "Open a session-scoped streaming queue publisher.",
			UseWhen:  "Queue payload is too large for inline enqueue or you need bounded-memory uploads.",
			Requires: fmt.Sprintf("Active MCP session is required. `queue` defaults to %q and `namespace` defaults to %q. Delay/visibility/ttl/attributes/content_type options match `lockd.queue.enqueue`.", queue, namespace),
			Effects:  "Creates a stream session and returns `stream_id` plus one-time `upload_url` for direct HTTP PUT upload.",
			Retry:    "Do not retry as idempotent. Begin creates a new stream each call; do not reuse identifiers from previous attempts.",
			Next:     nextWithBootstrap("upload to `upload_url`, then status/commit or abort."),
		}),
		toolQueueWriteStreamStatus: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				streamStatusLine,
			},
			Purpose:  "Inspect in-flight queue write-stream upload progress.",
			UseWhen:  "You need upload observability before publish commit.",
			Requires: "Active MCP session and `stream_id` are required.",
			Effects:  "Returns `bytes_received`, computed `payload_sha256`, upload-capability availability/expiry, and commit-readiness flags.",
			Retry:    "Safe to retry while stream exists.",
			Next:     nextWithBootstrap("if `can_commit=true`, call `lockd.queue.write_stream.commit`; otherwise finish upload or abort."),
		}),
		toolQueueWriteStreamCommit: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				streamIDTerminalLine,
			},
			Purpose:  "Finalize a queue write stream and publish the message.",
			UseWhen:  "All payload chunks for a queued message have been appended.",
			Requires: "Active MCP session and `stream_id` are required. Optional `expected_bytes` and `expected_sha256` enforce upload integrity before commit.",
			Effects:  "Closes upload stream, validates optional expectations, waits for upstream enqueue completion, and returns enqueue metadata.",
			Retry:    "After any commit attempt (success or failure), do not retry with the same `stream_id`. Start a new begin→upload→commit sequence.",
			Next:     nextWithBootstrap("continue with dequeue/ack workflows."),
		}),
		toolQueueWriteStreamAbort: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				streamIDTerminalLine,
			},
			Purpose:  "Abort an in-flight queue write stream.",
			UseWhen:  "Queued payload upload should be canceled.",
			Requires: "Active MCP session and `stream_id` are required. Optional `reason` is advisory.",
			Effects:  "Terminates upload flow and frees stream resources.",
			Retry:    "Safe to retry for cleanup, but do not assume abort reopens or reuses the stream; create a new stream for any retry path.",
			Next:     nextWithBootstrap("begin a new queue stream if publish should be retried."),
		}),
		toolQueueDequeue: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				dequeueIdentityLine,
				secretURLLine,
			},
			Purpose:  "Receive one available message lease from a queue.",
			UseWhen:  "A worker is ready to process the next queue item.",
			Requires: withAtomicity(fmt.Sprintf("`queue` defaults to %q. `namespace` defaults to %q. `owner` defaults to OAuth client id. Optional `stateful`, `visibility_seconds`, `cursor`, and `txn_id` tune dequeue behavior. `payload_mode` supports `auto` (default), `inline`, `stream`, `none`; `state_mode` (when `stateful=true`) supports the same values. For both modes, `auto` resolves inline when payload bytes <= `lockd.hint.inline_max_payload_bytes`, otherwise stream.", queue, namespace)),
			Effects:  "Returns `found=false` when no message is available, or lease metadata plus payload delivery according to `payload_mode` (inline fields or stream URL). For `stateful=true`, dequeue is all-or-nothing: state lease acquisition is required for success, and on contention/failure the call returns no leased message.",
			Retry:    "Safe to retry polling, but each retry may lease a different message. Never reuse stale lease/message identifiers.",
			Next:     nextWithBootstrap("process payload/state and then ack/nack/defer."),
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
			Top: []string{
				bootstrapOverviewLine,
				nonIdempotentAckLine,
			},
			Purpose:  "Acknowledge successful processing of a dequeued message.",
			UseWhen:  "Worker completed handling of a leased message.",
			Requires: withAtomicity(fmt.Sprintf("`queue`, `message_id`, `lease_id`, and `meta_etag` are required. `namespace` defaults to %q. Include `fencing_token` and any `state_*` fields exactly as returned by `lockd.queue.dequeue` for deterministic queue/state coordination.", namespace)),
			Effects:  "Confirms processing and removes/commits message state according to lockd queue semantics.",
			Retry:    "Do not replay ack blindly. After transport ambiguity, inspect message/lease state first; after any ack attempt, abandon that delivery attempt and dequeue again.",
			Next:     nextWithBootstrap("dequeue next message or run follow-up updates."),
		}),
		toolQueueNack: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				nonIdempotentAckLine,
			},
			Purpose:  "Requeue a message as a failure, consuming failure budget (`max_attempts`).",
			UseWhen:  "Message processing failed and should be retried later with failure semantics.",
			Requires: withAtomicity(fmt.Sprintf("`queue`, `message_id`, `lease_id`, and `meta_etag` are required. `namespace` defaults to %q. Include `fencing_token` and any `state_*` fields from dequeue. Optional `delay_seconds` and `reason` enrich retry behavior.", namespace)),
			Effects:  "Requeues message with `intent=failure`, updates queue metadata ETag, and records optional failure detail.",
			Retry:    "Do not replay nack for the same delivery attempt. Repeated nack can alter delay/attempt metadata; on ambiguity, inspect state and continue with a new dequeue cycle.",
			Next:     nextWithBootstrap("dequeue next message or inspect retry/dead-letter behavior."),
		}),
		toolQueueDefer: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				nonIdempotentAckLine,
			},
			Purpose:  "Requeue a leased message intentionally without counting it as failure.",
			UseWhen:  "Message is valid but should be processed later or by another worker.",
			Requires: withAtomicity(fmt.Sprintf("`queue`, `message_id`, `lease_id`, and `meta_etag` are required. `namespace` defaults to %q. Include `fencing_token` and any `state_*` fields from dequeue. Optional `delay_seconds` postpones visibility.", namespace)),
			Effects:  "Returns message to queue with defer intent and optional delay.",
			Retry:    "Do not replay defer for the same delivery attempt. Repeated defer can alter visibility timing; on ambiguity, inspect state and continue with a new dequeue cycle.",
			Next:     nextWithBootstrap("continue polling/subscription and dequeue again when ready."),
		}),
		toolQueueExtend: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				dequeueIdentityLine,
			},
			Purpose:  "Extend visibility/lease timeout for a currently leased queue message.",
			UseWhen:  "Message processing is still active and lease timeout is approaching.",
			Requires: withAtomicity(fmt.Sprintf("`queue`, `message_id`, `lease_id`, and `meta_etag` are required. `namespace` defaults to %q. Include `fencing_token` and optional state lease fields from dequeue for stateful workflows.", namespace)),
			Effects:  "Refreshes message lease expiry and visibility timeout; returns updated lease timing and metadata ETag.",
			Retry:    "Retry with the same identifiers only while lease remains valid. If lease may be expired or state mismatched, stop retrying and dequeue a new delivery attempt.",
			Next:     nextWithBootstrap("continue processing and ack/nack/defer when complete."),
		}),
		toolQueueSubscribe: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				sessionScopedLine,
			},
			Purpose:  "Subscribe current MCP session to queue-availability notifications over SSE.",
			UseWhen:  "You run a long-lived MCP runtime and want ongoing push wake-ups instead of pure dequeue polling.",
			Requires: fmt.Sprintf("Active MCP session is required. `queue` defaults to %q and `namespace` defaults to %q.", queue, namespace),
			Effects:  "Registers session subscription; future queue activity emits MCP progress notifications.",
			Retry:    "Safe to retry; duplicate subscribe is handled as already subscribed.",
			Next:     nextWithBootstrap("on notification, call `lockd.queue.dequeue` then `ack`/`nack`/`defer`."),
		}),
		toolQueueUnsubscribe: formatToolDescription(toolContract{
			Top: []string{
				bootstrapOverviewLine,
				sessionScopedLine,
			},
			Purpose:  "Remove current MCP session queue subscription.",
			UseWhen:  "Worker no longer wants notifications for a queue.",
			Requires: fmt.Sprintf("Active MCP session is required. `queue` defaults to %q and `namespace` defaults to %q.", queue, namespace),
			Effects:  "Stops notification forwarding for the targeted session+queue binding.",
			Retry:    "Safe to retry; unsubscribing an absent subscription returns false/unsubscribed state.",
			Next:     nextWithBootstrap("either resubscribe later or continue with explicit dequeue polling."),
		}),
	}
}
