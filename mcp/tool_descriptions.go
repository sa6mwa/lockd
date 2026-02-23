package mcp

import (
	"fmt"
	"strings"
)

const (
	toolLockAcquire          = "lockd.lock.acquire"
	toolLockKeepAlive        = "lockd.lock.keepalive"
	toolLockRelease          = "lockd.lock.release"
	toolGet                  = "lockd.get"
	toolDescribe             = "lockd.describe"
	toolQuery                = "lockd.query"
	toolStateUpdate          = "lockd.state.update"
	toolStateMetadata        = "lockd.state.metadata"
	toolStateRemove          = "lockd.state.remove"
	toolAttachmentsPut       = "lockd.attachments.put"
	toolAttachmentsList      = "lockd.attachments.list"
	toolAttachmentsGet       = "lockd.attachments.get"
	toolAttachmentsDelete    = "lockd.attachments.delete"
	toolAttachmentsDeleteAll = "lockd.attachments.delete_all"
	toolNamespaceGet         = "lockd.namespace.get"
	toolNamespaceUpdate      = "lockd.namespace.update"
	toolIndexFlush           = "lockd.index.flush"
	toolHelp                 = "lockd.help"
	toolQueueEnqueue         = "lockd.queue.enqueue"
	toolQueueDequeue         = "lockd.queue.dequeue"
	toolQueueAck             = "lockd.queue.ack"
	toolQueueNack            = "lockd.queue.nack"
	toolQueueDefer           = "lockd.queue.defer"
	toolQueueExtend          = "lockd.queue.extend"
	toolQueueSubscribe       = "lockd.queue.subscribe"
	toolQueueUnsubscribe     = "lockd.queue.unsubscribe"
)

var mcpToolNames = []string{
	toolLockAcquire,
	toolLockKeepAlive,
	toolLockRelease,
	toolGet,
	toolDescribe,
	toolQuery,
	toolStateUpdate,
	toolStateMetadata,
	toolStateRemove,
	toolAttachmentsPut,
	toolAttachmentsList,
	toolAttachmentsGet,
	toolAttachmentsDelete,
	toolAttachmentsDeleteAll,
	toolNamespaceGet,
	toolNamespaceUpdate,
	toolIndexFlush,
	toolHelp,
	toolQueueEnqueue,
	toolQueueDequeue,
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
			Requires: fmt.Sprintf("`key` is required. `namespace` defaults to %q. `public=true` enables public-read fallback; optional `lease_id` scopes lease-bound reads.", namespace),
			Effects:  "Returns read-only state, ETag/version metadata, and `found=false` when no state exists.",
			Retry:    "Safe to retry; this is a read operation.",
			Next:     "Use `lockd.query` for broader discovery or acquire a lock before mutation.",
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
			Purpose:  "Run LQL queries over namespace data in key or document mode.",
			UseWhen:  "You need to locate candidate keys/documents before reading or locking.",
			Requires: fmt.Sprintf("`query` expression is required. `namespace` defaults to %q. Optional `limit`, `cursor`, `return`, `engine`, `refresh`, and `fields` refine execution.", namespace),
			Effects:  "Returns matching keys or documents plus pagination/index metadata (`cursor`, `index_seq`).",
			Retry:    "Safe to retry. Cursor-based pagination should reuse the latest returned cursor.",
			Next:     "Call `lockd.get` for point reads, then lock/mutate selected keys as needed.",
		}),
		toolStateUpdate: formatToolDescription(toolContract{
			Purpose:  "Write JSON state under lease protection.",
			UseWhen:  "You need to create or update state for a locked key.",
			Requires: fmt.Sprintf("`key` and `lease_id` are required. `namespace` defaults to %q. Payload comes from `payload_text` or `payload_base64` (defaults to `{}`). Optional CAS/fencing fields harden concurrency safety.", namespace),
			Effects:  "Updates state and returns new version/ETag metadata; can also mutate `query_hidden` metadata.",
			Retry:    "Use `if_etag`, `if_version`, and `fencing_token` for safe retries. Without guards, retries can apply duplicate writes.",
			Next:     "Optionally update metadata/attachments, then release lease to commit or rollback.",
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
		toolAttachmentsPut: formatToolDescription(toolContract{
			Purpose:  "Upload or stage an attachment for a key under lease protection.",
			UseWhen:  "You need to associate binary/text payloads with key state.",
			Requires: fmt.Sprintf("`key`, `lease_id`, and `name` are required. `namespace` defaults to %q. Payload comes from `payload_text` or `payload_base64`. Optional `prevent_overwrite` enforces create-only behavior.", namespace),
			Effects:  "Stores attachment content/metadata and returns attachment id/version details.",
			Retry:    "Not strictly idempotent by default; retries can overwrite unless guarded by `prevent_overwrite` and deterministic naming.",
			Next:     "List/get attachments for verification, then release lease when complete.",
		}),
		toolAttachmentsList: formatToolDescription(toolContract{
			Purpose:  "List attachment metadata for a key.",
			UseWhen:  "You need attachment ids/names before get/delete workflows.",
			Requires: fmt.Sprintf("`key` is required. `namespace` defaults to %q. Provide lease fields for protected reads or set `public=true` for public-read listing.", namespace),
			Effects:  "Returns attachment metadata only (no payload bytes).",
			Retry:    "Safe to retry; this is a read operation.",
			Next:     "Use `lockd.attachments.get` for payload access or delete tools for cleanup.",
		}),
		toolAttachmentsGet: formatToolDescription(toolContract{
			Purpose:  "Fetch one attachment payload by id or name.",
			UseWhen:  "You need the actual attachment content for processing.",
			Requires: fmt.Sprintf("`key` plus either `id` or `name` is required. `namespace` defaults to %q. Provide lease fields for protected reads or set `public=true` for public-read access.", namespace),
			Effects:  "Returns attachment metadata plus payload (`payload_base64`, and `payload_text` when UTF-8).",
			Retry:    "Safe to retry; this is a read operation.",
			Next:     "Process payload or delete/replace attachment under lease as needed.",
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
			UseWhen:  "You need to signal work/events/context to queue consumers.",
			Requires: fmt.Sprintf("`queue` defaults to %q. `namespace` defaults to %q. Provide payload via `payload_text` or `payload_base64`; optional delay/visibility/ttl/attributes tune delivery.", queue, namespace),
			Effects:  "Appends a queue message and returns delivery metadata (`message_id`, visibility, attempts).",
			Retry:    "Not idempotent by default; retries can enqueue duplicates. Use application-level dedupe keys in payload/attributes.",
			Next:     "Consumers should `lockd.queue.dequeue` then `lockd.queue.ack` or `lockd.queue.defer`.",
		}),
		toolQueueDequeue: formatToolDescription(toolContract{
			Purpose:  "Receive one available message lease from a queue.",
			UseWhen:  "A worker is ready to process the next queue item.",
			Requires: fmt.Sprintf("`queue` defaults to %q. `namespace` defaults to %q. `owner` defaults to OAuth client id. Optional `stateful`, `visibility_seconds`, `page_size`, `start_after`, and `txn_id` tune dequeue semantics.", queue, namespace),
			Effects:  "Returns `found=false` when no message is available, or message payload plus lease fields required for ack/nack/defer/extend.",
			Retry:    "Safe to retry. Duplicate retries may lease different messages when queue state changes.",
			Next:     "If processed, call `lockd.queue.ack`; if failed call `lockd.queue.nack`; if not for this worker call `lockd.queue.defer`; extend long handlers with `lockd.queue.extend`.",
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
			UseWhen:  "You want push wake-ups instead of pure dequeue polling.",
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
