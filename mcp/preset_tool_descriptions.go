package mcp

import (
	"fmt"
	"sort"
	"strings"

	presetcfg "pkt.systems/lockd/mcp/preset"
)

const presetHelpFirstLine = "DISCOVERY: If you have not reviewed this preset in the current session, call this tool first and use its tool list as the authoritative surface."
const presetNamespaceLine = "SURFACE: Namespace is fixed by the preset binding; never invent or pass namespace arguments for preset tools."
const presetQueryLine = "QUERY DIALECT: `query` is lockd LQL, not natural-language search. Use exact field paths like `/title`, `/tags`, `/text`, `/company`, or `/time`."
const presetAttachmentLine = "SENSITIVE: Attachment payloads or one-time stream URLs may be returned; do not echo raw payload secrets or capability URLs into user-visible summaries."

func presetHelpToolDescription(def presetcfg.Definition) string {
	return formatToolDescription(toolContract{
		Top: []string{
			presetHelpFirstLine,
			presetNamespaceLine,
		},
		Purpose:  fmt.Sprintf("Summarize the generated %s preset tools and tell the agent which operations exist before it starts guessing.", def.Name),
		UseWhen:  "You are starting work with this preset, you need to confirm the available kinds/operations, or a prior tool call failed and you need to re-anchor on the actual surface.",
		Requires: "No arguments. The result is scoped to this preset only.",
		Effects:  "Returns the preset name, preset description, and the generated tool names that are actually available for this preset binding.",
		Retry:    "Safe to retry; read-only surface discovery.",
		Next:     "Pick the exact generated tool you need next. For retrieval, usually start with the relevant `<preset>.<kind>.query` or `<preset>.<kind>.state.get` tool.",
	})
}

func presetQueryToolDescription(def presetcfg.Definition, kind presetcfg.Kind) string {
	examples := presetQueryExamples(def.Name, kind)
	return formatToolDescription(toolContract{
		Top: []string{
			fmt.Sprintf("DISCOVERY: If you have not reviewed `%s.help` in this session, call it before relying on this query tool.", def.Name),
			presetQueryLine,
		},
		Purpose:  fmt.Sprintf("Run LQL queries over `%s` %s records and return matching keys for follow-up reads or updates.", def.Name, kindLabel(kind)),
		UseWhen:  fmt.Sprintf("You need to discover candidate %s records by tags, keywords, titles, names, URLs, dates, or other indexed fields before calling `%s.%s.state.get`.", kindLabel(kind), def.Name, kind.Name),
		Requires: fmt.Sprintf("`query` is required and is evaluated only within namespace %q for `%s` %s records. `limit` caps returned keys. `cursor` continues pagination from a previous result. Use empty query string (`\"\"`) to enumerate all keys for this kind.", kind.Namespace, def.Name, kindLabel(kind)),
		Effects:  fmt.Sprintf("Returns matching `keys`, optional `cursor` for continuation, observed `index_seq`, and lockd query metadata. It does not return documents; use the returned keys with `%s.%s.state.get`.", def.Name, kind.Name),
		Retry:    "Safe to retry; read-only query. If you receive a continuation cursor, pass that exact cursor back unchanged to fetch the next page.",
		Next:     fmt.Sprintf("Use returned keys with `%s.%s.state.get`, or refine the query if the result set is too broad. LQL examples for this tool: %s", def.Name, kind.Name, examples),
	})
}

func presetStatePutToolDescription(def presetcfg.Definition, kind presetcfg.Kind) string {
	required := append([]string{"key"}, kind.Schema.Required...)
	sort.Strings(required)
	return formatToolDescription(toolContract{
		Top: []string{
			fmt.Sprintf("DISCOVERY: Review `%s.help` if you are unsure which kind-specific fields belong on this `%s` write.", def.Name, kind.Name),
			presetNamespaceLine,
		},
		Purpose:  fmt.Sprintf("Create or replace one `%s` %s record using the preset schema as the durable API contract.", def.Name, kindLabel(kind)),
		UseWhen:  fmt.Sprintf("You already know the target key and want to store a `%s` %s record without using raw lockd document tools.", def.Name, kindLabel(kind)),
		Requires: fmt.Sprintf("`key` plus schema fields are accepted. Required fields for this kind are %s. The payload is serialized into the preset schema and written into fixed namespace %q.", quotedList(required), kind.Namespace),
		Effects:  "Writes the document and returns the flattened preset view: `_lockd_key`, optional `_lockd_attachments`, and the schema fields at top level.",
		Retry:    "If you retry after uncertain completion, treat this as replace-by-key semantics: the latest successful call wins for that key.",
		Next:     fmt.Sprintf("Use `%s.%s.state.get` to verify stored fields, `%s.%s.attachments.get` if the record has attachments, or `%s.%s.query` to rediscover it later.", def.Name, kind.Name, def.Name, kind.Name, def.Name, kind.Name),
	})
}

func presetStateGetToolDescription(def presetcfg.Definition, kind presetcfg.Kind) string {
	return formatToolDescription(toolContract{
		Top: []string{
			fmt.Sprintf("DISCOVERY: `%s.help` lists the sibling tools that can act on the same `%s` record after this read.", def.Name, kind.Name),
			presetNamespaceLine,
		},
		Purpose:  fmt.Sprintf("Read one `%s` %s record by key and return it in the flattened preset API shape.", def.Name, kindLabel(kind)),
		UseWhen:  fmt.Sprintf("You already know the key from user context or from `%s.%s.query` and need the actual stored fields.", def.Name, kind.Name),
		Requires: fmt.Sprintf("`key` is required. Namespace is fixed to %q. Output is not raw lockd JSON: schema fields are flattened at top level with `_lockd_key` and optional `_lockd_attachments` metadata.", kind.Namespace),
		Effects:  "Returns one record in preset form. If attachments exist, `_lockd_attachments` lists attachment metadata so the agent can decide whether to fetch one with the attachments tool.",
		Retry:    "Safe to retry; read-only point lookup.",
		Next:     fmt.Sprintf("Use `%s.%s.attachments.get` to fetch a specific attachment by name, `%s.%s.state.put` to overwrite fields, or `%s.%s.state.delete` to remove the record.", def.Name, kind.Name, def.Name, kind.Name, def.Name, kind.Name),
	})
}

func presetStateDeleteToolDescription(def presetcfg.Definition, kind presetcfg.Kind) string {
	return formatToolDescription(toolContract{
		Top: []string{
			fmt.Sprintf("DISCOVERY: Confirm the key with `%s.%s.state.get` first if you are not certain deletion is intended.", def.Name, kind.Name),
			presetNamespaceLine,
		},
		Purpose:  fmt.Sprintf("Delete one `%s` %s record by key.", def.Name, kindLabel(kind)),
		UseWhen:  "The user intent is removal, archival cleanup, or replacement via a fresh write under the same key.",
		Requires: fmt.Sprintf("`key` is required. This deletes from the fixed preset namespace %q and does not require raw lockd lease parameters.", kind.Namespace),
		Effects:  "Removes the keyed record and returns `_lockd_key` plus `removed` to confirm whether a record was deleted.",
		Retry:    "Usually safe to retry, but verify current state if deletion outcome matters for subsequent workflow steps.",
		Next:     fmt.Sprintf("If the record should still exist, recreate it with `%s.%s.state.put`; otherwise continue with adjacent records via `%s.%s.query`.", def.Name, kind.Name, def.Name, kind.Name),
	})
}

func presetQueueEnqueueToolDescription(def presetcfg.Definition, kind presetcfg.Kind) string {
	return formatToolDescription(toolContract{
		Top: []string{
			fmt.Sprintf("DISCOVERY: Use `%s.help` to confirm this kind supports queue enqueue before constructing message payloads.", def.Name),
			presetNamespaceLine,
		},
		Purpose:  fmt.Sprintf("Publish one `%s` %s-shaped payload into a lockd queue using the same schema fields as the preset record.", def.Name, kindLabel(kind)),
		UseWhen:  "You need asynchronous coordination, reminders, work dispatch, or event delivery rather than durable keyed state.",
		Requires: fmt.Sprintf("Queue payload fields follow the same schema as `%s.%s.state.put`, but there is no `key`. `queue` may be omitted to use the default queue. Delay/visibility/TTL/attempt settings are optional queue controls, not schema fields.", def.Name, kind.Name),
		Effects:  "Enqueues a message and returns queue/message metadata such as `message_id`, visibility deadline, payload size, and correlation id.",
		Retry:    "Do not blindly retry after uncertain enqueue outcome unless duplicate publication is acceptable for your workflow.",
		Next:     "If the payload should also be durable memory, write it separately with the state tool. Otherwise hand off to whatever queue consumer is responsible for this message type.",
	})
}

func presetAttachmentsGetToolDescription(def presetcfg.Definition, kind presetcfg.Kind) string {
	return formatToolDescription(toolContract{
		Top: []string{
			fmt.Sprintf("DISCOVERY: Call `%s.%s.state.get` first to inspect `_lockd_attachments` and confirm the attachment name you need.", def.Name, kind.Name),
			presetAttachmentLine,
		},
		Purpose:  fmt.Sprintf("Fetch one attachment associated with a `%s` %s record.", def.Name, kindLabel(kind)),
		UseWhen:  "The record has attachment metadata and you need one named attachment payload or its retrieval metadata.",
		Requires: "`key` and attachment `name` are required. `payload_mode` controls whether payload is omitted, returned inline when small, or provided via a one-time stream URL when large. Use `none` when you only need metadata.",
		Effects:  "Returns attachment metadata plus payload delivery fields according to `payload_mode`.",
		Retry:    "Safe to retry for metadata reads; for large payloads prefer fresh retrieval if a prior stream URL may have expired.",
		Next:     fmt.Sprintf("If you only needed to confirm attachment existence, return to `%s.%s.state.get`. If you need another attachment, call this tool again with the next attachment `name`.", def.Name, kind.Name),
	})
}

func presetQueryExamples(presetName string, kind presetcfg.Kind) string {
	if strings.EqualFold(presetName, "memory") {
		switch strings.ToLower(kind.Name) {
		case "memory":
			return "`\"\"` for all memory keys; `icontains{field=/text,value=renewal}` for free-text recall; `in{field=/tags,any=project-x|urgent}` for tag filtering."
		case "bookmarks":
			return "`\"\"` for all bookmarks; `/title=\"Quarterly plan\"` for exact title match; `icontains{field=/summary,value=benchmark}` or `icontains{field=/url,value=docs}` for fuzzy lookup."
		case "contacts":
			return "`\"\"` for all contacts; `/name=\"Annie Case\"` for exact name lookup; `icontains{field=/company,value=openai}` or `icontains{field=/email,value=@example.com}` for partial matches."
		case "reminders":
			return "`\"\"` for all reminders; `/title=\"Renew domain\"` for exact title lookup; `date{field=/time,since=today}` or `icontains{field=/summary,value=invoice}` for time/text retrieval."
		case "todo":
			return "`\"\"` for all todos; `/title=\"Ship MCP preset docs\"` for exact task lookup; `in{field=/tags,any=ops|q2}` or `icontains{field=/summary,value=follow up}` for filtered retrieval."
		}
	}
	return "`\"\"` to enumerate all keys, exact field comparisons such as `/title=\"...\"`, and `icontains{field=/...,value=...}` or `in{field=/tags,any=...}` for broader recall."
}

func kindLabel(kind presetcfg.Kind) string {
	if strings.TrimSpace(kind.Description) != "" {
		return strings.TrimSpace(kind.Description)
	}
	return kind.Name
}

func quotedList(values []string) string {
	if len(values) == 0 {
		return "(none)"
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, fmt.Sprintf("`%s`", value))
	}
	return strings.Join(out, ", ")
}
