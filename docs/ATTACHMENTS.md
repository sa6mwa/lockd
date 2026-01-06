# Attachments

State attachments let you stream binary payloads alongside a lease-backed key.
Attachments are staged under the lease transaction and become publicly readable
only after the lease is released (commit). Rollbacks discard staged changes.

## Lifecycle

- Upload while holding a lease.
- Attachments are staged under the transaction and register the key as a txn participant.
- Release commits the attachment metadata and payload.
- Public reads can list/download attachments after commit.
- Delete operations are also staged and applied on release.

Attachments are stored under:

- Committed: `state/<key>/attachments/<uuidv7>`
- Staged: `state/<key>/.staging/<txn_id>/attachments/<uuidv7>`

Attachment payloads flow through the same encryption-at-rest pipeline as
state and queue data when storage crypto is enabled.

## Limits and defaults

- `max_bytes` caps payload size per request. Set to `0` to allow unlimited size.
- When not set, the server uses its default attachment size limit (1 TiB by
  default via `DefaultAttachmentMaxBytes`).
- `content_type` defaults to `application/octet-stream` when omitted.
- `prevent_overwrite=true` makes the operation idempotent: if an attachment with
  the same name already exists, the upload is skipped and the existing
  attachment metadata is returned.
- When overwriting by name, the existing attachment UUIDv7 is reused.

## HTTP API

- `POST /v1/attachments` (upload)
- `GET /v1/attachments` (list)
- `GET /v1/attachment` (download)
- `DELETE /v1/attachment` (delete one)
- `DELETE /v1/attachments` (delete all)

Common query params:

- `namespace` (optional)
- `key` (required)
- `name` or `id` for single-attachment operations
- `public=true` to read without a lease
- `content_type`, `max_bytes`, `prevent_overwrite` for uploads

Lease-bound calls must supply `X-Lease-ID` + `X-Txn-ID` headers and the current
fencing token (when required by the client/SDK).

## Go SDK

```go
lease, _ := cli.Acquire(ctx, api.AcquireRequest{Key: "orders", Owner: "worker-1", TTLSeconds: 30})
file, _ := os.Open("invoice.pdf")
_, _ = lease.Attach(ctx, client.AttachRequest{
    Name:        "invoice.pdf",
    ContentType: "application/pdf",
    Body:        file,
})
_ = lease.Release(ctx)

resp, _ := cli.Get(ctx, "orders")
list, _ := resp.ListAttachments(ctx)
_ = list
```

Delete staged attachments before release:

```go
_, _ = lease.DeleteAttachment(ctx, client.AttachmentSelector{Name: "invoice.pdf"})
_, _ = lease.DeleteAllAttachments(ctx)
```

## CLI

```bash
eval "$(lockd client acquire --key orders)"
lockd client attachments put --name invoice.pdf --file ./invoice.pdf --content-type application/pdf
lockd client release

lockd client attachments list --key orders --public
lockd client attachments get --key orders --name invoice.pdf --public -o invoice.pdf
```
