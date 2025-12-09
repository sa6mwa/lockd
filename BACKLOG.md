# Backlog / Tracking

- [x] Switch transaction IDs and lease IDs to `github.com/rs/xid`; keep `uuidv7` for keys/manifests. (Implemented.)
- [x] Ensure future XA TC/RM records and any new APIs reuse xid generation; avoid reintroducing uuidv7 for txn/lease.
- [ ] Audit docs/Swagger/CLI help to clearly state xid format for txn/lease IDs and adjust examples accordingly.
- [ ] Add txn-aware smoke cases to each backend suite (disk/azure/minio) that exercise update/remove/keepalive via raw client (no LeaseSession autofill) to assert `missing_txn` handling and commit/rollback semantics.
- [ ] Ensure info/warn/error logs that mention `lease_id` also include `txn_id` (queue delivery/extend, ack/nack, metadata paths); add targeted asserts or log snapshot tests.
- [ ] Add staging cleanup to all backends and run integration suites to validate local TX commit/rollback.
  - [x] mem/query (local TX staging/commit/rollback verified)
  - [x] disk/query
  - [x] minio/lq
  - [x] s3 (aws/query)
  - [x] azure/query
  - [x] nfs/query
- [ ] XA Phase 0–1: transactional release + multi-key prepare/commit/rollback on one node; add txn record type, staging sweeper, restart recovery; cover disk/mem query + lq suites.
- [ ] XA Phase 2: multi-node TC/RM over shared backend; implement RM prepare/commit/rollback endpoints; TC decision record durability and fan-out.
- [ ] XA Phase 2.5: queue enlistment in XA (ACK on commit, NACK on rollback) with backend-local visibility holds and restart recovery.
- [ ] XA Phase 3: backend_hash “island” coordination for mixed backends; global decision record + per-island txn records.
- [ ] XA Phase 4: external RM adapters (e.g., Postgres) using XA-compatible prepare/commit/rollback; conformance tests.
- [ ] Observability: decision logs, watchdogs for long prepares, TC/RM metrics; backlog sweepers for orphaned txn/staging artifacts.
