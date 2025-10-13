# Lockd Backlog

## Integration / Performance / UX follow-up

- Azure Blob backend: end-to-end verification on real accounts (Shared Key and SAS), plus benchmarks mirroring the S3/MinIO suites for large and streaming payloads.
- Disk backend multi-replica benchmarking: stress SSD/NFS scenarios (concurrent writers, induced crashes) and ensure lockfile cleanup behaves under failure.
- Disk verification UX: add CLI/server tests ensuring `lockd verify store` and startup fail fast on misconfigured mounts (read-only, missing locks, etc.).
- CLI/docs polish: expand help text for new storage flags (`--azure-*`, disk verification), document `.env.azure` / `.env.disk` workflows.
- Regression benches: rerun MinIO/Pebble/disk benchmarks after locking changes to confirm no performance regressions.
- Fencing token design: prototype monotonic fencing counters for leases, update API, and add tests/benchmarks once implemented.
- Acquire-for-update coverage: add integration suites for MinIO/S3, disk, Pebble, Azure verifying stream open/close, update+release flows, and lease cleanup on disconnect/timeout.
- Streaming fuzzing: build fuzz tests exercising AcquireForUpdate + UpdateState/Release under random payloads and CAS conflicts, plus aggressive HTTP/2 connection aborts.
- Network fault injection: extend integration harness with simulated TCP resets and slowloris patterns to confirm server releases streaming leases promptly.
- Client refactor: redesign Go client API so `Acquire`/`AcquireForUpdate` return lease/session objects with methods `Update`, `UpdateBytes`, `Get`, `GetBytes`, `Release`, `Close`, etc., perhaps have an `AcquireResponse` and an `AcquireForUpdateResponse` where the latter doesn't have `Get` receiver functions. Update CLI/inprocess helpers and integration tests to use the new object-oriented interface, and only keep free functions where absolutely necessary. Each method should take a `context.Context` as first argument (as before) except `Close()`. Returning object should be a concrete type (struct) with the lease and other fields accessible just like before. There shall be client-level non-receiver-functions `Get`, `Update`, and `Release` aswell that take the object as 2nd argument (after `ctx`) as free helpers essentially.
