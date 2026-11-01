# Design and trade-off note

**Shape.** Read -> validate -> de-duplicate -> transactional load, one chunk at a time. Memory is
O(chunk_size + reference key sets); the file is never fully loaded. I chose pandas chunks for readable,
vectorised validation; the cost is a per-chunk DataFrame round trip into SQLite rows.

**Why SQLite.** Zero setup, real transactions, constraints (PK/FK/CHECK) and SQL for reconciliation. It is
a stand-in for a warehouse; the write helpers are the only place that would change.

**Two tables, one source of truth.** `order_line_events` is the immutable history; `order_lines` is derived
current state. The state rule (max `event_ts`, tie-break `event_id`) is order-independent, which is what
makes reruns, retries, late data and backfills converge without bookkeeping. Reconciliation re-derives
the state from history and compares.

**Idempotency without a watermark.** Correctness comes from the data model (deterministic `event_id`,
`INSERT OR IGNORE`, guarded upsert), not from remembering how far a previous run got. The sha256 skip is
only an optimisation/guard against accidental re-submission; `--force` bypasses it. In-chunk
`drop_duplicates` is likewise only an optimisation: the DB constraint is the guarantee.

**Rejecting vs counting duplicates.** Duplicates are legitimate at-least-once delivery, so they are counted,
not quarantined (quarantining them would make every rerun produce thousands of rejects). Invalid rows are
quarantined with enough context (file, line, field, raw text) to replay after a fix.

**Keeping bad lines in the stream.** Unparseable lines become sentinel rows instead of being dropped by the
parser, so `rows_in` always equals the number of physical data lines and reject line numbers are exact.
This forces pandas' slower Python engine; I accepted that for traceability.

**Atomic counters.** `run_log` counters are updated inside the same transaction as the chunk, so after any
crash the log describes exactly what is committed.

**Money and time.** Integer cents and UTC fixed-width timestamps avoid float drift and make string
comparison equal time comparison.

**Limitations / what I would do next:** see README (payload-conflict detection, SCD for reference data,
multiprocess validation, warehouse target).
