"""Pipeline orchestrator: read chunk -> validate -> dedupe -> transactional load -> log."""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

from . import db, ingest
from .config import Config
from .validate import validate_chunk

log = logging.getLogger("pipeline")


class InjectedFailure(RuntimeError):
    """Raised by the ``fail_at_chunk`` test hook to simulate a crash mid-load."""


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def new_run_id() -> str:
    return f"run_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:8]}"


def run_pipeline(source_path: str, cfg: Config, *, run_id: str | None = None, force: bool = False,
                 fail_at_chunk: int | None = None) -> dict:
    """Load one source file. Never raises for data/IO problems: the outcome is in the
    returned summary (``status`` = success | failed | skipped) and in ``run_log``.

    ``fail_at_chunk`` (1-based) is a test hook: the chunk's writes are executed and then the
    process "crashes" before COMMIT, proving the chunk rolls back and earlier chunks survive.
    """
    run_id = run_id or new_run_id()
    source_file = os.path.basename(source_path)
    t_start, started_at = time.perf_counter(), utc_now()
    conn = db.connect(cfg.db_path)
    db.start_run(conn, run_id, source_file, started_at)

    counters = {k: 0 for k in db.COUNTER_KEYS}
    timings = {"read_s": 0.0, "validate_s": 0.0, "write_s": 0.0}
    notes: list[str] = []
    status, failure, sha, fmt = "success", None, None, None
    log.info("run_start run_id=%s source=%s chunk_size=%d", run_id, source_file, cfg.chunk_size)

    try:
        sha = ingest.sha256_file(source_path)
        fmt = ingest.detect_format(source_path)
        db.set_source_info(conn, run_id, sha, fmt)

        if not force and db.already_loaded(conn, sha):
            status = "skipped"
            notes.append("identical file (sha256) already loaded successfully; use force to reprocess")
            log.warning("run_skipped run_id=%s reason=already_loaded", run_id)
        else:
            cust_ids, prod_ids = db.reference_ids(conn)
            if not cust_ids or not prod_ids:
                raise RuntimeError("reference tables are empty - load customers/products first")
            chunks = iter(ingest.iter_chunks(source_path, cfg.chunk_size, notes))
            n = 0
            while True:
                t0 = time.perf_counter()
                chunk = next(chunks, None)
                timings["read_s"] += time.perf_counter() - t0
                if chunk is None:
                    break
                n += 1

                t0 = time.perf_counter()
                good, rej = validate_chunk(chunk, cust_ids, prod_ids, cfg)
                deduped = good.drop_duplicates("event_id", keep="first")
                timings["validate_s"] += time.perf_counter() - t0

                t0 = time.perf_counter()
                conn.execute("BEGIN")
                try:
                    new_events = db.insert_events(conn, deduped, run_id, source_file)
                    state_changes = db.upsert_state(conn, deduped, run_id)
                    db.insert_rejects(conn, rej, run_id, source_file, utc_now())
                    nxt = dict(counters)
                    nxt["rows_in"] += len(chunk)
                    nxt["rows_valid"] += len(good)
                    nxt["rows_rejected"] += len(rej)
                    nxt["rows_duplicate"] += len(good) - new_events
                    nxt["rows_new_events"] += new_events
                    nxt["rows_state_changes"] += state_changes
                    nxt["chunks_committed"] += 1
                    db.update_counters(conn, run_id, nxt)
                    if fail_at_chunk is not None and n == fail_at_chunk:
                        raise InjectedFailure(f"simulated crash in chunk {n} before commit")
                    conn.execute("COMMIT")
                except Exception:
                    if conn.in_transaction:
                        conn.execute("ROLLBACK")
                    raise
                counters = nxt
                timings["write_s"] += time.perf_counter() - t0
                log.debug("chunk_committed n=%d rows=%d rejected=%d", n, len(chunk), len(rej))
            if counters["rows_in"] == 0:
                notes.append("empty batch: no data rows")
    except Exception as exc:  # recorded, not swallowed: status=failed + failure_reason
        status, failure = "failed", f"{type(exc).__name__}: {exc}"
        log.error("run_failed run_id=%s reason=%s", run_id, failure)

    db.finish_run(conn, run_id, status, utc_now(), failure, notes)
    conn.close()
    elapsed = time.perf_counter() - t_start
    summary = {
        "run_id": run_id, "source_file": source_file, "source_sha256": sha, "source_format": fmt,
        "status": status, "failure_reason": failure, "started_at": started_at,
        "elapsed_s": round(elapsed, 3),
        "rows_per_s": round(counters["rows_in"] / elapsed) if elapsed > 0 else None,
        "timings_s": {k: round(v, 3) for k, v in timings.items()},
        "notes": notes, **counters,
    }
    log.info("run_end %s", json.dumps(summary))
    return summary
