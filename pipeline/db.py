"""SQLite target: schema, reference loading and idempotent write helpers."""
from __future__ import annotations

import hashlib
import os
import sqlite3

import pandas as pd

from .config import VALID_STATUSES

_STATUS_LIST = ", ".join(f"'{s}'" for s in VALID_STATUSES)

DDL = f"""
CREATE TABLE IF NOT EXISTS customers (
    customer_id   TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    country       TEXT NOT NULL,
    created_at    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS products (
    product_id       TEXT PRIMARY KEY,
    product_name     TEXT NOT NULL,
    category         TEXT NOT NULL,
    list_price_cents INTEGER NOT NULL CHECK (list_price_cents >= 0)
);
-- Append-only history: one row per distinct order-line event. Never updated.
CREATE TABLE IF NOT EXISTS order_line_events (
    event_id         TEXT PRIMARY KEY,
    order_id         TEXT NOT NULL,
    line_no          INTEGER NOT NULL CHECK (line_no >= 1),
    customer_id      TEXT NOT NULL REFERENCES customers(customer_id),
    product_id       TEXT NOT NULL REFERENCES products(product_id),
    quantity         INTEGER NOT NULL CHECK (quantity >= 1),
    unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0),
    line_total_cents INTEGER NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ({_STATUS_LIST})),
    event_ts         TEXT NOT NULL,
    run_id           TEXT NOT NULL,
    source_file      TEXT NOT NULL,
    source_line      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_events_line ON order_line_events(order_id, line_no, event_ts);
-- Current state: latest event per order line (by event_ts, ties broken by event_id).
CREATE TABLE IF NOT EXISTS order_lines (
    order_id         TEXT NOT NULL,
    line_no          INTEGER NOT NULL,
    customer_id      TEXT NOT NULL REFERENCES customers(customer_id),
    product_id       TEXT NOT NULL REFERENCES products(product_id),
    quantity         INTEGER NOT NULL,
    unit_price_cents INTEGER NOT NULL,
    line_total_cents INTEGER NOT NULL,
    status           TEXT NOT NULL,
    event_ts         TEXT NOT NULL,
    last_event_id    TEXT NOT NULL,
    last_run_id      TEXT NOT NULL,
    PRIMARY KEY (order_id, line_no)
);
-- Quarantine: every rejected input row, traceable to file + line.
CREATE TABLE IF NOT EXISTS rejects (
    reject_id     TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL,
    source_file   TEXT NOT NULL,
    source_line   INTEGER NOT NULL,
    reason_code   TEXT NOT NULL,
    reason_detail TEXT,
    raw_record    TEXT,
    rejected_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS run_log (
    run_id             TEXT PRIMARY KEY,
    source_file        TEXT NOT NULL,
    source_sha256      TEXT,
    source_format      TEXT,
    status             TEXT NOT NULL CHECK (status IN ('running','success','failed','skipped')),
    started_at         TEXT NOT NULL,
    ended_at           TEXT,
    rows_in            INTEGER NOT NULL DEFAULT 0,
    rows_valid         INTEGER NOT NULL DEFAULT 0,
    rows_rejected      INTEGER NOT NULL DEFAULT 0,
    rows_duplicate     INTEGER NOT NULL DEFAULT 0,
    rows_new_events    INTEGER NOT NULL DEFAULT 0,
    rows_state_changes INTEGER NOT NULL DEFAULT 0,
    chunks_committed   INTEGER NOT NULL DEFAULT 0,
    failure_reason     TEXT,
    notes              TEXT
);
"""

COUNTER_KEYS = ["rows_in", "rows_valid", "rows_rejected", "rows_duplicate",
                "rows_new_events", "rows_state_changes", "chunks_committed"]


def connect(path: str) -> sqlite3.Connection:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)  # explicit BEGIN/COMMIT
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.executescript(DDL)
    return conn


def _rows(df: pd.DataFrame, cols: list[str]) -> list[list]:
    return df[cols].astype(object).values.tolist()


# ---------------------------------------------------------------- reference data
def load_reference(conn: sqlite3.Connection, customers_csv: str, products_csv: str) -> dict:
    """Idempotent upsert of the (small) dimension files. Bad rows are skipped and counted."""
    out = {}
    cust = pd.read_csv(customers_csv, dtype=str, keep_default_na=False)
    prod = pd.read_csv(products_csv, dtype=str, keep_default_na=False)
    for name, df, req in (("customers", cust, ["customer_id", "customer_name", "country", "created_at"]),
                          ("products", prod, ["product_id", "product_name", "category", "list_price"])):
        miss = [c for c in req if c not in df.columns]
        if miss:
            raise ValueError(f"{name}: missing columns {miss}")
        for c in req:
            df[c] = df[c].str.strip()
    n0c, n0p = len(cust), len(prod)
    cust = cust[(cust != "").all(axis=1)].drop_duplicates("customer_id", keep="first")
    prod = prod[(prod != "").all(axis=1)]
    prod = prod[pd.to_numeric(prod["list_price"], errors="coerce") >= 0].drop_duplicates("product_id", keep="first")
    prod = prod.assign(list_price_cents=(pd.to_numeric(prod["list_price"]) * 100).round().astype("int64"))
    conn.execute("BEGIN")
    try:
        conn.executemany(
            "INSERT INTO customers VALUES (?,?,?,?) ON CONFLICT(customer_id) DO UPDATE SET "
            "customer_name=excluded.customer_name, country=excluded.country, created_at=excluded.created_at",
            _rows(cust, ["customer_id", "customer_name", "country", "created_at"]))
        conn.executemany(
            "INSERT INTO products VALUES (?,?,?,?) ON CONFLICT(product_id) DO UPDATE SET "
            "product_name=excluded.product_name, category=excluded.category, list_price_cents=excluded.list_price_cents",
            _rows(prod, ["product_id", "product_name", "category", "list_price_cents"]))
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    out.update(customers_in=n0c, customers_loaded=len(cust), products_in=n0p, products_loaded=len(prod))
    return out


def reference_ids(conn: sqlite3.Connection) -> tuple[set, set]:
    c = {r[0] for r in conn.execute("SELECT customer_id FROM customers")}
    p = {r[0] for r in conn.execute("SELECT product_id FROM products")}
    return c, p


# ---------------------------------------------------------------- run log
def start_run(conn, run_id: str, source_file: str, started_at: str) -> None:
    conn.execute("INSERT INTO run_log (run_id, source_file, status, started_at) VALUES (?,?,'running',?)",
                 (run_id, source_file, started_at))


def set_source_info(conn, run_id: str, sha: str, fmt: str) -> None:
    conn.execute("UPDATE run_log SET source_sha256=?, source_format=? WHERE run_id=?", (sha, fmt, run_id))


def already_loaded(conn, sha: str) -> bool:
    row = conn.execute("SELECT 1 FROM run_log WHERE source_sha256=? AND status='success' LIMIT 1", (sha,)).fetchone()
    return row is not None


def update_counters(conn, run_id: str, c: dict) -> None:
    sets = ", ".join(f"{k}=?" for k in COUNTER_KEYS)
    conn.execute(f"UPDATE run_log SET {sets} WHERE run_id=?", [c[k] for k in COUNTER_KEYS] + [run_id])


def finish_run(conn, run_id: str, status: str, ended_at: str, failure: str | None, notes: list[str]) -> None:
    conn.execute("UPDATE run_log SET status=?, ended_at=?, failure_reason=?, notes=? WHERE run_id=?",
                 (status, ended_at, failure, "; ".join(notes) or None, run_id))


# ---------------------------------------------------------------- writes (all idempotent)
_EVENT_COLS = ["event_id", "order_id", "line_no", "customer_id", "product_id", "quantity",
               "unit_price_cents", "line_total_cents", "status", "event_ts"]


def insert_events(conn, df: pd.DataFrame, run_id: str, source_file: str) -> int:
    """INSERT OR IGNORE on event_id; returns the number of genuinely new events."""
    if df.empty:
        return 0
    rows = [r[:-1] + [run_id, source_file, r[-1]] for r in _rows(df, _EVENT_COLS + ["_line"])]
    cur = conn.executemany(
        "INSERT OR IGNORE INTO order_line_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    return cur.rowcount


_UPSERT_STATE = """
INSERT INTO order_lines (order_id, line_no, customer_id, product_id, quantity, unit_price_cents,
                         line_total_cents, status, event_ts, last_event_id, last_run_id)
VALUES (?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(order_id, line_no) DO UPDATE SET
    customer_id=excluded.customer_id, product_id=excluded.product_id, quantity=excluded.quantity,
    unit_price_cents=excluded.unit_price_cents, line_total_cents=excluded.line_total_cents,
    status=excluded.status, event_ts=excluded.event_ts,
    last_event_id=excluded.last_event_id, last_run_id=excluded.last_run_id
WHERE excluded.event_ts > order_lines.event_ts
   OR (excluded.event_ts = order_lines.event_ts AND excluded.last_event_id > order_lines.last_event_id)
"""


def upsert_state(conn, df: pd.DataFrame, run_id: str) -> int:
    """Late / out-of-order events never overwrite a newer state. Returns rows changed."""
    if df.empty:
        return 0
    cols = ["order_id", "line_no", "customer_id", "product_id", "quantity", "unit_price_cents",
            "line_total_cents", "status", "event_ts", "event_id"]
    rows = [r + [run_id] for r in _rows(df, cols)]
    return conn.executemany(_UPSERT_STATE, rows).rowcount


def insert_rejects(conn, rej: pd.DataFrame, run_id: str, source_file: str, now: str) -> int:
    if rej.empty:
        return 0
    rows = []
    for line, code, why, raw in rej[["source_line", "reason_code", "reason_detail", "raw_record"]].itertuples(index=False):
        rid = hashlib.sha1(f"{source_file}|{line}|{code}".encode()).hexdigest()[:16]
        rows.append((rid, run_id, source_file, int(line), code, why, raw, now))
    # REPLACE: re-processing the same file refreshes (never duplicates) its rejects.
    conn.executemany("INSERT OR REPLACE INTO rejects VALUES (?,?,?,?,?,?,?,?)", rows)
    return len(rows)
