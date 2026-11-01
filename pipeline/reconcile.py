"""Reconciliation checks + sample analytical reports."""
from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass, asdict


@dataclass
class Check:
    name: str
    passed: bool
    detail: str


def _one(conn, sql, *a):
    return conn.execute(sql, a).fetchone()[0]


def reconcile(conn: sqlite3.Connection) -> list[Check]:
    """Internal-consistency checks; valid for any DB state (any number of runs)."""
    out: list[Check] = []

    bad = conn.execute("""SELECT run_id, rows_in, rows_rejected, rows_duplicate, rows_new_events FROM run_log
        WHERE status IN ('success','failed') AND rows_in != rows_rejected + rows_duplicate + rows_new_events""").fetchall()
    n_runs = _one(conn, "SELECT COUNT(*) FROM run_log WHERE status IN ('success','failed')")
    out.append(Check("per-run identity: rows_in = rejected + duplicate + new_events", not bad,
                     f"{n_runs} runs checked, {len(bad)} violations"))

    ev, logged = _one(conn, "SELECT COUNT(*) FROM order_line_events"), \
        _one(conn, "SELECT COALESCE(SUM(rows_new_events),0) FROM run_log")
    out.append(Check("history rows = sum(run_log.rows_new_events)", ev == logged, f"events={ev}, logged={logged}"))

    lines, distinct = _one(conn, "SELECT COUNT(*) FROM order_lines"), \
        _one(conn, "SELECT COUNT(*) FROM (SELECT DISTINCT order_id, line_no FROM order_line_events)")
    out.append(Check("current-state lines = distinct (order_id, line_no) in history", lines == distinct,
                     f"order_lines={lines}, distinct_in_history={distinct}"))

    mism = _one(conn, """SELECT COUNT(*) FROM order_lines s JOIN (
            SELECT order_id, line_no, event_id, status, line_total_cents,
                   ROW_NUMBER() OVER (PARTITION BY order_id, line_no ORDER BY event_ts DESC, event_id DESC) rn
            FROM order_line_events) e ON e.order_id=s.order_id AND e.line_no=s.line_no AND e.rn=1
        WHERE s.last_event_id != e.event_id OR s.status != e.status OR s.line_total_cents != e.line_total_cents""")
    out.append(Check("current state = latest event per line (late data never wins)", mism == 0, f"{mism} mismatches"))

    orph_c = _one(conn, "SELECT COUNT(*) FROM order_line_events e LEFT JOIN customers c USING(customer_id) WHERE c.customer_id IS NULL")
    orph_p = _one(conn, "SELECT COUNT(*) FROM order_line_events e LEFT JOIN products p USING(product_id) WHERE p.product_id IS NULL")
    fk = len(conn.execute("PRAGMA foreign_key_check").fetchall())
    out.append(Check("referential integrity (customers, products, FK check)", orph_c == orph_p == fk == 0,
                     f"orphan_customers={orph_c}, orphan_products={orph_p}, fk_violations={fk}"))

    bad_total = _one(conn, "SELECT COUNT(*) FROM order_line_events WHERE line_total_cents != quantity * unit_price_cents")
    out.append(Check("derived field line_total_cents = quantity * unit_price_cents", bad_total == 0, f"{bad_total} violations"))

    cur_sum, hist_latest = _one(conn, "SELECT COALESCE(SUM(line_total_cents),0) FROM order_lines"), _one(
        conn, """SELECT COALESCE(SUM(line_total_cents),0) FROM (SELECT line_total_cents,
                 ROW_NUMBER() OVER (PARTITION BY order_id, line_no ORDER BY event_ts DESC, event_id DESC) rn
                 FROM order_line_events) WHERE rn=1""")
    out.append(Check("key aggregate: sum(line_total_cents) current state = latest-in-history", cur_sum == hist_latest,
                     f"state={cur_sum}, history_latest={hist_latest}"))
    return out


def check_against_expected(conn, run_id: str, expected_summary: str, labels_csv: str) -> list[Check]:
    """Compare one run on a *fresh* DB with the generator's ground truth (kept outside the input)."""
    exp = json.load(open(expected_summary))
    run = conn.execute("SELECT status, rows_in, rows_rejected, rows_duplicate, rows_new_events FROM run_log WHERE run_id=?",
                       (run_id,)).fetchone()
    out = [Check("run finished with status=success", bool(run) and run[0] == "success", f"status={run[0] if run else None}")]
    out.append(Check("rows_in = generator total_rows", run[1] == exp["total_rows"], f"{run[1]} vs {exp['total_rows']}"))
    out.append(Check("rejected count matches labels", run[2] == exp["expected_rejected_total"], f"{run[2]} vs {exp['expected_rejected_total']}"))
    out.append(Check("duplicate count matches", run[3] == exp["expected_duplicates"], f"{run[3]} vs {exp['expected_duplicates']}"))
    out.append(Check("new events match", run[4] == exp["expected_new_events"], f"{run[4]} vs {exp['expected_new_events']}"))

    # Rejects are keyed by (source_file, line, reason) and refreshed when a file is reprocessed,
    # so compare by file rather than by the (possibly superseded) run_id.
    src = conn.execute("SELECT source_file FROM run_log WHERE run_id=?", (run_id,)).fetchone()[0]
    got = {(int(l), c) for l, c in conn.execute("SELECT source_line, reason_code FROM rejects WHERE source_file=?", (src,))}
    with open(labels_csv) as f:
        want = {(int(r["source_line"]), r["expected_reason_code"]) for r in csv.DictReader(f)}
    out.append(Check("every reject traced to the labelled (line, reason)", got == want,
                     f"got={len(got)}, labelled={len(want)}, missing={len(want - got)}, unexpected={len(got - want)}"))

    lines = _one(conn, "SELECT COUNT(*) FROM order_lines")
    out.append(Check("current-state line count matches", lines == exp["expected_current_lines"], f"{lines} vs {exp['expected_current_lines']}"))
    sums = dict(conn.execute("SELECT status, SUM(line_total_cents) FROM order_lines GROUP BY status"))
    out.append(Check("current-state revenue by status matches", sums == exp["expected_current_sum_cents_by_status"],
                     f"statuses compared={len(sums)}"))
    return out


SAMPLE_QUERIES = {
    "Current order lines and revenue by status": """
        SELECT status, COUNT(*) AS order_lines, SUM(line_total_cents)/100.0 AS revenue
        FROM order_lines GROUP BY status ORDER BY order_lines DESC""",
    "Top 5 categories by net revenue (excl. cancelled/returned)": """
        SELECT p.category, COUNT(*) AS order_lines, ROUND(SUM(s.line_total_cents)/100.0, 2) AS revenue
        FROM order_lines s JOIN products p USING(product_id)
        WHERE s.status NOT IN ('cancelled','returned') GROUP BY p.category ORDER BY revenue DESC LIMIT 5""",
    "Rejects by reason": "SELECT reason_code, COUNT(*) AS rows FROM rejects GROUP BY reason_code ORDER BY rows DESC",
    "Superseded (historical) events kept for audit": """
        SELECT COUNT(*) AS superseded_events FROM order_line_events e
        JOIN order_lines s ON s.order_id=e.order_id AND s.line_no=e.line_no WHERE e.event_id != s.last_event_id""",
    "Run history": """
        SELECT run_id, source_file, status, rows_in, rows_rejected, rows_duplicate, rows_new_events, chunks_committed
        FROM run_log ORDER BY started_at""",
}


def _md_table(cols, rows) -> str:
    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    lines += ["| " + " | ".join(str(v) for v in r) + " |" for r in rows]
    return "\n".join(lines)


def write_report(conn, checks: list[Check], md_path: str, json_path: str) -> bool:
    ok = all(c.passed for c in checks)
    with open(json_path, "w") as f:
        json.dump({"all_passed": ok, "checks": [asdict(c) for c in checks]}, f, indent=2)
    parts = ["# Reconciliation & validation report", "",
             f"**Overall: {'PASS' if ok else 'FAIL'}** ({sum(c.passed for c in checks)}/{len(checks)} checks)", "",
             _md_table(["check", "result", "detail"], [(c.name, "PASS" if c.passed else "FAIL", c.detail) for c in checks]), "",
             "## Sample analytical queries", ""]
    for title, sql in SAMPLE_QUERIES.items():
        cur = conn.execute(sql)
        parts += [f"### {title}", "", _md_table([d[0] for d in cur.description], cur.fetchall()), ""]
    with open(md_path, "w") as f:
        f.write("\n".join(parts))
    return ok
