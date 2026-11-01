import os
import sqlite3

import pytest

from pipeline import db, reconcile as rec
from pipeline.config import Config
from pipeline.generator import generate
from pipeline.ingest import iter_chunks
from pipeline.run import run_pipeline
from .conftest import HEADER, ev


def reasons(env):
    return dict(env.q("SELECT source_line, reason_code FROM rejects"))


def state(env):
    return env.q("SELECT order_id, line_no, status, event_ts, line_total_cents FROM order_lines ORDER BY 1,2")


# ---------------------------------------------------------------- happy path
def test_happy_path_csv(env):
    p = env.write_csv("a.csv", [ev(status="created"), ev(status="paid", ts="2026-01-01T11:00:00Z"),
                                ev(order="O2", prod="P2", qty="3", price="25.50")])
    s = run_pipeline(p, env.cfg)
    assert s["status"] == "success"
    assert (s["rows_in"], s["rows_rejected"], s["rows_duplicate"], s["rows_new_events"]) == (3, 0, 0, 3)
    assert s["chunks_committed"] == 1
    assert state(env) == [("O1", 1, "paid", "2026-01-01T11:00:00.000000Z", 2000),
                          ("O2", 1, "created", "2026-01-01T10:00:00.000000Z", 7650)]
    assert all(c.passed for c in rec.reconcile(sqlite3.connect(env.cfg.db_path)))


def test_happy_path_jsonl_with_native_types(env):
    p = env.write_jsonl("a.jsonl", [ev(), ev(status="shipped", ts="2026-01-02T00:00:00Z")])
    s = run_pipeline(p, env.cfg)
    assert s["status"] == "success" and s["rows_new_events"] == 2 and s["source_format"] == "jsonl"


def test_chunking_bounds_rows_per_chunk(env):
    p = env.write_csv("a.csv", [ev(line=i + 1) for i in range(10)])
    sizes = [len(c) for c in iter_chunks(p, 4, [])]
    assert sizes == [4, 4, 2] and max(sizes) <= 4
    s = run_pipeline(p, env.cfg)  # chunk_size=3 -> 4 chunks
    assert s["chunks_committed"] == 4 and s["rows_in"] == 10


# ---------------------------------------------------------------- empty / malformed / nulls
def test_empty_inputs_are_successful_noops(env):
    zero = env.tmp / "zero.csv"; zero.write_text("")
    header_only = env.write_csv("h.csv", [])
    jzero = env.tmp / "z.jsonl"; jzero.write_text("")
    for path in (str(zero), header_only, str(jzero)):
        s = run_pipeline(path, env.cfg, force=True)
        assert s["status"] == "success" and s["rows_in"] == 0
    assert env.q("SELECT COUNT(*) FROM order_line_events")[0][0] == 0


def test_malformed_csv_row_kept_with_correct_line_number(env):
    p = env.write_csv("m.csv", [ev(), ev(line=2)], raw_lines=["O3,1,C1,P1,2,10.00,created,2026-01-01T10:00:00Z,EXTRA", "O4,1,C1,P1,2,10.00,created,2026-01-01T10:00:00Z"])
    s = run_pipeline(p, env.cfg)
    assert s["status"] == "success" and s["rows_in"] == 4 and s["rows_rejected"] == 1
    assert reasons(env) == {4: "MALFORMED_RECORD"}  # header=1, rows 2-3, bad line=4
    raw = env.q("SELECT raw_record FROM rejects")[0][0]
    assert raw.startswith("O3,1,C1,P1")


def test_malformed_json_lines(env):
    p = env.write_jsonl("m.jsonl", [ev()], raw_lines=['{"order_id": "O9", "line_no": ', "[1,2]", ""])
    s = run_pipeline(p, env.cfg)
    assert s["rows_in"] == 4 and s["rows_rejected"] == 3
    assert reasons(env) == {2: "MALFORMED_RECORD", 3: "MALFORMED_RECORD", 4: "MALFORMED_RECORD"}


def test_missing_required_fields_and_nulls(env):
    rows = [ev(cust=""), ev(order="O2", qty=""), ev(order="O3", ts="   "), ev(order="O4")]
    p = env.write_csv("n.csv", rows)
    s = run_pipeline(p, env.cfg)
    assert s["rows_rejected"] == 3 and s["rows_new_events"] == 1
    assert set(reasons(env).values()) == {"MISSING_REQUIRED_FIELD"}
    assert sorted(r[0] for r in env.q("SELECT reason_detail FROM rejects")) == ["customer_id", "event_ts", "quantity"]


def test_json_null_is_missing(env):
    import json
    line = json.dumps({"order_id": "O1", "line_no": 1, "customer_id": None, "product_id": "P1",
                       "quantity": 2, "unit_price": 10.0, "status": "created", "event_ts": "2026-01-01T00:00:00Z"})
    p = env.tmp / "x.jsonl"; p.write_text(line + "\n")
    run_pipeline(str(p), env.cfg)
    assert env.q("SELECT reason_code, reason_detail FROM rejects") == [("MISSING_REQUIRED_FIELD", "customer_id")]


# ---------------------------------------------------------------- duplicates & idempotency
def test_duplicates_within_and_across_chunks(env):
    e1 = ev()
    p = env.write_csv("d.csv", [e1, e1, ev(status="paid", ts="2026-01-01T11:00:00Z"), e1, e1])  # chunk_size=3
    s = run_pipeline(p, env.cfg)
    assert (s["rows_in"], s["rows_duplicate"], s["rows_new_events"]) == (5, 3, 2)
    assert env.q("SELECT COUNT(*) FROM order_line_events")[0][0] == 2


def test_same_timestamp_instant_in_different_offsets_is_a_duplicate(env):
    p = env.write_csv("tz.csv", [ev(ts="2026-01-01T10:00:00Z"), ev(ts="2026-01-01T15:30:00+05:30")])
    s = run_pipeline(p, env.cfg)
    assert s["rows_new_events"] == 1 and s["rows_duplicate"] == 1


def test_rerun_same_input_is_idempotent(env):
    p = env.write_csv("r.csv", [ev(), ev(status="paid", ts="2026-01-01T11:00:00Z"), ev(cust="C9")])
    s1 = run_pipeline(p, env.cfg)
    before = (state(env), env.q("SELECT event_id, run_id FROM order_line_events ORDER BY 1"), env.q("SELECT COUNT(*) FROM rejects"))
    s2 = run_pipeline(p, env.cfg)  # default: skipped
    assert s2["status"] == "skipped" and s2["rows_in"] == 0
    s3 = run_pipeline(p, env.cfg, force=True)  # forced reprocess
    assert s3["status"] == "success" and s3["rows_new_events"] == 0 and s3["rows_duplicate"] == 2
    after = (state(env), env.q("SELECT event_id, run_id FROM order_line_events ORDER BY 1"), env.q("SELECT COUNT(*) FROM rejects"))
    assert before == after  # nothing duplicated, original provenance kept, rejects not duplicated
    assert s1["rows_new_events"] == 2


# ---------------------------------------------------------------- schema change
def test_missing_column_fails_fast_and_loads_nothing(env):
    p = env.write_csv("s.csv", [[r for i, r in enumerate(ev()) if i != 4]], header=[c for c in HEADER if c != "quantity"])
    s = run_pipeline(p, env.cfg)
    assert s["status"] == "failed" and s["failure_reason"].startswith("SchemaError: missing required columns")
    assert env.q("SELECT COUNT(*) FROM order_line_events")[0][0] == 0
    row = env.q("SELECT status, failure_reason FROM run_log WHERE run_id=?", s["run_id"])[0]
    assert row[0] == "failed" and "quantity" in row[1]


def test_extra_column_is_ignored_with_note(env):
    p = env.write_csv("x.csv", [ev() + ["whatever"]], header=HEADER + ["channel"])
    s = run_pipeline(p, env.cfg)
    assert s["status"] == "success" and s["rows_new_events"] == 1
    assert any("extra columns" in n for n in s["notes"])


def test_renamed_column_and_json_array_and_bad_extension_fail(env):
    p = env.write_csv("rn.csv", [ev()], header=["order_id", "line_no", "customer_id", "product_id", "qty", "unit_price", "status", "event_ts"])
    assert run_pipeline(p, env.cfg)["status"] == "failed"
    arr = env.tmp / "a.json"; arr.write_text('[{"order_id": "O1"}]')
    s = run_pipeline(str(arr), env.cfg)
    assert s["status"] == "failed" and "JSON array" in s["failure_reason"]
    txt = env.tmp / "a.txt"; txt.write_text("x")
    assert run_pipeline(str(txt), env.cfg)["status"] == "failed"
    assert run_pipeline(str(env.tmp / "missing.csv"), env.cfg)["status"] == "failed"


def test_empty_reference_tables_fail_clearly(tmp_path):
    cfg = Config(db_path=str(tmp_path / "e.db"))
    p = tmp_path / "o.csv"; p.write_text(",".join(HEADER) + "\n")
    s = run_pipeline(str(p), cfg)
    assert s["status"] == "failed" and "reference tables are empty" in s["failure_reason"]


# ---------------------------------------------------------------- referential integrity
def test_invalid_reference_keys_rejected_not_loaded(env):
    p = env.write_csv("ri.csv", [ev(cust="C404"), ev(order="O2", prod="P404"), ev(order="O3", cust="C2", prod="P2")])
    s = run_pipeline(p, env.cfg)
    assert s["rows_rejected"] == 2 and s["rows_new_events"] == 1
    assert sorted(reasons(env).values()) == ["INVALID_REFERENCE_CUSTOMER", "INVALID_REFERENCE_PRODUCT"]
    assert all(c.passed for c in rec.reconcile(sqlite3.connect(env.cfg.db_path)))


# ---------------------------------------------------------------- domain edge cases
def test_late_arriving_event_never_overwrites_newer_state(env):
    newer = ev(status="delivered", ts="2026-01-05T00:00:00Z")
    older = ev(status="paid", ts="2026-01-02T00:00:00Z")
    run_pipeline(env.write_csv("1.csv", [newer]), env.cfg)
    s = run_pipeline(env.write_csv("2.csv", [older]), env.cfg)  # arrives a file later
    assert s["rows_new_events"] == 1 and s["rows_state_changes"] == 0
    assert state(env)[0][2] == "delivered"
    assert env.q("SELECT COUNT(*) FROM order_line_events")[0][0] == 2  # history keeps both


def test_out_of_order_and_tie_break_are_deterministic(env, tmp_path):
    a = ev(status="paid", ts="2026-01-02T00:00:00Z")
    b = ev(status="shipped", ts="2026-01-02T00:00:00Z")  # same instant, different status
    c = ev(status="created", ts="2026-01-01T00:00:00Z")
    out = []
    for i, order in enumerate(([a, b, c], [c, b, a], [b, c, a])):
        cfg = Config(db_path=str(tmp_path / f"o{i}.db"), chunk_size=2)
        conn = db.connect(cfg.db_path)
        db.load_reference(conn, str(env.tmp / "ref" / "customers.csv"), str(env.tmp / "ref" / "products.csv")); conn.close()
        run_pipeline(env.write_csv(f"ord{i}.csv", order), cfg)
        out.append(sqlite3.connect(cfg.db_path).execute("SELECT status, last_event_id FROM order_lines").fetchall())
    assert out[0] == out[1] == out[2]


def test_boundary_values_and_derived_total(env):
    rows = [ev(order="O1", qty="1"), ev(order="O2", qty="1000", price="0.00"), ev(order="O3", qty="0"),
            ev(order="O4", qty="1001"), ev(order="O5", price="-0.01"), ev(order="O6", qty="2.5"),
            ev(order="O7", price="19.99", qty="3"), ev(order="O8", ts="2026-13-45T00:00:00Z"), ev(order="O9", line=0),
            ev(order="O10", status="TELEPORTED")]
    run_pipeline(env.write_csv("b.csv", rows), env.cfg)
    assert dict(env.q("SELECT order_id, line_total_cents FROM order_lines")) == {"O1": 1000, "O2": 0, "O7": 5997}
    assert reasons(env) == {4: "OUT_OF_RANGE", 5: "OUT_OF_RANGE", 6: "OUT_OF_RANGE", 7: "INVALID_FORMAT",
                            9: "INVALID_FORMAT", 10: "OUT_OF_RANGE", 11: "INVALID_STATUS"}


def test_status_is_case_and_whitespace_normalised(env):
    run_pipeline(env.write_csv("c.csv", [ev(status=" Paid ", cust=" C1 ")]), env.cfg)
    assert state(env)[0][2] == "paid"


# ---------------------------------------------------------------- partial failure and retry
def test_partial_failure_rolls_back_chunk_then_retry_recovers(env, tmp_path):
    rows = [ev(line=i + 1) for i in range(8)] + [ev(line=1, status="paid", ts="2026-01-02T00:00:00Z")]
    p = env.write_csv("pf.csv", rows)  # chunk_size=3 -> 3 chunks

    failed = run_pipeline(p, env.cfg, fail_at_chunk=2)
    assert failed["status"] == "failed" and "InjectedFailure" in failed["failure_reason"]
    assert failed["chunks_committed"] == 1 and failed["rows_in"] == 3  # chunk 2 rolled back
    assert env.q("SELECT COUNT(*) FROM order_line_events")[0][0] == 3
    log = env.q("SELECT status, rows_in, rows_new_events FROM run_log WHERE run_id=?", failed["run_id"])[0]
    assert log == ("failed", 3, 3)  # run_log counters match committed data exactly

    retry = run_pipeline(p, env.cfg)  # failed runs never block a retry
    assert retry["status"] == "success" and retry["rows_duplicate"] == 3 and retry["rows_new_events"] == 6

    # end state identical to a clean single run
    clean = Config(db_path=str(tmp_path / "clean.db"), chunk_size=3)
    conn = db.connect(clean.db_path)
    db.load_reference(conn, str(env.tmp / "ref" / "customers.csv"), str(env.tmp / "ref" / "products.csv")); conn.close()
    run_pipeline(p, clean)
    q = "SELECT event_id, order_id, line_no, status, event_ts FROM order_line_events ORDER BY 1"
    assert env.q(q) == sqlite3.connect(clean.db_path).execute(q).fetchall()
    assert all(c.passed for c in rec.reconcile(sqlite3.connect(env.cfg.db_path)))


# ---------------------------------------------------------------- generator + end-to-end reconciliation
@pytest.mark.parametrize("fmt", ["csv", "jsonl"])
def test_end_to_end_against_generator_ground_truth(tmp_path, fmt):
    d = str(tmp_path / "gen")
    g = generate(d, n_rows=3000, seed=11, fmt=fmt)
    cfg = Config(db_path=str(tmp_path / "e2e.db"), chunk_size=700)
    conn = db.connect(cfg.db_path)
    db.load_reference(conn, os.path.join(d, "reference", "customers.csv"), os.path.join(d, "reference", "products.csv")); conn.close()
    s = run_pipeline(g["paths"]["input"], cfg)
    assert s["status"] == "success" and s["rows_in"] == 3000
    conn = sqlite3.connect(cfg.db_path)
    checks = rec.reconcile(conn) + rec.check_against_expected(
        conn, s["run_id"], os.path.join(d, "expected", "expected_summary.json"), os.path.join(d, "expected", "reject_labels.csv"))
    failed = [c for c in checks if not c.passed]
    assert not failed, failed
    assert set(g["expected_rejects_by_reason"]) >= {"MALFORMED_RECORD", "INVALID_FORMAT"}  # every defect type present


def test_generator_is_deterministic_and_labels_are_separate(tmp_path):
    a, b = generate(str(tmp_path / "a"), 500, seed=5), generate(str(tmp_path / "b"), 500, seed=5)
    assert open(a["paths"]["input"]).read() == open(b["paths"]["input"]).read()
    assert "expected" not in open(a["paths"]["input"]).read().lower().split("\n")[0]
    assert open(a["paths"]["input"]).read() != open(generate(str(tmp_path / "c"), 500, seed=6)["paths"]["input"]).read()
    assert not os.path.commonpath([a["paths"]["input"], os.path.join(a["paths"]["expected"], "x")]) == a["paths"]["input"]
