"""Command line: generate | run | reconcile | bench."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys

from . import db, reconcile as rec
from .config import Config
from .generator import generate
from .run import run_pipeline


def _cfg(a) -> Config:
    return Config(db_path=a.db, chunk_size=getattr(a, "chunk_size", 10_000))


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="pipeline")
    p.add_argument("--log-level", default="INFO")
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="create seeded synthetic data")
    g.add_argument("--out", required=True); g.add_argument("--rows", type=int, default=200)
    g.add_argument("--seed", type=int, default=42); g.add_argument("--format", choices=["csv", "jsonl"], default="csv")

    r = sub.add_parser("run", help="load reference data (optional) and one input file")
    r.add_argument("--input", required=True); r.add_argument("--reference-dir")
    r.add_argument("--db", default="output/warehouse.db"); r.add_argument("--chunk-size", type=int, default=10_000)
    r.add_argument("--force", action="store_true", help="reprocess a file whose sha256 was already loaded")

    c = sub.add_parser("reconcile", help="run reconciliation checks and write the report")
    c.add_argument("--db", default="output/warehouse.db"); c.add_argument("--report-dir", default="output")
    c.add_argument("--run-id"); c.add_argument("--expected-dir", help="generator 'expected' folder (fresh-DB runs)")

    b = sub.add_parser("bench", help="performance + memory measurements")
    b.add_argument("--workdir", default="output/bench"); b.add_argument("--rows", type=int, nargs="+", default=[50_000, 200_000])
    b.add_argument("--chunk-sizes", type=int, nargs="+", default=[1_000, 5_000, 20_000, 100_000])
    b.add_argument("--format", choices=["csv", "jsonl"], default="csv")

    a = p.parse_args(argv)
    logging.basicConfig(level=a.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if a.cmd == "generate":
        s = generate(a.out, a.rows, a.seed, a.format)
        print(json.dumps({k: v for k, v in s.items()}, indent=2)); return 0

    if a.cmd == "run":
        cfg = _cfg(a)
        if a.reference_dir:
            conn = db.connect(cfg.db_path)
            print(json.dumps(db.load_reference(conn, os.path.join(a.reference_dir, "customers.csv"),
                                               os.path.join(a.reference_dir, "products.csv"))))
            conn.close()
        s = run_pipeline(a.input, cfg, force=a.force)
        print(json.dumps(s, indent=2)); return 0 if s["status"] != "failed" else 1

    if a.cmd == "reconcile":
        conn = sqlite3.connect(a.db)
        conn.execute("PRAGMA foreign_keys = ON")
        checks = rec.reconcile(conn)
        if a.expected_dir:
            rid = a.run_id or conn.execute("SELECT run_id FROM run_log WHERE status='success' ORDER BY started_at DESC LIMIT 1").fetchone()[0]
            checks += rec.check_against_expected(conn, rid, os.path.join(a.expected_dir, "expected_summary.json"),
                                                 os.path.join(a.expected_dir, "reject_labels.csv"))
        os.makedirs(a.report_dir, exist_ok=True)
        ok = rec.write_report(conn, checks, os.path.join(a.report_dir, "reconciliation_report.md"),
                              os.path.join(a.report_dir, "reconciliation_report.json"))
        for ch in checks:
            print(f"[{'PASS' if ch.passed else 'FAIL'}] {ch.name} -- {ch.detail}")
        return 0 if ok else 1

    if a.cmd == "bench":
        from .bench import run_bench
        run_bench(a.workdir, a.rows, a.chunk_sizes, a.format); return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
