"""Simple performance / bounded-memory measurement.

Time and memory are measured in separate runs because tracemalloc slows Python down.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tracemalloc

from . import db
from .config import Config
from .generator import generate
from .run import run_pipeline


def _fresh_run(src, ref_dir, db_path, chunk_size):
    for ext in ("", "-wal", "-shm"):
        if os.path.exists(db_path + ext):
            os.remove(db_path + ext)
    conn = db.connect(db_path)
    db.load_reference(conn, os.path.join(ref_dir, "customers.csv"), os.path.join(ref_dir, "products.csv"))
    conn.close()
    return run_pipeline(src, Config(db_path=db_path, chunk_size=chunk_size))


def run_bench(workdir: str, rows_list, chunk_sizes, fmt: str = "csv") -> list[dict]:
    logging.getLogger("pipeline").setLevel(logging.WARNING)
    os.makedirs(workdir, exist_ok=True)
    results = []
    datasets = {}
    for n in sorted(set(rows_list) | {sorted(rows_list)[0]}):
        d = os.path.join(workdir, f"data_{n}")
        s = generate(d, n, seed=42, fmt=fmt)
        datasets[n] = (s["paths"]["input"], s["paths"]["reference"], os.path.getsize(s["paths"]["input"]))

    def measure(n, chunk):
        src, ref, size = datasets[n]
        dbp = os.path.join(workdir, "bench.db")
        t = _fresh_run(src, ref, dbp, chunk)
        tracemalloc.start()
        _fresh_run(src, ref, dbp, chunk)
        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
        row = {"rows": n, "file_mb": round(size / 1e6, 1), "chunk_size": chunk, "status": t["status"],
               "elapsed_s": t["elapsed_s"], "rows_per_s": t["rows_per_s"],
               "peak_python_mb": round(peak / 1e6, 1), **{f"{k}": v for k, v in t["timings_s"].items()}}
        results.append(row)
        print(json.dumps(row))

    print("# 1) memory vs input size (fixed chunk_size=10000): peak should stay ~flat")
    for n in sorted(rows_list):
        measure(n, 10_000)
    print("# 2) chunk-size sweep on the smallest dataset: throughput vs memory trade-off")
    base = sorted(rows_list)[0]
    for c in chunk_sizes:
        measure(base, c)
    print("# 3) process peak RSS (fresh interpreter per run, chunk_size=10000); interpreter+pandas baseline is ~constant")
    child = ("import resource,sys,logging; logging.disable(logging.CRITICAL);"
             "from pipeline.config import Config; from pipeline.run import run_pipeline;"
             "run_pipeline(sys.argv[1], Config(db_path=sys.argv[2], chunk_size=10000));"
             "print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)")
    for n in sorted(rows_list):
        src, ref, size = datasets[n]
        dbp = os.path.join(workdir, "rss.db")
        for ext in ("", "-wal", "-shm"):
            if os.path.exists(dbp + ext):
                os.remove(dbp + ext)
        conn = db.connect(dbp)
        db.load_reference(conn, os.path.join(ref, "customers.csv"), os.path.join(ref, "products.csv"))
        conn.close()
        out = subprocess.run([sys.executable, "-c", child, src, dbp], capture_output=True, text=True, check=True)
        row = {"rows": n, "file_mb": round(size / 1e6, 1), "peak_rss_mb": round(int(out.stdout.split()[-1]) / 1024, 1)}
        results.append(row)
        print(json.dumps(row))
    with open(os.path.join(workdir, "benchmark_results.json"), "w") as f:
        json.dump(results, f, indent=2)
    return results
