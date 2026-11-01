import csv
import json
import os
import sqlite3

import pytest

from pipeline import db
from pipeline.config import REQUIRED_COLUMNS, Config

HEADER = REQUIRED_COLUMNS


def ev(order="O1", line=1, status="created", ts="2026-01-01T10:00:00Z", cust="C1", prod="P1", qty="2", price="10.00"):
    return [order, str(line), cust, prod, qty, price, status, ts]


@pytest.fixture
def env(tmp_path):
    """Fresh DB with 2 customers and 2 products + helpers to write inputs."""
    ref = tmp_path / "ref"
    ref.mkdir()
    (ref / "customers.csv").write_text("customer_id,customer_name,country,created_at\n"
                                       "C1,Ann,IN,2025-01-01T00:00:00Z\nC2,Bob,US,2025-02-01T00:00:00Z\n")
    (ref / "products.csv").write_text("product_id,product_name,category,list_price\n"
                                      "P1,Pen,stationery,10.00\nP2,Book,books,25.50\n")
    cfg = Config(db_path=str(tmp_path / "t.db"), chunk_size=3)
    conn = db.connect(cfg.db_path)
    db.load_reference(conn, str(ref / "customers.csv"), str(ref / "products.csv"))
    conn.close()

    class Env:
        pass

    e = Env()
    e.tmp, e.cfg = tmp_path, cfg

    def write_csv(name, rows, header=HEADER, raw_lines=()):
        p = tmp_path / name
        with open(p, "w", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(header)
            w.writerows(rows)
            for r in raw_lines:
                f.write(r + "\n")
        return str(p)

    def write_jsonl(name, rows, raw_lines=()):
        p = tmp_path / name
        with open(p, "w") as f:
            for r in rows:
                f.write(json.dumps(dict(zip(HEADER, r))) + "\n")
            for r in raw_lines:
                f.write(r + "\n")
        return str(p)

    def q(sql, *a):
        c = sqlite3.connect(cfg.db_path)
        try:
            return c.execute(sql, a).fetchall()
        finally:
            c.close()

    e.write_csv, e.write_jsonl, e.q = write_csv, write_jsonl, q
    return e
