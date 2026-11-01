"""Seeded synthetic data generator (streams to disk; memory ~ shuffle window).

Writes three separate folders so labels never leak into the input:
    <out>/reference/{customers,products}.csv
    <out>/input/orders.{csv|jsonl}
    <out>/expected/{expected_summary.json,reject_labels.csv}   <- ground truth only

The mix: normal lifecycle events, boundary values (qty 1 / 1000), exact duplicates,
out-of-order events (local shuffle), very late events (emitted at the end), mixed
UTC / +05:30 timestamps, and deliberately invalid rows (one defect each, labelled).
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import random
from collections import Counter, namedtuple
from datetime import datetime, timedelta, timezone

from .config import REQUIRED_COLUMNS, TS_FORMAT

Row = namedtuple("Row", "rec label meta")  # meta = (order_id, line_no, ts_utc, status, total_cents) for valid events
FLOW = ["created", "paid", "shipped", "delivered"]
COUNTRIES = ["IN", "US", "DE", "GB", "SG", "AU", "BR", "JP"]
CATEGORIES = ["electronics", "books", "home", "grocery", "toys", "fashion", "sports"]
DEFECTS = ["MISSING_REQUIRED_FIELD", "INVALID_FORMAT", "OUT_OF_RANGE", "INVALID_STATUS",
           "INVALID_REFERENCE_CUSTOMER", "INVALID_REFERENCE_PRODUCT", "MALFORMED_RECORD"]
BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)
IST = timezone(timedelta(hours=5, minutes=30))


def _event_id(order_id, line_no, ts_utc, status) -> str:
    return hashlib.sha1(f"{order_id}|{line_no}|{ts_utc}|{status}".encode()).hexdigest()[:16]


def _fmt_ts(rng, ts: datetime) -> str:
    if rng.random() < 0.10:  # same instant, expressed in IST
        return ts.astimezone(IST).strftime("%Y-%m-%dT%H:%M:%S.%f") + "+05:30"
    return ts.strftime(TS_FORMAT)


def _record(rng, order_id, line_no, cust, prod, qty, price_cents, status, ts):
    return {"order_id": order_id, "line_no": str(line_no), "customer_id": cust, "product_id": prod,
            "quantity": str(qty), "unit_price": f"{price_cents / 100:.2f}", "status": status,
            "event_ts": _fmt_ts(rng, ts)}


def _order_events(rng, idx, cust_ids, products):
    order_id = f"O{idx:08d}"
    cust = rng.choice(cust_ids)
    t0 = BASE + timedelta(seconds=rng.randint(0, 90 * 86400))
    events = []
    for line_no in range(1, rng.randint(1, 4) + 1):
        pid, list_cents = rng.choice(products)
        price = list_cents if rng.random() < 0.8 else max(0, round(list_cents * 0.9))
        r = rng.random()
        qty = 1000 if r < 0.01 else 1 if r < 0.06 else rng.randint(2, 10)
        statuses = FLOW[:rng.choice([1, 2, 3, 4, 4, 4])]
        if len(statuses) <= 2 and rng.random() < 0.07:
            statuses.append("cancelled")
        elif len(statuses) == 4 and rng.random() < 0.05:
            statuses.append("returned")
        t = t0
        for st in statuses:
            t = t + timedelta(seconds=rng.randint(60, 2 * 86400), microseconds=rng.randint(0, 999_999))
            rec = _record(rng, order_id, line_no, cust, pid, qty, price, st, t)
            events.append(Row(rec, None, (order_id, line_no, t.strftime(TS_FORMAT), st, qty * price)))
    return events


def _bad_row(rng, defect, seq, fmt, cust_ids, products):
    cust, (pid, cents) = rng.choice(cust_ids), rng.choice(products)
    ts = BASE + timedelta(seconds=rng.randint(0, 90 * 86400))
    rec = _record(rng, f"B{seq:08d}", 1, cust, pid, rng.randint(1, 5), cents, "created", ts)
    if defect == "MISSING_REQUIRED_FIELD":
        rec[rng.choice(REQUIRED_COLUMNS)] = ""
    elif defect == "INVALID_FORMAT":
        k, v = rng.choice([("quantity", "abc"), ("event_ts", "2026-13-45T00:00:00Z"),
                           ("unit_price", "12,5x"), ("line_no", "1.5")])
        rec[k] = v
    elif defect == "OUT_OF_RANGE":
        k, v = rng.choice([("quantity", "0"), ("quantity", "-3"), ("quantity", "1001"),
                           ("unit_price", "-5.00"), ("line_no", "0")])
        rec[k] = v
    elif defect == "INVALID_STATUS":
        rec["status"] = "teleported"
    elif defect == "INVALID_REFERENCE_CUSTOMER":
        rec["customer_id"] = "C9999999"
    elif defect == "INVALID_REFERENCE_PRODUCT":
        rec["product_id"] = "P99999X"
    elif defect == "MALFORMED_RECORD":
        if fmt == "csv":
            rec = {"__raw__": ",".join(rec[c] for c in REQUIRED_COLUMNS) + ",unexpected_extra_field"}
        else:
            rec = {"__raw__": '{"order_id": "B%08d", "line_no": 1, "customer_' % seq}
    return Row(rec, defect, None)


def _row_stream(rng, n_rows, fmt, cust_ids, products, bad_rate, dup_rate, late_rate):
    count, late, order_idx, bad_seq = 0, [], 0, 0
    guaranteed = list(DEFECTS)  # make sure every defect type appears even in tiny samples
    while count < n_rows:
        order_idx += 1
        for ev in _order_events(rng, order_idx, cust_ids, products):
            batch = [("late" if (rng.random() < late_rate or count == 4) else "main", ev)]
            if rng.random() < dup_rate or count == 1:
                batch.append(("main", Row(dict(ev.rec), "DUPLICATE", ev.meta)))
            if (guaranteed and count >= 8 and count % 3 == 0) or rng.random() < bad_rate:
                defect = guaranteed.pop(0) if (guaranteed and count >= 8 and count % 3 == 0) else rng.choice(DEFECTS)
                bad_seq += 1
                batch.append(("main", _bad_row(rng, defect, bad_seq, fmt, cust_ids, products)))
            for dest, row in batch:
                if count >= n_rows:
                    break
                count += 1
                if dest == "late":
                    late.append(row)
                else:
                    yield row
    yield from late


def _jsonify(k, v):
    if v == "":
        return None
    if k in ("line_no", "quantity"):
        try:
            return int(v)
        except ValueError:
            try:
                return float(v)
            except ValueError:
                return v
    if k == "unit_price":
        try:
            return float(v)
        except ValueError:
            return v
    return v


def generate(out_dir: str, n_rows: int = 200, seed: int = 42, fmt: str = "csv", bad_rate: float = 0.03,
             dup_rate: float = 0.02, late_rate: float = 0.01, shuffle_window: int = 500) -> dict:
    assert fmt in ("csv", "jsonl")
    rng = random.Random(seed)
    for sub in ("reference", "input", "expected"):
        os.makedirs(os.path.join(out_dir, sub), exist_ok=True)

    n_cust, n_prod = max(20, n_rows // 50), max(10, min(n_rows // 200, 5000))
    cust_ids = [f"C{i:06d}" for i in range(1, n_cust + 1)]
    products = [(f"P{i:05d}", rng.randint(199, 49_999)) for i in range(1, n_prod + 1)]
    with open(os.path.join(out_dir, "reference", "customers.csv"), "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["customer_id", "customer_name", "country", "created_at"])
        for c in cust_ids:
            w.writerow([c, f"customer_{c[1:]}", rng.choice(COUNTRIES),
                        (BASE - timedelta(days=rng.randint(1, 900))).strftime("%Y-%m-%dT%H:%M:%SZ")])
    with open(os.path.join(out_dir, "reference", "products.csv"), "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["product_id", "product_name", "category", "list_price"])
        for pid, cents in products:
            w.writerow([pid, f"product_{pid[1:]}", rng.choice(CATEGORIES), f"{cents / 100:.2f}"])

    in_path = os.path.join(out_dir, "input", f"orders.{fmt}")
    reject_labels, seen, state = [], set(), {}
    expected_rejects, dups, new_events, line = Counter(), 0, 0, 0
    first_line = 2 if fmt == "csv" else 1

    def write_row(fh, writer, row: Row):
        nonlocal line, dups, new_events
        line = line + 1
        phys = first_line + line - 1
        if "__raw__" in row.rec:
            fh.write(row.rec["__raw__"] + "\n")
        elif fmt == "csv":
            writer.writerow([row.rec[c] for c in REQUIRED_COLUMNS])
        else:
            fh.write(json.dumps({k: _jsonify(k, row.rec[k]) for k in REQUIRED_COLUMNS}) + "\n")
        if row.label not in (None, "DUPLICATE"):
            expected_rejects[row.label] += 1
            reject_labels.append((phys, row.label))
            return
        order_id, line_no, ts, status, total = row.meta
        eid = _event_id(order_id, line_no, ts, status)
        if eid in seen:
            dups += 1
            return
        seen.add(eid)
        new_events += 1
        cur = state.get((order_id, line_no))
        if cur is None or (ts, eid) > (cur[0], cur[1]):
            state[(order_id, line_no)] = (ts, eid, status, total)

    with open(in_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n") if fmt == "csv" else None
        if writer:
            writer.writerow(REQUIRED_COLUMNS)
        buf = []
        for row in _row_stream(rng, n_rows, fmt, cust_ids, products, bad_rate, dup_rate, late_rate):
            buf.append(row)
            if len(buf) >= shuffle_window:  # local shuffle => out-of-order arrival
                rng.shuffle(buf)
                for r in buf:
                    write_row(fh, writer, r)
                buf = []
        rng.shuffle(buf)
        for r in buf:
            write_row(fh, writer, r)

    by_status = Counter(v[2] for v in state.values())
    sums = Counter()
    for v in state.values():
        sums[v[2]] += v[3]
    summary = {
        "seed": seed, "format": fmt, "total_rows": line,
        "expected_rejects_by_reason": dict(expected_rejects),
        "expected_rejected_total": sum(expected_rejects.values()),
        "expected_duplicates": dups, "expected_new_events": new_events,
        "expected_current_lines": len(state),
        "expected_current_rows_by_status": dict(by_status),
        "expected_current_sum_cents_by_status": dict(sums),
    }
    with open(os.path.join(out_dir, "expected", "expected_summary.json"), "w") as f:
        json.dump(summary, f, indent=2, sort_keys=True)
    with open(os.path.join(out_dir, "expected", "reject_labels.csv"), "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["source_line", "expected_reason_code"])
        w.writerows(sorted(reject_labels))
    summary["paths"] = {"input": in_path, "reference": os.path.join(out_dir, "reference"),
                        "expected": os.path.join(out_dir, "expected")}
    return summary
