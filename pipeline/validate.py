"""Vectorised validation + deterministic transformation of one chunk.

Each rejected row gets exactly one reason code (first failing rule wins, in the
order below). Valid rows are normalised: trimmed, status lower-cased,
timestamps converted to canonical UTC, money converted to integer cents and a
deterministic ``event_id`` derived from the business key.
"""
from __future__ import annotations

import hashlib
import json

import numpy as np
import pandas as pd

from .config import (MALFORMED_SENTINEL, REQUIRED_COLUMNS, TS_FORMAT, VALID_STATUSES, Config)

VALID_COLUMNS = [
    "event_id", "order_id", "line_no", "customer_id", "product_id", "quantity",
    "unit_price_cents", "line_total_cents", "status", "event_ts", "_line",
]


def make_event_ids(keys) -> list[str]:
    """sha1(order_id|line_no|event_ts_utc|status)[:16] - stable across runs/machines."""
    return [hashlib.sha1(k.encode("utf-8")).hexdigest()[:16] for k in keys]


def _raw_record(rec: dict) -> str:
    oid = rec.get("order_id", "")
    if oid.startswith(MALFORMED_SENTINEL):
        return oid[len(MALFORMED_SENTINEL):]
    return json.dumps({k: rec[k] for k in REQUIRED_COLUMNS}, ensure_ascii=False)


def validate_chunk(df: pd.DataFrame, customer_ids: set, product_ids: set, cfg: Config):
    """Return ``(valid_df, rejects_df)``. ``valid_df`` has VALID_COLUMNS."""
    raw = df.reset_index(drop=True)
    df = raw.copy()
    for c in ["order_id", "line_no", "customer_id", "product_id", "quantity", "unit_price", "event_ts"]:
        df[c] = df[c].str.strip()
    df["status"] = df["status"].str.strip().str.lower()

    reason = pd.Series("", index=df.index, dtype=object)
    detail = pd.Series("", index=df.index, dtype=object)

    def flag(mask, code: str, why: str = "") -> None:
        m = mask & (reason == "")
        reason[m] = code
        detail[m] = why

    flag(df["order_id"].str.startswith(MALFORMED_SENTINEL), "MALFORMED_RECORD", "unparseable line / wrong field count")
    for col in REQUIRED_COLUMNS:
        flag(df[col] == "", "MISSING_REQUIRED_FIELD", col)

    line_no = pd.to_numeric(df["line_no"], errors="coerce")
    qty = pd.to_numeric(df["quantity"], errors="coerce")
    price = pd.to_numeric(df["unit_price"], errors="coerce")
    ts = pd.to_datetime(df["event_ts"], errors="coerce", utc=True, format="ISO8601")

    def is_int(s):
        return np.isfinite(s) & (s == np.floor(s))

    flag(~is_int(line_no), "INVALID_FORMAT", "line_no")
    flag(~is_int(qty), "INVALID_FORMAT", "quantity")
    flag(~np.isfinite(price), "INVALID_FORMAT", "unit_price")
    flag(ts.isna(), "INVALID_FORMAT", "event_ts")

    flag(line_no < 1, "OUT_OF_RANGE", "line_no")
    flag((qty < 1) | (qty > cfg.max_quantity), "OUT_OF_RANGE", "quantity")
    flag((price < 0) | (price > cfg.max_unit_price), "OUT_OF_RANGE", "unit_price")
    flag(~df["status"].isin(VALID_STATUSES), "INVALID_STATUS", "status")
    flag(~df["customer_id"].isin(customer_ids), "INVALID_REFERENCE_CUSTOMER", "customer_id")
    flag(~df["product_id"].isin(product_ids), "INVALID_REFERENCE_PRODUCT", "product_id")

    bad = reason != ""

    rej = pd.DataFrame({
        "source_line": raw.loc[bad, "_line"].astype("int64").to_numpy(),
        "reason_code": reason[bad].to_numpy(),
        "reason_detail": detail[bad].to_numpy(),
        "raw_record": [_raw_record(r) for r in raw.loc[bad, REQUIRED_COLUMNS].to_dict("records")],
    })

    ok = ~bad
    good = df.loc[ok].copy()
    good["line_no"] = line_no[ok].astype("int64")
    good["quantity"] = qty[ok].astype("int64")
    good["unit_price_cents"] = np.rint(price[ok] * 100).astype("int64")
    good["line_total_cents"] = good["quantity"] * good["unit_price_cents"]
    good["event_ts"] = ts[ok].dt.strftime(TS_FORMAT)
    keys = good["order_id"] + "|" + good["line_no"].astype(str) + "|" + good["event_ts"] + "|" + good["status"]
    good["event_id"] = make_event_ids(keys)
    return good[VALID_COLUMNS], rej
