"""Chunked readers for CSV and JSON Lines.

Both readers yield pandas DataFrames of at most ``chunk_size`` rows, with every
column as ``str`` plus a ``_line`` column (1-based physical line number in the
source file, header = line 1 for CSV). Unparseable lines are *kept* as sentinel
rows so that nothing is silently dropped and line numbers stay correct.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from itertools import islice
from typing import Iterator

import pandas as pd

from .config import MALFORMED_SENTINEL, REQUIRED_COLUMNS

log = logging.getLogger(__name__)


class SchemaError(Exception):
    """The file's structure is unusable (missing columns, JSON array, bad extension)."""


def detect_format(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return "csv"
    if ext in (".jsonl", ".ndjson", ".json"):
        return "jsonl"
    raise SchemaError(f"unsupported file extension {ext!r} (use .csv or .jsonl)")


def sha256_file(path: str, block: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(block):
            h.update(chunk)
    return h.hexdigest()


def _check_columns(cols, notes: list[str]) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in cols]
    if missing:
        raise SchemaError(f"missing required columns: {missing}")
    extra = [c for c in cols if c not in REQUIRED_COLUMNS]
    if extra:
        msg = f"unexpected extra columns ignored: {extra}"
        log.warning(msg)
        notes.append(msg)


def _read_csv(path: str, chunk_size: int, notes: list[str]) -> Iterator[pd.DataFrame]:
    if os.path.getsize(path) == 0:
        notes.append("empty file (0 bytes)")
        return
    header = [c.strip() for c in pd.read_csv(path, nrows=0, dtype=str, encoding="utf-8-sig").columns]
    _check_columns(header, notes)
    width, oid_pos = len(header), header.index("order_id")

    def on_bad_line(fields: list[str]) -> list[str]:
        # Too many fields. Keep the row (as a sentinel) instead of dropping it.
        row = [""] * width
        row[oid_pos] = MALFORMED_SENTINEL + ",".join(fields)
        return row

    reader = pd.read_csv(
        path, dtype=str, keep_default_na=False, na_filter=False,
        chunksize=chunk_size, engine="python", on_bad_lines=on_bad_line,
        skip_blank_lines=False, encoding="utf-8-sig",
    )
    offset = 0
    for chunk in reader:
        chunk.columns = [c.strip() for c in chunk.columns]
        chunk = chunk.fillna("")
        chunk["_line"] = range(offset + 2, offset + 2 + len(chunk))
        offset += len(chunk)
        yield chunk[REQUIRED_COLUMNS + ["_line"]]


def _to_str(v) -> str:
    if v is None or (isinstance(v, float) and v != v):
        return ""
    return v if isinstance(v, str) else str(v)


def _parse_json_line(raw: str):
    try:
        obj = json.loads(raw)
    except ValueError:
        return None
    return obj if isinstance(obj, dict) else None


def _read_jsonl(path: str, chunk_size: int, notes: list[str]) -> Iterator[pd.DataFrame]:
    with open(path, "r", encoding="utf-8-sig") as f:
        first = ""
        for ch in iter(lambda: f.read(1), ""):
            if not ch.isspace():
                first = ch
                break
        if first == "":
            notes.append("empty file")
            return
        if first == "[":
            raise SchemaError("top-level JSON array cannot be streamed; provide JSON Lines (one object per line)")
        f.seek(0)

        line_no, checked = 0, False
        while True:
            lines = list(islice(f, chunk_size))
            if not lines:
                break
            recs, keys = [], set()
            for raw in lines:
                line_no += 1
                obj = _parse_json_line(raw)
                if obj is None:
                    obj = {"order_id": MALFORMED_SENTINEL + raw.rstrip("\r\n")}
                else:
                    keys.update(obj.keys())
                obj["_line"] = line_no
                recs.append(obj)
            if keys and not checked:
                _check_columns(keys, notes)
                checked = True
            df = pd.DataFrame(recs, dtype=object)
            for c in REQUIRED_COLUMNS:
                if c not in df.columns:
                    df[c] = ""
                df[c] = df[c].map(_to_str)
            yield df[REQUIRED_COLUMNS + ["_line"]]


def iter_chunks(path: str, chunk_size: int, notes: list[str]) -> Iterator[pd.DataFrame]:
    fmt = detect_format(path)
    return _read_csv(path, chunk_size, notes) if fmt == "csv" else _read_jsonl(path, chunk_size, notes)
