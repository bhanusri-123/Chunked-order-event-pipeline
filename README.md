# Chunked Order-Event Pipeline

A small, well-tested data pipeline that reads **large CSV or JSON Lines files in chunks** (so memory stays flat no matter how big the file is), checks every row, sets bad rows aside with the exact line number, and loads the good ones into a database **safely, so you can re-run it without creating duplicates**.

Built with Python, pandas and SQLite. No external services or credentials needed.

## What problem does it solve?

A source system sends a stream of **order-line events** (`created`, `paid`, `shipped`, ...). The files are big, and real feeds are messy: duplicate events, missing values, bad dates, unknown customers, events arriving late or out of order, and sometimes a changed file layout. This pipeline:

1. Reads the file `chunk_size` rows at a time, never the whole file.
2. Validates each row and quarantines bad ones with a reason and the source line number.
3. Drops duplicates and loads the rest in one transaction per chunk.
4. Keeps both a full **event history** and the **current state** of every order line.
5. Records every run (counts, timings, failure reason) and **reconciles** the result against expected numbers.

## How it works

```
 orders.csv / orders.jsonl                  customers.csv, products.csv
        |  read N rows at a time                       |
        v                                              v
   read chunk --> validate --+--> good rows --> dedupe --> BEGIN
                             |                              history table   (append-only, all events)
                             |                              current state   (latest event per order line)
                             +--> bad rows ---------------> rejects table  (reason + source line + raw text)
                                                            run_log counters
                                                          COMMIT   (one transaction per chunk)
```

## Quick start

You need Python 3.10 or newer (developed and tested on 3.12).

**Windows (PowerShell) / VS Code terminal**
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest -q
```

**macOS / Linux**
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest -q
```

You should see `24 passed`. Then run the pipeline on the 200-row sample (the same commands work on every OS):

```
python -m pipeline generate --out data/sample --rows 200 --seed 7 --format csv
python -m pipeline run --input data/sample/input/orders.csv --reference-dir data/sample/reference --db output/sample.db --chunk-size 50
python -m pipeline reconcile --db output/sample.db --report-dir output/sample_report --expected-dir data/sample/expected
```

If the `run` step says `"status": "skipped"`, that database already contains this exact file. Delete `output/sample.db` and run again, or add `--force`.

For a bigger run, generate more rows, for example `--rows 100000 --out data/large`, and point `run` and `reconcile` at that folder. JSON Lines works too: use `--format jsonl`.

## What you get

| Output | Where | What it is |
|---|---|---|
| Console summary | terminal | status, rows in, rejected, duplicates, new events, timings |
| Database | `output/sample.db` | SQLite file with the tables listed below |
| Validation report | `output/sample_report/reconciliation_report.md` | PASS/FAIL checks plus sample analytical queries |

Expected result for the sample: `rows_in 200`, `rows_rejected 10`, `rows_duplicate 7`, `rows_new_events 183`, and 15 of 15 reconciliation checks `[PASS]`.

To open the database in VS Code, install the **SQLite Viewer** extension. To export a table to CSV, change `order_lines` below to `rejects` or `order_line_events` for the others:

```
python -c "import sqlite3,pandas as pd; pd.read_sql('select * from order_lines', sqlite3.connect('output/sample.db')).to_csv('output/order_lines.csv', index=False)"
```

### Database tables

| Table | Purpose |
|---|---|
| `order_line_events` | Append-only history, one row per distinct event |
| `order_lines` | Current state: the latest event per order line |
| `rejects` | Quarantine: reason code, offending field, raw row, source file and line |
| `run_log` | One row per run: run id, file checksum, status, counts, failure reason |
| `customers`, `products` | Reference data used for key checks |

## How messy data is handled

| Situation | Behaviour |
|---|---|
| Malformed row (extra fields, broken JSON) | Quarantined as `MALFORMED_RECORD` with the exact line number |
| Missing required field | Quarantined as `MISSING_REQUIRED_FIELD`, with the field name |
| Bad number or date, out-of-range value, unknown status | Quarantined as `INVALID_FORMAT`, `OUT_OF_RANGE` or `INVALID_STATUS` |
| Unknown customer or product | Quarantined as `INVALID_REFERENCE_CUSTOMER` / `_PRODUCT` |
| Duplicate event | Counted as a duplicate, not inserted again |
| Late or out-of-order event | Stored in history; never overwrites a newer current state |
| Same file run twice | Skipped by checksum; with `--force`, adds no new rows |
| Crash halfway | The unfinished chunk rolls back; re-run the same command to finish |
| Changed file layout (missing or renamed column) | Run fails before loading anything and records the reason |
| Empty file | Succeeds with zero rows |

## Testing and correctness evidence

- **24 automated tests** cover the happy path, empty input, malformed rows, nulls, duplicates, schema changes, invalid keys, late and out-of-order events, boundary values, crash and retry, and reruns.
- **Ground-truth check:** the data generator writes the expected answers to a separate `expected/` folder. Reconciliation compares the loaded result with them: row counts, every reject's line and reason, duplicates, and revenue by status.
- **Internal checks:** per-run counts add up, history matches the run log, current state equals the latest event, referential integrity holds, and totals are consistent.

## Performance

Measured on one machine (see `output/benchmark_output.txt`):

| Input rows | File size | Rows per second | Peak Python memory | Process peak RSS |
|---|---|---|---|---|
| 50,000 | 3.6 MB | about 29,000 | 11.1 MB | 185 MB |
| 200,000 | 14.4 MB | about 26,000 | 11.7 MB | 185 MB |

The input grew 4x while memory stayed flat. Chunk size controls the trade-off: 1,000-row chunks ran about 1.7x slower than 5,000 and above, while peak memory grew with chunk size (2.1 MB at 1,000 rows, 45 MB at 100,000). The default is 10,000.

## Project layout

```
pipeline/    ingest.py  validate.py  db.py  run.py  reconcile.py  generator.py  bench.py  cli.py
tests/       24 pytest tests
data/sample/ 200-row starter dataset (reference/, input/, expected/)
docs/        DATA_DICTIONARY.md  DESIGN.md
output/      test results, demo output, benchmark results, reconciliation reports
```

More detail: [`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md) for schemas and naming rules, and [`docs/DESIGN.md`](docs/DESIGN.md) for design choices and trade-offs.

## Assumptions and limitations

- "JSON" means **JSON Lines** (one object per line). A single huge JSON array can't be streamed this way and is rejected with a clear message.
- Reference data (customers, products) is small and kept in memory; it is overwritten with current values, with no history.
- CSV line numbers assume no quoted fields that contain line breaks.
- Two events with the same order, line, time and status but different quantities are treated as duplicates (the first one wins).
- Timestamps without a timezone are treated as UTC.
- SQLite allows one writer at a time, so don't run two loads against the same database at once.

## Possible next steps

Detect conflicting duplicates by hashing the full row, export rejects to a CSV file, add per-column data profiling to the report, write Parquet for analytics, validate chunks in parallel, and wrap the run in a scheduler such as Airflow with alerts on failure.
