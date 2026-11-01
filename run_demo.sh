#!/usr/bin/env bash
# End-to-end demo from a clean checkout: sample run, 100k-row run, rerun/idempotency, reconciliation.
set -euo pipefail
PY=${PYTHON:-python3}
mkdir -p output && rm -f output/sample.db* output/large.db*

echo "== 1. starter sample (200 rows, CSV) =="
$PY -m pipeline generate --out data/sample --rows 200 --seed 7 --format csv >/dev/null
$PY -m pipeline run --input data/sample/input/orders.csv --reference-dir data/sample/reference --db output/sample.db --chunk-size 50 >/dev/null
$PY -m pipeline reconcile --db output/sample.db --report-dir output/sample_report --expected-dir data/sample/expected

echo; echo "== 2. larger run (100,000 rows, CSV, chunk_size=10,000) =="
$PY -m pipeline generate --out data/large --rows 100000 --seed 42 --format csv >/dev/null
$PY -m pipeline run --input data/large/input/orders.csv --reference-dir data/large/reference --db output/large.db --chunk-size 10000 2>/dev/null \
  | $PY -c "import sys,json; t=sys.stdin.read(); j=json.loads(t[t.index('\n{')+1:]); print({k:j[k] for k in ('status','rows_in','rows_rejected','rows_duplicate','rows_new_events','chunks_committed','elapsed_s','rows_per_s','timings_s')})"

echo; echo "== 3. rerun the SAME file: skipped; then forced reprocess: no duplicates =="
$PY -m pipeline run --input data/large/input/orders.csv --db output/large.db 2>/dev/null | $PY -c "import sys,json; j=json.load(sys.stdin); print('rerun   ->', j['status'], j['notes'])"
$PY -m pipeline run --input data/large/input/orders.csv --db output/large.db --force 2>/dev/null | $PY -c "import sys,json; j=json.load(sys.stdin); print('forced  ->', j['status'], 'new_events=%d duplicates=%d' % (j['rows_new_events'], j['rows_duplicate']))"

echo; echo "== 4. reconciliation (internal checks after 3 runs; ground-truth check on the first run) =="
FIRST=$(sqlite3 output/large.db "select run_id from run_log where status='success' order by started_at limit 1" 2>/dev/null || $PY -c "import sqlite3;print(sqlite3.connect('output/large.db').execute(\"select run_id from run_log where status='success' order by started_at limit 1\").fetchone()[0])")
$PY -m pipeline reconcile --db output/large.db --report-dir output/large_report --expected-dir data/large/expected --run-id "$FIRST"

echo; echo "== 5. same sample as JSON Lines =="
rm -f output/sample_jsonl.db*
$PY -m pipeline generate --out data/sample_jsonl --rows 200 --seed 7 --format jsonl >/dev/null
$PY -m pipeline run --input data/sample_jsonl/input/orders.jsonl --reference-dir data/sample_jsonl/reference --db output/sample_jsonl.db --chunk-size 50 >/dev/null 2>&1
$PY -m pipeline reconcile --db output/sample_jsonl.db --report-dir output/sample_jsonl_report --expected-dir data/sample_jsonl/expected | tail -3
