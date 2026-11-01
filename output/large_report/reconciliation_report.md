# Reconciliation & validation report

**Overall: PASS** (15/15 checks)

| check | result | detail |
|---|---|---|
| per-run identity: rows_in = rejected + duplicate + new_events | PASS | 2 runs checked, 0 violations |
| history rows = sum(run_log.rows_new_events) | PASS | events=95211, logged=95211 |
| current-state lines = distinct (order_id, line_no) in history | PASS | order_lines=31202, distinct_in_history=31202 |
| current state = latest event per line (late data never wins) | PASS | 0 mismatches |
| referential integrity (customers, products, FK check) | PASS | orphan_customers=0, orphan_products=0, fk_violations=0 |
| derived field line_total_cents = quantity * unit_price_cents | PASS | 0 violations |
| key aggregate: sum(line_total_cents) current state = latest-in-history | PASS | state=12519737160, history_latest=12519737160 |
| run finished with status=success | PASS | status=success |
| rows_in = generator total_rows | PASS | 100000 vs 100000 |
| rejected count matches labels | PASS | 2855 vs 2855 |
| duplicate count matches | PASS | 1934 vs 1934 |
| new events match | PASS | 95211 vs 95211 |
| every reject traced to the labelled (line, reason) | PASS | got=2855, labelled=2855, missing=0, unexpected=0 |
| current-state line count matches | PASS | 31202 vs 31202 |
| current-state revenue by status matches | PASS | statuses compared=6 |

## Sample analytical queries

### Current order lines and revenue by status

| status | order_lines | revenue |
|---|---|---|
| delivered | 14905 | 62358002.24 |
| shipped | 5202 | 18612572.35 |
| paid | 4828 | 19101565.95 |
| created | 4798 | 16822486.25 |
| returned | 748 | 4678697.22 |
| cancelled | 721 | 3624047.59 |

### Top 5 categories by net revenue (excl. cancelled/returned)

| category | order_lines | revenue |
|---|---|---|
| fashion | 5421 | 20514784.52 |
| sports | 4719 | 18685739.67 |
| electronics | 3719 | 17814489.36 |
| books | 3620 | 17575748.66 |
| toys | 4902 | 17374340.94 |

### Rejects by reason

| reason_code | rows |
|---|---|
| INVALID_REFERENCE_PRODUCT | 453 |
| MISSING_REQUIRED_FIELD | 427 |
| MALFORMED_RECORD | 414 |
| INVALID_FORMAT | 413 |
| OUT_OF_RANGE | 394 |
| INVALID_STATUS | 379 |
| INVALID_REFERENCE_CUSTOMER | 375 |

### Superseded (historical) events kept for audit

| superseded_events |
|---|
| 64009 |

### Run history

| run_id | source_file | status | rows_in | rows_rejected | rows_duplicate | rows_new_events | chunks_committed |
|---|---|---|---|---|---|---|---|
| run_20261101T154041_4cad8ec1 | orders.csv | success | 100000 | 2855 | 1934 | 95211 | 10 |
| run_20261101T154044_4ff774d7 | orders.csv | skipped | 0 | 0 | 0 | 0 | 0 |
| run_20261101T154044_9c2c28e3 | orders.csv | success | 100000 | 2855 | 97145 | 0 | 10 |
