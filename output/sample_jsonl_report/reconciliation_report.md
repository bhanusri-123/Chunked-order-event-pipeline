# Reconciliation & validation report

**Overall: PASS** (15/15 checks)

| check | result | detail |
|---|---|---|
| per-run identity: rows_in = rejected + duplicate + new_events | PASS | 1 runs checked, 0 violations |
| history rows = sum(run_log.rows_new_events) | PASS | events=183, logged=183 |
| current-state lines = distinct (order_id, line_no) in history | PASS | order_lines=63, distinct_in_history=63 |
| current state = latest event per line (late data never wins) | PASS | 0 mismatches |
| referential integrity (customers, products, FK check) | PASS | orphan_customers=0, orphan_products=0, fk_violations=0 |
| derived field line_total_cents = quantity * unit_price_cents | PASS | 0 violations |
| key aggregate: sum(line_total_cents) current state = latest-in-history | PASS | state=7806330, history_latest=7806330 |
| run finished with status=success | PASS | status=success |
| rows_in = generator total_rows | PASS | 200 vs 200 |
| rejected count matches labels | PASS | 10 vs 10 |
| duplicate count matches | PASS | 7 vs 7 |
| new events match | PASS | 183 vs 183 |
| every reject traced to the labelled (line, reason) | PASS | got=10, labelled=10, missing=0, unexpected=0 |
| current-state line count matches | PASS | 63 vs 63 |
| current-state revenue by status matches | PASS | statuses compared=6 |

## Sample analytical queries

### Current order lines and revenue by status

| status | order_lines | revenue |
|---|---|---|
| delivered | 26 | 31038.99 |
| created | 16 | 15489.98 |
| shipped | 9 | 14667.7 |
| paid | 7 | 9628.52 |
| returned | 4 | 5119.03 |
| cancelled | 1 | 2119.08 |

### Top 5 categories by net revenue (excl. cancelled/returned)

| category | order_lines | revenue |
|---|---|---|
| sports | 12 | 33857.78 |
| home | 14 | 11087.21 |
| fashion | 10 | 10432.04 |
| grocery | 16 | 8365.39 |
| toys | 6 | 7082.77 |

### Rejects by reason

| reason_code | rows |
|---|---|
| INVALID_REFERENCE_CUSTOMER | 3 |
| INVALID_FORMAT | 2 |
| OUT_OF_RANGE | 1 |
| MISSING_REQUIRED_FIELD | 1 |
| MALFORMED_RECORD | 1 |
| INVALID_STATUS | 1 |
| INVALID_REFERENCE_PRODUCT | 1 |

### Superseded (historical) events kept for audit

| superseded_events |
|---|
| 120 |

### Run history

| run_id | source_file | status | rows_in | rows_rejected | rows_duplicate | rows_new_events | chunks_committed |
|---|---|---|---|---|---|---|---|
| run_20261101T154058_cf264dda | orders.jsonl | success | 200 | 10 | 7 | 183 | 4 |
