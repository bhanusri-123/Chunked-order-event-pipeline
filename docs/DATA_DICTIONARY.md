# Data dictionary

## Source: `orders.csv` / `orders.jsonl` (event feed) - all 8 columns required

| Column | Type (input) | Rule | Notes |
|---|---|---|---|
| `order_id` | text | non-empty | e.g. `O00000020` |
| `line_no` | integer >= 1 | `3`, `"3"` and `3.0` accepted; `1.5` rejected | with `order_id` identifies the order line |
| `customer_id` | text | must exist in `customers` | |
| `product_id` | text | must exist in `products` | |
| `quantity` | integer | 1..1000 | |
| `unit_price` | decimal | 0..100000, converted to cents | price at order time (may differ from list price) |
| `status` | text | one of created, paid, shipped, delivered, cancelled, returned | trimmed, lower-cased |
| `event_ts` | ISO-8601 | with offset or naive (= UTC); normalised to UTC | e.g. `2026-01-25T21:37:26+05:30` |

Keys: line = (`order_id`, `line_no`); event = line + `event_ts` + `status`. Nullability: nothing may be null/blank.

## Reference: `customers.csv`, `products.csv`

`customers(customer_id PK, customer_name, country, created_at)`;
`products(product_id PK, product_name, category, list_price)` -> stored as `list_price_cents`.
Rows with blank fields or duplicate keys are skipped (first wins); counts returned by `load_reference`.

## Target tables (SQLite)

**`order_line_events`** - append-only history, grain = one distinct event.
`event_id` PK (sha1 of `order_id|line_no|event_ts|status`, first 16 hex), `order_id`, `line_no`,
`customer_id` FK, `product_id` FK, `quantity`, `unit_price_cents`, `line_total_cents` (= quantity x
unit_price_cents), `status`, `event_ts` (UTC `YYYY-MM-DDTHH:MM:SS.ffffffZ`), `run_id`, `source_file`,
`source_line` (provenance of the *first* arrival). Index on (`order_id`, `line_no`, `event_ts`).

**`order_lines`** - current state, grain = one order line. PK (`order_id`, `line_no`); same business columns
as above for the latest event (max `event_ts`, tie-break max `event_id`) plus `last_event_id`, `last_run_id`.

**`rejects`** - quarantine. `reject_id` PK (sha1 of `source_file|source_line|reason_code`), `run_id`,
`source_file`, `source_line`, `reason_code`, `reason_detail` (field name), `raw_record`, `rejected_at`.

**`run_log`** - one row per run. `run_id` PK, `source_file`, `source_sha256`, `source_format`, `status`
(`running|success|failed|skipped`), `started_at`, `ended_at`, `rows_in`, `rows_valid`, `rows_rejected`,
`rows_duplicate`, `rows_new_events`, `rows_state_changes`, `chunks_committed`, `failure_reason`, `notes`.

## Reject codes

| Code | Meaning |
|---|---|
| `MALFORMED_RECORD` | CSV row with too many fields, or JSON line that is not a valid JSON object (incl. blank line) |
| `MISSING_REQUIRED_FIELD` | blank / null required field (`reason_detail` names it) |
| `INVALID_FORMAT` | non-integer `line_no`/`quantity`, non-numeric `unit_price`, unparseable `event_ts` |
| `OUT_OF_RANGE` | `line_no < 1`, quantity outside 1..1000, price < 0 or > 100000 |
| `INVALID_STATUS` | status not in the allowed list |
| `INVALID_REFERENCE_CUSTOMER` / `_PRODUCT` | key not present in the reference table |

## Generator ground truth (`data/*/expected/`, never part of the input)

`expected_summary.json` (row, reject, duplicate, new-event counts; current-state line count and revenue
by status) and `reject_labels.csv` (`source_line`, `expected_reason_code`).
