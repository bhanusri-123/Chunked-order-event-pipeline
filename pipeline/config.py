"""Shared constants and run configuration."""
from __future__ import annotations

from dataclasses import dataclass

# Required columns of the orders event feed (all are mandatory).
REQUIRED_COLUMNS = [
    "order_id", "line_no", "customer_id", "product_id",
    "quantity", "unit_price", "status", "event_ts",
]
VALID_STATUSES = ("created", "paid", "shipped", "delivered", "cancelled", "returned")

# Marks a physical line that could not be parsed (wrong field count / bad JSON).
# The readers keep such lines in the chunk (so line numbers stay aligned) and
# the validator turns them into MALFORMED_RECORD rejects.
MALFORMED_SENTINEL = "__MALFORMED__:"

TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"  # canonical UTC timestamp, fixed width, sortable


@dataclass(frozen=True)
class Config:
    db_path: str = "output/warehouse.db"
    chunk_size: int = 10_000
    max_quantity: int = 1000
    max_unit_price: float = 100_000.0
    log_level: str = "INFO"
