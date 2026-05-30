"""
Schema validator.

Lightweight pre-load checks run before data hits BigQuery.
Catches the most common API breakage patterns:
  - Empty responses (API down or auth expired)
  - Missing required columns
  - Null rates above threshold

Full contract enforcement (dbt schema tests, YAML contracts)
runs downstream after the raw load.
"""

import logging

logger = logging.getLogger(__name__)

# Required columns per source — if any are missing from every record, fail fast
REQUIRED_COLUMNS: dict[str, list[str]] = {
    "stripe":  ["payment_id", "amount", "currency", "status", "created_at"],
    "hubspot": ["customer_id", "email", "created_at"],
    "shopify": ["order_id", "total_price", "created_at"],
    "ga4":     ["session_id", "event_name", "event_date"],
}

# Maximum allowed null rate per required column (0.05 = 5%)
NULL_RATE_THRESHOLD = 0.05


def validate_source(source_name: str, records: list[dict]) -> list[str]:
    """
    Validate records from a single source.

    Returns a list of error strings (empty = all good).
    """
    errors: list[str] = []

    # Check 1 — non-empty response
    if not records:
        errors.append(f"No records returned — API may be down or auth expired.")
        return errors   # no point checking columns if empty

    required = REQUIRED_COLUMNS.get(source_name, [])
    n = len(records)

    for col in required:
        # Check 2 — column exists in at least one record
        present_count = sum(1 for r in records if col in r)
        if present_count == 0:
            errors.append(
                f"Column '{col}' missing from all {n} records. "
                f"Possible API schema change."
            )
            continue

        # Check 3 — null rate within threshold
        null_count = sum(1 for r in records if r.get(col) is None)
        null_rate = null_count / n
        if null_rate > NULL_RATE_THRESHOLD:
            errors.append(
                f"Column '{col}' has {null_rate:.1%} null rate "
                f"({null_count}/{n} records). Threshold: {NULL_RATE_THRESHOLD:.0%}."
            )

    return errors


def validate_all(source_records: dict[str, list[dict]]) -> dict[str, str]:
    """
    Validate all sources. Returns dict of source_name -> error string
    for any sources that failed. Empty dict = all passed.
    """
    failures: dict[str, str] = {}

    for source_name, records in source_records.items():
        errors = validate_source(source_name, records)
        if errors:
            failure_msg = "; ".join(errors)
            logger.error("[%s] Validation failed: %s", source_name, failure_msg)
            failures[source_name] = failure_msg
        else:
            logger.info("[%s] Validation passed (%d records)", source_name, len(records))

    return failures
