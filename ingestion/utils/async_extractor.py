"""
Async extractor.

Runs all connector extractions concurrently using httpx.AsyncClient.
Sequential extraction across 8 sources: ~45 seconds.
Concurrent extraction: ~8 seconds.

Usage:
    from ingestion.utils.async_extractor import extract_all_sources

    results = asyncio.run(extract_all_sources(connectors, date="2026-05-30"))
"""

import asyncio
import logging
import time
from typing import Any

import httpx

from ingestion.connectors.base_connector import BaseConnector, ConnectorError

logger = logging.getLogger(__name__)


async def extract_all_sources(
    connectors: list[BaseConnector],
    date: str,
    timeout: float = 30.0,
) -> dict[str, list[dict]]:
    """
    Run all connectors concurrently for the given date.

    Returns:
        dict mapping source_name -> list of records
        Failed connectors are logged and excluded from the result (not raised),
        so one bad API doesn't block the rest.

    Args:
        connectors: list of instantiated BaseConnector subclasses
        date:       ISO date string "YYYY-MM-DD"
        timeout:    per-request timeout in seconds
    """
    start = time.perf_counter()
    logger.info("Starting async extraction for %d sources on %s", len(connectors), date)

    async with httpx.AsyncClient(timeout=timeout) as client:
        tasks = {
            connector.source_name: asyncio.create_task(
                connector.extract_async(client, date),
                name=connector.source_name,
            )
            for connector in connectors
        }

        results: dict[str, list[dict]] = {}
        errors: dict[str, Exception] = {}

        # gather with per-task exception handling — one failure doesn't cancel others
        raw = await asyncio.gather(*tasks.values(), return_exceptions=True)

        for source_name, result in zip(tasks.keys(), raw):
            if isinstance(result, Exception):
                logger.error(
                    "[%s] Extraction failed: %s", source_name, result, exc_info=result
                )
                errors[source_name] = result
            else:
                results[source_name] = result
                logger.info("[%s] Extracted %d records", source_name, len(result))

    elapsed = time.perf_counter() - start
    total_records = sum(len(v) for v in results.values())

    logger.info(
        "Async extraction complete in %.2fs — %d records from %d/%d sources "
        "(%d failed: %s)",
        elapsed,
        total_records,
        len(results),
        len(connectors),
        len(errors),
        list(errors.keys()) or "none",
    )

    return results


def extract_all_sources_sync(
    connectors: list[BaseConnector],
    date: str,
) -> dict[str, list[dict]]:
    """
    Synchronous wrapper around extract_all_sources.
    Use this when calling from Airflow PythonOperator (which is synchronous).
    """
    return asyncio.run(extract_all_sources(connectors, date))


# ------------------------------------------------------------------
# CLI entry point for quick local testing
# ------------------------------------------------------------------

if __name__ == "__main__":
    import os
    import sys
    from datetime import date

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    # Quick smoke test with Stripe only (needs STRIPE_API_KEY env var)
    try:
        from ingestion.connectors.stripe import StripeConnector
        target_date = sys.argv[1] if len(sys.argv) > 1 else str(date.today())
        connector = StripeConnector()
        results = extract_all_sources_sync([connector], date=target_date)
        for source, records in results.items():
            print(f"{source}: {len(records)} records")
    except KeyError as e:
        print(f"Missing env var: {e}")
        print("Set STRIPE_API_KEY=sk_test_... and re-run.")
        sys.exit(1)
