"""
Stripe connector.

Uses the Stripe REST API (list charges endpoint).
Works with a free Stripe test key — no real charges needed.

Env var required: STRIPE_API_KEY (sk_test_... for local dev)
"""

import os
from typing import Any

import httpx
import requests

from ingestion.connectors.base_connector import BaseConnector, RateLimitError

STRIPE_BASE_URL = "https://api.stripe.com/v1"


class StripeConnector(BaseConnector):
    """
    Extracts payment charges from the Stripe API.

    Paginates using Stripe's cursor-based pagination (starting_after).
    Normalises raw API response to a flat dict before returning.
    """

    def __init__(self):
        api_key = os.environ["STRIPE_API_KEY"]
        super().__init__(api_key=api_key, base_url=STRIPE_BASE_URL)
        self._auth = (api_key, "")  # Stripe uses HTTP Basic Auth

    @property
    def source_name(self) -> str:
        return "stripe"

    # ------------------------------------------------------------------
    # Sync implementation
    # ------------------------------------------------------------------

    def fetch_page(
        self,
        session: requests.Session,
        date: str,
        cursor: Any | None,
    ) -> tuple[list[dict], Any | None]:
        """
        Fetch one page of charges created on `date`.
        date format: "YYYY-MM-DD"
        """
        import time
        from datetime import datetime, timezone

        # Convert date string to Unix timestamps for Stripe's filter
        dt = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        created_gte = int(dt.timestamp())
        created_lt = created_gte + 86400  # next day

        params: dict[str, Any] = {
            "limit": self.PAGE_SIZE,
            "created[gte]": created_gte,
            "created[lt]": created_lt,
        }
        if cursor:
            params["starting_after"] = cursor

        response = session.get(
            f"{self.base_url}/charges",
            auth=self._auth,
            params=params,
            timeout=30,
        )

        if response.status_code == 429:
            raise RateLimitError("Stripe rate limit hit")
        response.raise_for_status()

        data = response.json()
        records = [self._normalise(charge) for charge in data.get("data", [])]

        next_cursor = records[-1]["payment_id"] if data.get("has_more") and records else None
        return records, next_cursor

    # ------------------------------------------------------------------
    # Async implementation
    # ------------------------------------------------------------------

    async def fetch_page_async(
        self,
        client: httpx.AsyncClient,
        date: str,
        cursor: Any | None,
    ) -> tuple[list[dict], Any | None]:
        from datetime import datetime, timezone

        dt = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        created_gte = int(dt.timestamp())
        created_lt = created_gte + 86400

        params: dict[str, Any] = {
            "limit": self.PAGE_SIZE,
            "created[gte]": created_gte,
            "created[lt]": created_lt,
        }
        if cursor:
            params["starting_after"] = cursor

        response = await client.get(
            f"{self.base_url}/charges",
            auth=self._auth,
            params=params,
        )

        if response.status_code == 429:
            raise RateLimitError("Stripe rate limit hit (async)")
        response.raise_for_status()

        data = response.json()
        records = [self._normalise(charge) for charge in data.get("data", [])]
        next_cursor = records[-1]["payment_id"] if data.get("has_more") and records else None
        return records, next_cursor

    # ------------------------------------------------------------------
    # Normalisation — raw API → flat dict matching our schema
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise(charge: dict) -> dict:
        """Flatten a raw Stripe charge object into our canonical schema."""
        return {
            "payment_id":       charge["id"],
            "customer_id":      charge.get("customer"),
            "amount":           charge["amount"] / 100,          # Stripe stores cents
            "amount_refunded":  charge.get("amount_refunded", 0) / 100,
            "currency":         charge["currency"].upper(),
            "status":           charge["status"],
            "paid":             charge["paid"],
            "refunded":         charge.get("refunded", False),
            "description":      charge.get("description"),
            "receipt_email":    charge.get("receipt_email"),
            "failure_code":     charge.get("failure_code"),
            "failure_message":  charge.get("failure_message"),
            "created_at":       charge["created"],               # Unix timestamp
            "source_name":      "stripe",
        }
