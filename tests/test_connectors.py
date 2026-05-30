"""
Unit tests for connectors.

These run in CI without any real API keys.
All external calls are mocked — tests validate logic, not connectivity.
"""

import unittest
from unittest.mock import MagicMock, patch

from ingestion.connectors.base_connector import ConnectorError, RateLimitError
from ingestion.utils.schema_validator import validate_source, validate_all


# ------------------------------------------------------------------
# Schema validator tests — no mocking needed
# ------------------------------------------------------------------

class TestSchemaValidator(unittest.TestCase):

    def test_passes_for_valid_stripe_records(self):
        records = [
            {
                "payment_id": "ch_001",
                "amount": 99.99,
                "currency": "USD",
                "status": "succeeded",
                "created_at": 1700000000,
            }
        ] * 10
        errors = validate_source("stripe", records)
        self.assertEqual(errors, [], f"Expected no errors, got: {errors}")

    def test_fails_for_empty_records(self):
        errors = validate_source("stripe", [])
        self.assertTrue(len(errors) > 0, "Expected error for empty records")
        self.assertIn("No records", errors[0])

    def test_fails_for_missing_required_column(self):
        records = [{"amount": 10.0, "currency": "USD", "status": "succeeded", "created_at": 123}] * 5
        # payment_id is missing
        errors = validate_source("stripe", records)
        self.assertTrue(any("payment_id" in e for e in errors))

    def test_fails_for_high_null_rate(self):
        records = [{"payment_id": f"ch_{i}", "amount": None, "currency": "USD", "status": "succeeded", "created_at": 123} for i in range(20)]
        errors = validate_source("stripe", records)
        self.assertTrue(any("amount" in e for e in errors))

    def test_validate_all_returns_empty_for_clean_data(self):
        source_records = {
            "stripe": [
                {
                    "payment_id": f"ch_{i}",
                    "amount": 10.0,
                    "currency": "USD",
                    "status": "succeeded",
                    "created_at": 123,
                }
                for i in range(5)
            ]
        }
        failures = validate_all(source_records)
        self.assertEqual(failures, {})

    def test_validate_all_returns_failures_dict(self):
        failures = validate_all({"stripe": []})
        self.assertIn("stripe", failures)


# ------------------------------------------------------------------
# Stripe connector tests — mock the HTTP layer
# ------------------------------------------------------------------

class TestStripeConnector(unittest.TestCase):

    def _make_connector(self):
        with patch.dict("os.environ", {"STRIPE_API_KEY": "sk_test_fake"}):
            from ingestion.connectors.stripe import StripeConnector
            return StripeConnector()

    def test_normalise_converts_cents_to_dollars(self):
        from ingestion.connectors.stripe import StripeConnector
        raw = {
            "id": "ch_001",
            "customer": "cus_001",
            "amount": 9999,           # cents
            "amount_refunded": 0,
            "currency": "usd",
            "status": "succeeded",
            "paid": True,
            "refunded": False,
            "description": "Test charge",
            "receipt_email": "test@example.com",
            "failure_code": None,
            "failure_message": None,
            "created": 1700000000,
        }
        result = StripeConnector._normalise(raw)
        self.assertAlmostEqual(result["amount"], 99.99)
        self.assertEqual(result["currency"], "USD")   # uppercased
        self.assertEqual(result["payment_id"], "ch_001")

    def test_source_name_is_stripe(self):
        connector = self._make_connector()
        self.assertEqual(connector.source_name, "stripe")

    @patch("ingestion.connectors.stripe.requests.Session.get")
    def test_fetch_page_raises_rate_limit_error_on_429(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_get.return_value = mock_response

        connector = self._make_connector()
        with self.assertRaises(RateLimitError):
            connector.fetch_page(connector._session, "2026-01-01", None)


# ------------------------------------------------------------------
# Async extractor tests
# ------------------------------------------------------------------

class TestAsyncExtractor(unittest.TestCase):

    def test_failed_connector_excluded_from_results(self):
        """A connector that raises should not block others."""
        import asyncio
        from ingestion.utils.async_extractor import extract_all_sources
        from ingestion.connectors.base_connector import BaseConnector
        import httpx

        class GoodConnector(BaseConnector):
            source_name = "good"
            def fetch_page(self, session, date, cursor):
                return [{"id": "1"}], None
            async def fetch_page_async(self, client, date, cursor):
                return [{"id": "1"}], None

        class BadConnector(BaseConnector):
            source_name = "bad"
            def fetch_page(self, session, date, cursor):
                raise RuntimeError("API down")
            async def fetch_page_async(self, client, date, cursor):
                raise RuntimeError("API down")

        with patch.dict("os.environ", {"STRIPE_API_KEY": "sk_test_fake"}):
            results = asyncio.run(
                extract_all_sources(
                    [GoodConnector("k", "https://example.com"), BadConnector("k", "https://example.com")],
                    date="2026-01-01",
                )
            )

        self.assertIn("good", results)
        self.assertNotIn("bad", results)
        self.assertEqual(len(results["good"]), 1)


if __name__ == "__main__":
    unittest.main()
