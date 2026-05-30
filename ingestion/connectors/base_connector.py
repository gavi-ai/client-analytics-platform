"""
Base connector class.
All API connectors extend this. Provides pagination, exponential backoff,
rate-limit handling (HTTP 429), and structured logging out of the box.
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Generator

import httpx
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """
    Abstract base for all SaaS API connectors.

    Subclasses must implement:
        - source_name (str property)
        - fetch_page(session, date, cursor) -> (records, next_cursor)

    Everything else — retry logic, pagination loop, rate limiting — is inherited.
    """

    MAX_RETRIES = 3
    BACKOFF_FACTOR = 2          # wait 2s, 4s, 8s between retries
    RATE_LIMIT_WAIT = 60        # seconds to wait on HTTP 429
    PAGE_SIZE = 100

    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self._session = self._build_session()

    # ------------------------------------------------------------------
    # Abstract interface — subclasses implement these two methods only
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Unique identifier for this connector, e.g. 'stripe', 'hubspot'."""

    @abstractmethod
    def fetch_page(
        self,
        session: requests.Session,
        date: str,
        cursor: Any | None,
    ) -> tuple[list[dict], Any | None]:
        """
        Fetch one page of records for the given date.

        Returns:
            records   — list of raw dicts from the API
            cursor    — next page cursor, or None if last page
        """

    # ------------------------------------------------------------------
    # Async variant (used by async_extractor.py for parallel pulls)
    # ------------------------------------------------------------------

    @abstractmethod
    async def fetch_page_async(
        self,
        client: httpx.AsyncClient,
        date: str,
        cursor: Any | None,
    ) -> tuple[list[dict], Any | None]:
        """Async version of fetch_page. Must be implemented by subclasses."""

    # ------------------------------------------------------------------
    # Public extract methods
    # ------------------------------------------------------------------

    def extract(self, date: str) -> list[dict]:
        """
        Full synchronous extraction for a given date.
        Paginates until no more pages remain.
        """
        all_records: list[dict] = []
        cursor = None
        page = 1

        logger.info("[%s] Starting extraction for date=%s", self.source_name, date)

        while True:
            records, cursor = self._fetch_with_retry(date, cursor)
            all_records.extend(records)
            logger.info(
                "[%s] Page %d — fetched %d records (total so far: %d)",
                self.source_name, page, len(records), len(all_records),
            )
            page += 1
            if cursor is None:
                break

        logger.info(
            "[%s] Extraction complete — %d total records", self.source_name, len(all_records)
        )
        return all_records

    async def extract_async(self, client: httpx.AsyncClient, date: str) -> list[dict]:
        """
        Full async extraction for a given date.
        Used by async_extractor.py for concurrent multi-source pulls.
        """
        all_records: list[dict] = []
        cursor = None
        page = 1

        logger.info("[%s] Async extraction started for date=%s", self.source_name, date)

        while True:
            records, cursor = await self._fetch_async_with_retry(client, date, cursor)
            all_records.extend(records)
            page += 1
            if cursor is None:
                break

        logger.info("[%s] Async extraction complete — %d records", self.source_name, len(all_records))
        return all_records

    # ------------------------------------------------------------------
    # Retry wrappers
    # ------------------------------------------------------------------

    def _fetch_with_retry(
        self, date: str, cursor: Any | None
    ) -> tuple[list[dict], Any | None]:
        last_exc = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                return self.fetch_page(self._session, date, cursor)
            except RateLimitError:
                logger.warning(
                    "[%s] Rate limited. Waiting %ds before retry.",
                    self.source_name, self.RATE_LIMIT_WAIT,
                )
                time.sleep(self.RATE_LIMIT_WAIT)
            except requests.RequestException as exc:
                wait = self.BACKOFF_FACTOR ** attempt
                logger.warning(
                    "[%s] Attempt %d/%d failed (%s). Retrying in %ds.",
                    self.source_name, attempt, self.MAX_RETRIES, exc, wait,
                )
                last_exc = exc
                time.sleep(wait)

        raise ConnectorError(
            f"[{self.source_name}] All {self.MAX_RETRIES} retries exhausted."
        ) from last_exc

    async def _fetch_async_with_retry(
        self, client: httpx.AsyncClient, date: str, cursor: Any | None
    ) -> tuple[list[dict], Any | None]:
        last_exc = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                return await self.fetch_page_async(client, date, cursor)
            except RateLimitError:
                logger.warning("[%s] Rate limited (async). Waiting %ds.", self.source_name, self.RATE_LIMIT_WAIT)
                await asyncio.sleep(self.RATE_LIMIT_WAIT)
            except httpx.HTTPError as exc:
                wait = self.BACKOFF_FACTOR ** attempt
                logger.warning(
                    "[%s] Async attempt %d/%d failed (%s). Retrying in %ds.",
                    self.source_name, attempt, self.MAX_RETRIES, exc, wait,
                )
                last_exc = exc
                await asyncio.sleep(wait)

        raise ConnectorError(
            f"[{self.source_name}] All async retries exhausted."
        ) from last_exc

    # ------------------------------------------------------------------
    # Session factory
    # ------------------------------------------------------------------

    def _build_session(self) -> requests.Session:
        """Build a requests Session with connection-level retry and timeouts."""
        session = requests.Session()
        retry = Retry(
            total=self.MAX_RETRIES,
            backoff_factor=self.BACKOFF_FACTOR,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session


# ------------------------------------------------------------------
# Custom exceptions
# ------------------------------------------------------------------

class RateLimitError(Exception):
    """Raised when the API responds with HTTP 429."""


class ConnectorError(Exception):
    """Raised when all retries are exhausted."""
