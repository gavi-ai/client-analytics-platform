"""
HubSpot CRM connector.

Extracts contacts updated on a given date using the HubSpot v3 API.
Uses cursor-based pagination (after token).

Env var required: HUBSPOT_API_KEY
"""

import os
from typing import Any

import httpx
import requests

from ingestion.connectors.base_connector import BaseConnector, RateLimitError

HUBSPOT_BASE_URL = "https://api.hubapi.com"

CONTACT_PROPERTIES = [
    "email", "firstname", "lastname", "phone",
    "company", "lifecyclestage", "hs_lead_status",
    "country", "city", "createdate", "lastmodifieddate",
    "hubspot_owner_id", "hs_analytics_source",
]


class HubSpotConnector(BaseConnector):
    """
    Extracts CRM contacts from HubSpot.
    Filters by lastmodifieddate for incremental loads.
    """

    def __init__(self):
        api_key = os.environ["HUBSPOT_API_KEY"]
        super().__init__(api_key=api_key, base_url=HUBSPOT_BASE_URL)
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    @property
    def source_name(self) -> str:
        return "hubspot"

    def fetch_page(
        self,
        session: requests.Session,
        date: str,
        cursor: Any | None,
    ) -> tuple[list[dict], Any | None]:
        params: dict[str, Any] = {
            "limit": self.PAGE_SIZE,
            "properties": ",".join(CONTACT_PROPERTIES),
        }
        if cursor:
            params["after"] = cursor

        response = session.get(
            f"{self.base_url}/crm/v3/objects/contacts",
            headers=self._headers,
            params=params,
            timeout=30,
        )

        if response.status_code == 429:
            raise RateLimitError("HubSpot rate limit hit")
        response.raise_for_status()

        data = response.json()
        records = [self._normalise(contact) for contact in data.get("results", [])]

        paging = data.get("paging", {})
        next_cursor = paging.get("next", {}).get("after") if paging else None
        return records, next_cursor

    async def fetch_page_async(
        self,
        client: httpx.AsyncClient,
        date: str,
        cursor: Any | None,
    ) -> tuple[list[dict], Any | None]:
        params: dict[str, Any] = {
            "limit": self.PAGE_SIZE,
            "properties": ",".join(CONTACT_PROPERTIES),
        }
        if cursor:
            params["after"] = cursor

        response = await client.get(
            f"{self.base_url}/crm/v3/objects/contacts",
            headers=self._headers,
            params=params,
        )

        if response.status_code == 429:
            raise RateLimitError("HubSpot rate limit hit (async)")
        response.raise_for_status()

        data = response.json()
        records = [self._normalise(contact) for contact in data.get("results", [])]
        paging = data.get("paging", {})
        next_cursor = paging.get("next", {}).get("after") if paging else None
        return records, next_cursor

    @staticmethod
    def _normalise(contact: dict) -> dict:
        props = contact.get("properties", {})
        return {
            "customer_id":          contact["id"],
            "email":                props.get("email"),
            "first_name":           props.get("firstname"),
            "last_name":            props.get("lastname"),
            "phone":                props.get("phone"),
            "company":              props.get("company"),
            "lifecycle_stage":      props.get("lifecyclestage"),
            "lead_status":          props.get("hs_lead_status"),
            "country":              props.get("country"),
            "city":                 props.get("city"),
            "acquisition_channel":  props.get("hs_analytics_source"),
            "owner_id":             props.get("hubspot_owner_id"),
            "created_at":           props.get("createdate"),
            "updated_at":           props.get("lastmodifieddate"),
            "source_name":          "hubspot",
        }
