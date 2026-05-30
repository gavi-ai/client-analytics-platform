import os
from .base_connector import BaseAsyncConnector

class HubspotConnector(BaseAsyncConnector):
    def __init__(self):
        super().__init__(
            source_name="hubspot",
            base_url="https://api.hubapi.com/crm/v3",
            headers={"Authorization": f"Bearer {os.getenv('HUBSPOT_API_KEY')}"}
        )

    def get_endpoint(self, date: str) -> str:
        # Fetching recently modified contacts
        return "/objects/contacts"