import httpx
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class BaseAsyncConnector(ABC):
    def __init__(self, source_name: str, base_url: str, headers: dict):
        self.source_name = source_name
        self.base_url = base_url
        self.headers = headers

    @abstractmethod
    def get_endpoint(self, date: str) -> str:
        pass

    async def extract_async(self, client: httpx.AsyncClient, date: str) -> dict:
        endpoint = self.get_endpoint(date)
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = await client.get(url, headers=self.headers, timeout=30.0)
            response.raise_for_status()
            return {"source": self.source_name, "status": "success", "data": response.json()}
        except Exception as e:
            logger.error(f"[{self.source_name}] Connection error: {str(e)}")
            return {"source": self.source_name, "status": "failed", "error": str(e)}
