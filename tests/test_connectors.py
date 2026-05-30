import pytest
from unittest.mock import AsyncMock
from ingestion.connectors.stripe import StripeConnector

@pytest.mark.asyncio
async def test_stripe_connector_success():
    connector = StripeConnector()
    mock_client = AsyncMock()
    
    mock_response = AsyncMock()
    mock_response.json.return_value = {"data": [{"id": "ch_123", "amount": 5000}]}
    mock_response.raise_for_status = AsyncMock()
    
    mock_client.get.return_value = mock_response

    result = await connector.extract_async(mock_client, "2025-01-01")
    
    assert result["status"] == "success"
    assert result["source"] == "stripe"
    assert "data" in result["data"]

@pytest.mark.asyncio
async def test_stripe_connector_failure():
    connector = StripeConnector()
    mock_client = AsyncMock()
    
    mock_client.get.side_effect = Exception("API Timeout")

    result = await connector.extract_async(mock_client, "2025-01-01")
    
    assert result["status"] == "failed"
    assert "API Timeout" in result["error"]
