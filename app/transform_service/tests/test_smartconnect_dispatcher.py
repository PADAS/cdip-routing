import pytest
from app.transform_service.dispatchers import SmartConnectDispatcher

@pytest.mark.asyncio
async def test_cleaning_a_smartrequest(
    mock_cache,
    destination_integration_v1_smartconnect,
    smartrequest_with_no_attachments

):
    sd = SmartConnectDispatcher(destination_integration_v1_smartconnect)
    sd.clean_smart_request(smartrequest_with_no_attachments)



