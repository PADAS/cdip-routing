import pytest
from app.transform_service.services import transform_observation_v2
from app.transform_service.dispatchers import SmartConnectDispatcher

@pytest.mark.asyncio
async def test_cleaning_an_observation(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    leopard_detected_event_v2,
    destination_integration_v1_smartconnect,
    route_config_with_event_type_mappings

):
    SmartConnectDispatcher(destination_integration_v1_smartconnect)



