import pytest
from app.transform_service.services import transform_observation_v2


@pytest.mark.asyncio
async def test_event_type_mapping(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    leopard_detected_event_v2,
    destination_integration_v2_er,
    route_config_with_event_type_mappings

):
    transformed_observation = transform_observation_v2(
        observation=leopard_detected_event_v2,
        destination=destination_integration_v2_er,
        route_configuration=route_config_with_event_type_mappings
    )
    assert transformed_observation['event_type'] == 'leopard_sighting'


@pytest.mark.asyncio
async def test_event_type_mapping_default(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    unmapped_animal_detected_event_v2,
    destination_integration_v2_er,
    route_config_with_event_type_mappings

):
    # Test with a species that is not in the map
    transformed_observation = transform_observation_v2(
        observation=unmapped_animal_detected_event_v2,
        destination=destination_integration_v2_er,
        route_configuration=route_config_with_event_type_mappings
    )
    # Check that it's mapped to the default type
    assert transformed_observation['event_type'] == 'wildlife_sighting_rep'
