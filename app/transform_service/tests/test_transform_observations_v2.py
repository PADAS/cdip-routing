import pytest
from app.transform_service.services import transform_observation_v2


@pytest.mark.asyncio
async def test_event_type_mapping(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    leopard_detected_event_v2,
    destination_integration_v2_er,
    route_config_with_event_type_mappings,
    connection_v2

):
    transformed_observation = transform_observation_v2(
        observation=leopard_detected_event_v2,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
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
    route_config_with_event_type_mappings,
    connection_v2
):
    # Test with a species that is not in the map
    transformed_observation = transform_observation_v2(
        observation=unmapped_animal_detected_event_v2,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=route_config_with_event_type_mappings
    )
    # Check that it's mapped to the default type
    assert transformed_observation['event_type'] == 'wildlife_sighting_rep'


@pytest.mark.asyncio
async def test_movebank_transform_observation(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    observation_object_v2,
    destination_integration_v2_movebank,
    route_config_with_no_mappings,
    connection_v2
):
    transformed_observation = transform_observation_v2(
        observation=observation_object_v2,
        destination=destination_integration_v2_movebank,
        provider=connection_v2.provider,
        route_configuration=route_config_with_no_mappings
    )
    # Check dictionary schema
    expected_keys = ["recorded_at", "tag_id", "lon", "lat", "sensor_type", "tag_manufacturer_name", "gundi_urn"]
    for key in transformed_observation.keys():
        assert key in expected_keys
    assert transformed_observation['tag_manufacturer_name'] == 'TrapTagger'
    assert transformed_observation['sensor_type'] == 'GPS'


@pytest.mark.asyncio
async def test_transform_observations_for_earthranger(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    observation_object_v2,
    destination_integration_v2_er,
    route_config_with_no_mappings,
    connection_v2

):
    transformed_observation = transform_observation_v2(
        observation=observation_object_v2,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=route_config_with_no_mappings
    )
    assert transformed_observation
    assert transformed_observation.get("manufacturer_id") == observation_object_v2.external_source_id
    assert transformed_observation.get("source_type") == observation_object_v2.type
    assert transformed_observation.get("subject_name") == observation_object_v2.source_name
    assert transformed_observation.get("recorded_at") == observation_object_v2.recorded_at
    assert transformed_observation.get("location") == {
        "lon": observation_object_v2.location.lon,
        "lat": observation_object_v2.location.lat
    }
    assert transformed_observation.get("additional") == observation_object_v2.additional

