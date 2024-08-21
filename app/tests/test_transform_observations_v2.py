from datetime import datetime
import pytest
import pytz
from smartconnect.models import SMARTCONNECT_DATFORMAT
from gundi_core import schemas
from app.core.errors import ReferenceDataError
from app.services.transformers import transform_observation_v2


@pytest.mark.asyncio
async def test_event_type_mapping(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    leopard_detected_from_species_event_v2,
    destination_integration_v2_er,
    route_config_with_event_type_mappings,
    connection_v2,
):
    transformed_observation = await transform_observation_v2(
        observation=leopard_detected_from_species_event_v2,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=route_config_with_event_type_mappings,
    )
    assert transformed_observation.event_type == "leopard_sighting"


@pytest.mark.asyncio
async def test_event_type_mapping_default(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    unmapped_animal_detected_event_v2,
    destination_integration_v2_er,
    route_config_with_event_type_mappings,
    connection_v2,
):
    # Test with a species that is not in the map
    transformed_observation = await transform_observation_v2(
        observation=unmapped_animal_detected_event_v2,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=route_config_with_event_type_mappings,
    )
    # Check that it's mapped to the default type
    assert transformed_observation.event_type == "wildlife_sighting_rep"


@pytest.mark.asyncio
async def test_event_type_mapping_with_empty_event_details(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    event_v2_with_empty_event_details,
    destination_integration_v2_er,
    route_config_with_event_type_mappings,
    connection_v2,
):
    transformed_observation = await transform_observation_v2(
        observation=event_v2_with_empty_event_details,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=route_config_with_event_type_mappings,
    )
    # If event_details is empty, expect the event_type to be the same as the original one
    assert transformed_observation.event_type == event_v2_with_empty_event_details.event_type


@pytest.mark.asyncio
async def test_movebank_transform_observation(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    observation_object_v2,
    destination_integration_v2_movebank,
    route_config_with_no_mappings,
    connection_v2,
):
    transformed_observation = await transform_observation_v2(
        observation=observation_object_v2,
        destination=destination_integration_v2_movebank,
        provider=connection_v2.provider,
        route_configuration=route_config_with_no_mappings,
    )
    # Check dictionary schema
    expected_keys = [
        "recorded_at",
        "tag_id",
        "lon",
        "lat",
        "sensor_type",
        "tag_manufacturer_name",
        "gundi_urn",
    ]
    for key in transformed_observation.keys():
        assert key in expected_keys
    assert transformed_observation["tag_manufacturer_name"] == "TrapTagger"
    assert transformed_observation["sensor_type"] == "GPS"


@pytest.mark.asyncio
async def test_transform_observations_for_earthranger(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    observation_object_v2,
    destination_integration_v2_er,
    route_config_with_no_mappings,
    connection_v2,
):
    transformed_observation = await transform_observation_v2(
        observation=observation_object_v2,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=route_config_with_no_mappings,
    )
    assert transformed_observation
    assert transformed_observation.manufacturer_id == observation_object_v2.external_source_id
    assert transformed_observation.source_type == observation_object_v2.type
    assert transformed_observation.subject_name == observation_object_v2.source_name
    assert transformed_observation.recorded_at == observation_object_v2.recorded_at
    assert transformed_observation.location.lon == observation_object_v2.location.lon
    assert transformed_observation.location.lat == observation_object_v2.location.lat
    assert transformed_observation.additional == observation_object_v2.additional


@pytest.mark.asyncio
async def test_transform_observations_without_route_configuration(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    observation_object_v2,
    destination_integration_v2_er,
    connection_v2,
):
    transformed_observation = await transform_observation_v2(
        observation=observation_object_v2,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=None,
    )
    assert transformed_observation


@pytest.mark.asyncio
async def test_transform_events_without_route_configuration(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    animals_sign_event_v2,
    destination_integration_v2_er,
    connection_v2,
):
    transformed_observation = await transform_observation_v2(
        observation=animals_sign_event_v2,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=None,
    )
    assert transformed_observation


@pytest.mark.asyncio
async def test_provider_key_mapping_with_default(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    animals_sign_event_v2,
    destination_integration_v2_er,
    route_config_with_provider_key_mappings,
    connection_v2,
):
    # Test with a species that is not in the map
    transformed_observation = await transform_observation_v2(
        observation=animals_sign_event_v2,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=route_config_with_provider_key_mappings,
    )
    # Check that it's mapped to the default type
    assert transformed_observation.provider_key == "mapipedia"


@pytest.mark.asyncio
async def test_provider_key_mapping_in_observations_v2(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    observation_object_v2,
    destination_integration_v2_er,
    route_config_with_provider_key_mappings,
    connection_v2,
):
    # Test with a species that is not in the map
    transformed_observation = await transform_observation_v2(
        observation=observation_object_v2,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=route_config_with_provider_key_mappings,
    )
    # Check that it's mapped to the default type
    assert transformed_observation.provider_key == "mapipedia"


@pytest.mark.asyncio
async def test_provider_key_mapping_in_events_v2(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    leopard_detected_event_v2,
    destination_integration_v2_er,
    route_config_with_provider_key_mappings,
    connection_v2,
):
    # Test with a species that is not in the map
    transformed_observation = await transform_observation_v2(
        observation=leopard_detected_event_v2,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=route_config_with_provider_key_mappings,
    )
    # Check that it's mapped to the provider key
    assert transformed_observation.provider_key == "mapipedia"


@pytest.mark.asyncio
async def test_transform_events_for_smart(
    mocker,
    smart_ca_uuid,
    mock_smart_async_client_class,
    animals_sign_event_v2,
    connection_v2,
    destination_integration_v2_smart,
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient",
        mock_smart_async_client_class,
    )
    transformed_observation = await transform_observation_v2(
        observation=animals_sign_event_v2,
        destination=destination_integration_v2_smart,
        provider=connection_v2.provider,
        route_configuration=None,
    )
    assert transformed_observation
    assert transformed_observation.ca_uuid == smart_ca_uuid
    assert len(transformed_observation.waypoint_requests) == 1
    waypoint = transformed_observation.waypoint_requests[0]
    location = waypoint.geometry.coordinates
    assert location
    assert location == [animals_sign_event_v2.location.lon, animals_sign_event_v2.location.lat]
    properties = waypoint.properties
    assert properties
    assert properties.smartDataType == "incident"
    assert properties.smartFeatureType == "waypoint/new"
    attributes = properties.smartAttributes
    assert attributes
    assert len(attributes.observationGroups) == 1
    observation_group = attributes.observationGroups[0]
    assert len(observation_group.observations) == 1
    observation = observation_group.observations[0]
    assert observation.category == "animals.sign"
    assert observation.attributes == {"ageofsign": "days", "species": "lion"}


@pytest.mark.asyncio
async def test_transform_event_for_smart_raises_on_category_missmatch(
    mocker,
    smart_ca_uuid,
    mock_smart_async_client_class,
    smart_event_v2_with_unmatched_category,
    connection_v2,
    destination_integration_v2_smart_without_transform_rules,
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient",
        mock_smart_async_client_class,
    )
    with pytest.raises(ReferenceDataError):
        transformed_observation = await transform_observation_v2(
            observation=smart_event_v2_with_unmatched_category,
            destination=destination_integration_v2_smart_without_transform_rules,
            provider=connection_v2.provider,
            route_configuration=None,
        )


@pytest.mark.asyncio
async def test_transform_event_update_full_for_smart(
    mocker,
    smart_ca_uuid,
    mock_smart_async_client_class,
    animals_sign_event_update_v2,
    connection_v2,
    destination_integration_v2_smart,
):
    mocker.patch("app.services.transformers.AsyncSmartClient", mock_smart_async_client_class)
    transformed_observation = await transform_observation_v2(
        observation=animals_sign_event_update_v2,
        destination=destination_integration_v2_smart,
        provider=connection_v2.provider,
        route_configuration=None,
    )
    changes = animals_sign_event_update_v2.changes
    gundi_id = str(animals_sign_event_update_v2.gundi_id)
    assert transformed_observation
    assert transformed_observation.ca_uuid == smart_ca_uuid
    assert len(transformed_observation.waypoint_requests) == 2  # When changing event details, two requests are expected
    # Changes in base attributes like title must generate an incident update request
    waypoint_1 = transformed_observation.waypoint_requests[0]
    assert waypoint_1.geometry  # Location is mapped to geometry.coordinates
    location = waypoint_1.geometry.coordinates
    assert location == [changes["location"]["lon"], changes["location"]["lat"]]
    properties = waypoint_1.properties
    assert properties
    assert properties.smartDataType == "incident"
    assert properties.smartFeatureType == "waypoint"  # Incident Update
    # Check that the datetime is correctly converted to local time and formatted (SMART doesn't accept timezone)
    timestamp = datetime.fromisoformat(changes["recorded_at"])
    local_tz = pytz.timezone("Africa/Lagos")
    localized_timestamp = timestamp.astimezone(local_tz)
    smart_timestamp = localized_timestamp.strftime(SMARTCONNECT_DATFORMAT)
    assert properties.dateTime.strftime(SMARTCONNECT_DATFORMAT) == smart_timestamp
    attributes = properties.smartAttributes
    assert attributes
    assert attributes.incidentId == f"gundi_ev_{gundi_id}"
    assert attributes.incidentUuid == gundi_id
    assert changes["title"] in attributes.comment  # Title is mapped to comment
    # Changes in event details must generate a second request with an observation update
    waypoint_2 = transformed_observation.waypoint_requests[1]
    properties = waypoint_2.properties
    assert properties
    assert properties.smartDataType == "incident"
    assert properties.smartFeatureType == "waypoint/observation"  # Observation Update
    assert properties.smartAttributes.category == "animals.sign"
    observation = properties.smartAttributes
    assert observation.observationUuid == gundi_id
    assert observation.attributes == {"species": "puma", "ageofsign": "weeks"}


@pytest.mark.asyncio
async def test_transform_event_update_title_for_smart(
    mocker,
    smart_ca_uuid,
    mock_smart_async_client_class,
    animals_sign_event_update_title_v2,
    connection_v2,
    destination_integration_v2_smart,
):
    mocker.patch("app.services.transformers.AsyncSmartClient", mock_smart_async_client_class)
    transformed_observation = await transform_observation_v2(
        observation=animals_sign_event_update_title_v2,
        destination=destination_integration_v2_smart,
        provider=connection_v2.provider,
        route_configuration=None,
    )
    changes = animals_sign_event_update_title_v2.changes
    gundi_id = str(animals_sign_event_update_title_v2.gundi_id)
    assert transformed_observation
    assert transformed_observation.ca_uuid == smart_ca_uuid
    assert len(transformed_observation.waypoint_requests) == 1
    waypoint_1 = transformed_observation.waypoint_requests[0]
    # Changes in base attributes like title must generate an incident update request
    properties = waypoint_1.properties
    assert properties
    assert properties.smartDataType == "incident"
    assert properties.smartFeatureType == "waypoint"  # Incident Update
    attributes = properties.smartAttributes
    assert attributes
    assert attributes.incidentId == f"gundi_ev_{gundi_id}"
    assert attributes.incidentUuid == gundi_id
    assert changes["title"] in attributes.comment  # Title is mapped to comment


@pytest.mark.asyncio
async def test_transform_event_update_location_for_smart(
    mocker,
    smart_ca_uuid,
    mock_smart_async_client_class,
    animals_sign_event_update_location_v2,
    connection_v2,
    destination_integration_v2_smart,
):
    mocker.patch("app.services.transformers.AsyncSmartClient", mock_smart_async_client_class)
    transformed_observation = await transform_observation_v2(
        observation=animals_sign_event_update_location_v2,
        destination=destination_integration_v2_smart,
        provider=connection_v2.provider,
        route_configuration=None,
    )
    changes = animals_sign_event_update_location_v2.changes
    gundi_id = str(animals_sign_event_update_location_v2.gundi_id)
    assert transformed_observation
    assert transformed_observation.ca_uuid == smart_ca_uuid
    assert len(transformed_observation.waypoint_requests) == 1
    # Changes in base attributes like location must generate an incident update request
    waypoint_1 = transformed_observation.waypoint_requests[0]
    assert waypoint_1.geometry  # Location is mapped to geometry.coordinates
    location = waypoint_1.geometry.coordinates
    assert location == [changes["location"]["lon"], changes["location"]["lat"]]
    properties = waypoint_1.properties
    assert properties
    assert properties.smartDataType == "incident"
    assert properties.smartFeatureType == "waypoint"  # Incident Update
    attributes = properties.smartAttributes
    assert attributes
    assert attributes.incidentId == f"gundi_ev_{gundi_id}"
    assert attributes.incidentUuid == gundi_id


@pytest.mark.asyncio
async def test_transform_event_update_details_for_smart(
    mocker,
    smart_ca_uuid,
    mock_smart_async_client_class,
    animals_sign_event_update_details_v2,
    connection_v2,
    destination_integration_v2_smart,
):
    mocker.patch("app.services.transformers.AsyncSmartClient", mock_smart_async_client_class)
    transformed_observation = await transform_observation_v2(
        observation=animals_sign_event_update_details_v2,
        destination=destination_integration_v2_smart,
        provider=connection_v2.provider,
        route_configuration=None,
    )
    gundi_id = str(animals_sign_event_update_details_v2.gundi_id)
    assert transformed_observation
    assert transformed_observation.ca_uuid == smart_ca_uuid
    assert len(transformed_observation.waypoint_requests) == 1
    # Changes in event details must generate a request with an observation update
    waypoint_1 = transformed_observation.waypoint_requests[0]
    properties = waypoint_1.properties
    assert properties
    assert properties.smartDataType == "incident"
    assert properties.smartFeatureType == "waypoint/observation"  # Observation Update
    assert properties.smartAttributes.category == "animals.sign"
    observation = properties.smartAttributes
    assert observation.observationUuid == gundi_id
    assert observation.attributes == {"species": "leopard"}


@pytest.mark.asyncio
async def test_transform_event_update_details_requires_event_type_for_smart(
    mocker,
    smart_ca_uuid,
    mock_smart_async_client_class,
    animals_sign_event_update_details_without_event_type_v2,
    connection_v2,
    destination_integration_v2_smart,
):
    mocker.patch("app.services.transformers.AsyncSmartClient", mock_smart_async_client_class)
    with pytest.raises(ValueError):
        transformed_observation = await transform_observation_v2(
            observation=animals_sign_event_update_details_without_event_type_v2,
            destination=destination_integration_v2_smart,
            provider=connection_v2.provider,
            route_configuration=None,
        )


@pytest.mark.asyncio
async def test_transform_event_update_with_type_mapping_for_earthranger(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    event_update_species_wildcat,
    destination_integration_v2_er,
    route_config_with_event_type_mappings,
    connection_v2,
):
    transformed_observation = await transform_observation_v2(
        observation=event_update_species_wildcat,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=route_config_with_event_type_mappings,
    )
    assert transformed_observation.changes.get("event_type") == "wild_cat_sighting"


@pytest.mark.asyncio
async def test_transform_event_update_partial_location_with_type_mapping_for_earthranger(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    event_update_location_lon,
    destination_integration_v2_er,
    route_config_with_event_type_mappings,
    connection_v2,
):
    transformed_observation = await transform_observation_v2(
        observation=event_update_location_lon,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=route_config_with_event_type_mappings,
    )
    er_location_changes = {
        "longitude": event_update_location_lon.changes["location"]["lon"]
    }
    assert transformed_observation.changes.get("location") == er_location_changes
    # Field mapping must not be applied as species isn't changed
    assert "event_type" not in transformed_observation.changes


@pytest.mark.asyncio
async def test_transform_event_update_full_location_with_type_mapping_for_earthranger(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    event_update_location_full,
    destination_integration_v2_er,
    route_config_with_event_type_mappings,
    connection_v2,
):
    transformed_observation = await transform_observation_v2(
        observation=event_update_location_full,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=route_config_with_event_type_mappings,
    )
    er_location_changes = {
        "longitude": event_update_location_full.changes["location"]["lon"],
        "latitude": event_update_location_full.changes["location"]["lat"]
    }
    assert transformed_observation.changes.get("location") == er_location_changes
    assert "event_type" not in transformed_observation.changes


@pytest.mark.asyncio
async def test_transform_event_for_wpswatch(
    animals_sign_event_v2,
    connection_v2_traptagger_to_wpswatch,
    destination_integration_v2_wpswatch,
):
    transformed_observation = await transform_observation_v2(
        observation=animals_sign_event_v2,
        destination=destination_integration_v2_wpswatch,
        provider=connection_v2_traptagger_to_wpswatch.provider,
        route_configuration=None,
    )
    assert transformed_observation
    # The external source/device id is used as metadata when uploading images to for WPS Watch
    assert type(transformed_observation) == schemas.v2.WPSWatchImageMetadata
    assert transformed_observation.camera_id == animals_sign_event_v2.external_source_id


@pytest.mark.asyncio
async def test_transform_attachment_for_wpswatch(
    photo_attachment_v2,
    connection_v2_traptagger_to_wpswatch,
    destination_integration_v2_wpswatch,
):
    transformed_observation = await transform_observation_v2(
        observation=photo_attachment_v2,
        destination=destination_integration_v2_wpswatch,
        provider=connection_v2_traptagger_to_wpswatch.provider,
        route_configuration=None,
    )
    assert transformed_observation
    # Attachments are images for WPS Watch
    assert type(transformed_observation) == schemas.v2.WPSWatchImage
    assert transformed_observation.file_path == photo_attachment_v2.file_path
