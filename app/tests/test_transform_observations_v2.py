import os
from datetime import datetime
import httpx
import json
import pytest
import pytz
from smartconnect.models import SMARTCONNECT_DATFORMAT, ConfigurableDataModel
from gundi_core import schemas
from app.conftest import async_return
from app.core.errors import ReferenceDataError
from app.services.transformers import (
    SmartEventTransformerV2,
    _timezone_finder,
    transform_observation_v2,
)


@pytest.mark.asyncio
async def test_event_type_mapping(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub,
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
    mock_pubsub,
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
    mock_pubsub,
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
    assert (
        transformed_observation.event_type
        == event_v2_with_empty_event_details.event_type
    )


@pytest.mark.asyncio
async def test_movebank_transform_observation(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub,
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
    mock_pubsub,
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
    assert (
        transformed_observation.manufacturer_id
        == observation_object_v2.external_source_id
    )
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
    mock_pubsub,
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
    mock_pubsub,
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
async def test_transform_events_with_status_for_earthranger(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub,
    animals_sign_event_with_status,
    destination_integration_v2_er,
    connection_v2,
):
    transformed_observation = await transform_observation_v2(
        observation=animals_sign_event_with_status,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=None,
    )
    assert transformed_observation
    assert transformed_observation.state
    assert transformed_observation.state == animals_sign_event_with_status.status


@pytest.mark.asyncio
async def test_transform_event_with_null_location_for_earthranger(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub,
    animals_sign_event_v2_with_null_location,
    destination_integration_v2_er,
    connection_v2,
):
    transformed_observation = await transform_observation_v2(
        observation=animals_sign_event_v2_with_null_location,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=None,
    )
    assert transformed_observation
    # Location is set to None when null
    assert transformed_observation.location is None


@pytest.mark.asyncio
async def test_provider_key_mapping_with_default(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub,
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
    mock_pubsub,
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
    mock_pubsub,
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
    assert location == [
        animals_sign_event_v2.location.lon,
        animals_sign_event_v2.location.lat,
    ]
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
async def test_transform_event_with_null_location_for_smart(
    mocker,
    smart_ca_uuid,
    mock_smart_async_client_class,
    animals_sign_event_v2_with_null_location,
    connection_v2,
    destination_integration_v2_smart,
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient",
        mock_smart_async_client_class,
    )
    transformed_observation = await transform_observation_v2(
        observation=animals_sign_event_v2_with_null_location,
        destination=destination_integration_v2_smart,
        provider=connection_v2.provider,
        route_configuration=None,
    )
    assert transformed_observation
    assert transformed_observation.ca_uuid == smart_ca_uuid
    assert len(transformed_observation.waypoint_requests) == 1
    waypoint = transformed_observation.waypoint_requests[0]
    location = waypoint.geometry.coordinates
    # Location is set to [0, 0] when null
    assert location == [0, 0]
    properties = waypoint.properties
    assert properties
    assert properties.smartDataType == "incident"
    assert properties.smartFeatureType == "waypoint/new"
    attributes = properties.smartAttributes
    assert attributes
    assert len(attributes.observationGroups) == 1
    observation = attributes.observationGroups[0].observations[0]
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
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
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
    assert (
        len(transformed_observation.waypoint_requests) == 2
    )  # When changing event details, two requests are expected
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
@pytest.mark.parametrize(
    "recorded_at", ["2024-08-05T13:27:10.463000Z", "2024-08-05T13:27:10Z"]
)
async def test_transform_event_update_utc_recorded_at_for_smart(
    mocker,
    mock_smart_async_client_class,
    animals_sign_event_update_v2,
    connection_v2,
    destination_integration_v2_smart,
    recorded_at,
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
    animals_sign_event_update_v2.changes["recorded_at"] = recorded_at

    transformed_observation = await transform_observation_v2(
        observation=animals_sign_event_update_v2,
        destination=destination_integration_v2_smart,
        provider=connection_v2.provider,
        route_configuration=None,
    )

    expected = datetime(2024, 8, 5, 13, 27, 10, tzinfo=pytz.utc).astimezone(
        pytz.timezone("Africa/Lagos")
    )
    properties = transformed_observation.waypoint_requests[0].properties
    assert properties.dateTime.strftime(SMARTCONNECT_DATFORMAT) == expected.strftime(
        SMARTCONNECT_DATFORMAT
    )


@pytest.mark.asyncio
async def test_transform_event_update_title_for_smart(
    mocker,
    smart_ca_uuid,
    mock_smart_async_client_class,
    animals_sign_event_update_title_v2,
    connection_v2,
    destination_integration_v2_smart,
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
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
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
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
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
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
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
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
    mock_pubsub,
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
    mock_pubsub,
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
    mock_pubsub,
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
        "latitude": event_update_location_full.changes["location"]["lat"],
    }
    assert transformed_observation.changes.get("location") == er_location_changes
    assert "event_type" not in transformed_observation.changes


@pytest.mark.asyncio
async def test_transform_event_update_status_resolved_for_earthranger(
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub,
    event_update_status_resolved,
    destination_integration_v2_er,
    connection_v2,
):
    transformed_observation = await transform_observation_v2(
        observation=event_update_status_resolved,
        destination=destination_integration_v2_er,
        provider=connection_v2.provider,
        route_configuration=None,
    )
    assert transformed_observation.changes
    assert transformed_observation.changes.get("state") == "resolved"


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
    assert isinstance(transformed_observation, schemas.v2.WPSWatchImageMetadata)
    assert transformed_observation.camera_id == animals_sign_event_v2.external_source_id


@pytest.mark.asyncio
async def test_transform_attachment_for_wpswatch(
    photo_attachment_v2_jpg,
    connection_v2_traptagger_to_wpswatch,
    destination_integration_v2_wpswatch,
):
    transformed_observation = await transform_observation_v2(
        observation=photo_attachment_v2_jpg,
        destination=destination_integration_v2_wpswatch,
        provider=connection_v2_traptagger_to_wpswatch.provider,
        route_configuration=None,
    )
    assert transformed_observation
    # Attachments are images for WPS Watch
    assert isinstance(transformed_observation, schemas.v2.WPSWatchImage)
    assert transformed_observation.file_path == photo_attachment_v2_jpg.file_path


@pytest.mark.asyncio
async def test_transform_attachment_for_wpswatch_with_invalid_format(
    photo_attachment_v2_svg,
    connection_v2_traptagger_to_wpswatch,
    destination_integration_v2_wpswatch,
):
    with pytest.raises(ValueError):
        await transform_observation_v2(
            observation=photo_attachment_v2_svg,
            destination=destination_integration_v2_wpswatch,
            provider=connection_v2_traptagger_to_wpswatch.provider,
            route_configuration=None,
        )


@pytest.mark.asyncio
async def test_transform_event_for_traptagger(
    animals_sign_event_v2,
    connection_v2_wpswatch_to_traptagger,
    destination_integration_v2_traptagger,
):
    transformed_observation = await transform_observation_v2(
        observation=animals_sign_event_v2,
        destination=destination_integration_v2_traptagger,
        provider=connection_v2_wpswatch_to_traptagger.provider,
        route_configuration=None,
    )
    assert transformed_observation
    assert isinstance(transformed_observation, schemas.v2.TrapTaggerImageMetadata)
    assert transformed_observation.camera == animals_sign_event_v2.external_source_id
    assert transformed_observation.latitude == str(animals_sign_event_v2.location.lat)
    assert transformed_observation.longitude == str(animals_sign_event_v2.location.lon)
    assert (
        transformed_observation.timestamp
        == animals_sign_event_v2.recorded_at.strftime("%Y-%m-%d %H:%M:%S")
    )


@pytest.mark.asyncio
async def test_transform_event_with_null_location_for_traptagger(
    animals_sign_event_v2_with_null_location,
    connection_v2_wpswatch_to_traptagger,
    destination_integration_v2_traptagger,
):
    transformed_observation = await transform_observation_v2(
        observation=animals_sign_event_v2_with_null_location,
        destination=destination_integration_v2_traptagger,
        provider=connection_v2_wpswatch_to_traptagger.provider,
        route_configuration=None,
    )
    assert transformed_observation
    assert isinstance(transformed_observation, schemas.v2.TrapTaggerImageMetadata)
    assert (
        transformed_observation.camera
        == animals_sign_event_v2_with_null_location.external_source_id
    )
    assert transformed_observation.latitude == "0.0"
    assert transformed_observation.longitude == "0.0"
    assert (
        transformed_observation.timestamp
        == animals_sign_event_v2_with_null_location.recorded_at.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )


@pytest.mark.asyncio
async def test_transform_attachment_for_traptagger(
    photo_attachment_v2_jpg,
    connection_v2_wpswatch_to_traptagger,
    destination_integration_v2_traptagger,
):
    transformed_observation = await transform_observation_v2(
        observation=photo_attachment_v2_jpg,
        destination=destination_integration_v2_traptagger,
        provider=connection_v2_wpswatch_to_traptagger.provider,
        route_configuration=None,
    )
    assert transformed_observation
    # Attachments are images for WPS Watch
    assert isinstance(transformed_observation, schemas.v2.TrapTaggerImage)
    assert transformed_observation.file_path == photo_attachment_v2_jpg.file_path


@pytest.mark.parametrize(
    "attachment",  # Only jpg is supported by TrapTagger
    [
        "photo_attachment_v2_svg",
        "photo_attachment_v2_png",
        "photo_attachment_v2_gif",
        "photo_attachment_v2_bmp",
    ],
)
@pytest.mark.asyncio
async def test_transform_attachment_for_traptagger_with_invalid_format(
    request,
    attachment,
    connection_v2_wpswatch_to_traptagger,
    destination_integration_v2_traptagger,
):
    attachment = request.getfixturevalue(attachment)
    with pytest.raises(ValueError):
        await transform_observation_v2(
            observation=attachment,
            destination=destination_integration_v2_traptagger,
            provider=connection_v2_wpswatch_to_traptagger.provider,
            route_configuration=None,
        )


@pytest.mark.asyncio
async def test_transform_attachment_for_smart(
    photo_attachment_v2_jpg,
    connection_v2_traptagger_to_smart,
    destination_integration_v2_smart,
):
    transformed_observation = await transform_observation_v2(
        observation=photo_attachment_v2_jpg,
        destination=destination_integration_v2_smart,
        provider=connection_v2_traptagger_to_smart.provider,
        route_configuration=None,
    )
    assert transformed_observation
    # Attachments added to SMART incidents by updating the waypoint attachments
    assert type(transformed_observation) == schemas.v2.SMARTUpdateRequest
    assert transformed_observation.waypoint_requests
    assert len(transformed_observation.waypoint_requests) == 1
    attachment = transformed_observation.waypoint_requests[
        0
    ].properties.smartAttributes.attachments[0]
    assert attachment.filename == os.path.basename(photo_attachment_v2_jpg.file_path)
    assert attachment.data == f"gundi:storage:{photo_attachment_v2_jpg.file_path}"


@pytest.mark.asyncio
async def test_transform_text_message_for_earthranger(
    text_message_from_inreach,
    connection_v2_inreach_to_er,
    destination_integration_v2,
):
    transformed_observation = await transform_observation_v2(
        observation=text_message_from_inreach,
        destination=destination_integration_v2,
        provider=connection_v2_inreach_to_er.provider,
        route_configuration=None,
    )
    assert transformed_observation
    assert isinstance(transformed_observation, schemas.v2.ERMessage)
    assert transformed_observation.message_type == "inbox"
    assert transformed_observation.manufacturer_id == text_message_from_inreach.sender
    assert transformed_observation.text == text_message_from_inreach.text
    assert transformed_observation.message_time == text_message_from_inreach.created_at
    assert transformed_observation.device_location
    assert (
        transformed_observation.device_location.longitude
        == text_message_from_inreach.location.lon
    )
    assert (
        transformed_observation.device_location.latitude
        == text_message_from_inreach.location.lat
    )
    assert transformed_observation.additional == text_message_from_inreach.additional


@pytest.mark.asyncio
async def test_transform_text_message_for_inreach(
    text_message_from_earthranger,
    connection_v2_er_to_inreach,
    destination_integration_v2_inreach,
):
    transformed_observation = await transform_observation_v2(
        observation=text_message_from_earthranger,
        destination=destination_integration_v2_inreach,
        provider=connection_v2_er_to_inreach.provider,
        route_configuration=None,
    )
    assert transformed_observation
    assert isinstance(transformed_observation, schemas.v2.InReachIPCMessage)
    assert transformed_observation.Message == text_message_from_earthranger.text
    assert transformed_observation.Sender == text_message_from_earthranger.sender
    assert (
        transformed_observation.Recipients == text_message_from_earthranger.recipients
    )
    assert transformed_observation.Timestamp == text_message_from_earthranger.created_at


@pytest.mark.asyncio
async def test_smart_transformer_v2_resolves_v2_choice_values(
    mocker, mock_smart_async_client_class, destination_integration_v2_smart
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
    transformer = SmartEventTransformerV2(config=destination_integration_v2_smart)
    assert await transformer._resolve_attribute("causeofdeath", "natural_disease") == (
        "causeofdeath",
        "natural.disease",
    )
    assert await transformer._resolve_attribute("wildlifestate", "healthy") == (
        "wildlifestate",
        "healthy",
    )


def _multi_config_cm():
    # Attribute key absent from the CA data model, so only the CM's configs
    # can resolve it.
    xml = (
        '<ConfigurableModel xmlns="http://www.smartconservationsoftware.org/xml/1.0/configurablemodel">'
        '<name value="Test CM" language_code="en"/><nodes/>'
        '<attributeConfig id="cfg-1" attributeKey="cmonlystate" isDefault="true">'
        '<listItem keyRef="Healthy" isActive="true"/></attributeConfig>'
        '<attributeConfig id="cfg-2" attributeKey="cmonlystate" isDefault="false">'
        '<listItem keyRef="Injured" isActive="true"/></attributeConfig>'
        "</ConfigurableModel>"
    )
    cm = ConfigurableDataModel()
    cm.load(xml)
    return cm


@pytest.mark.asyncio
async def test_smart_transformer_v2_resolves_across_all_attribute_configs(
    mocker, mock_smart_async_client_class, destination_integration_v2_smart
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
    transformer = SmartEventTransformerV2(config=destination_integration_v2_smart)
    mocker.patch.object(
        transformer, "get_configurable_models", return_value=[_multi_config_cm()]
    )
    assert await transformer._resolve_attribute("cmonlystate", "injured") == (
        "cmonlystate",
        "Injured",
    )


@pytest.mark.asyncio
async def test_smart_transformer_v2_survives_ca_datamodel_outage_when_cm_matches(
    mocker, mock_smart_async_client_class, destination_integration_v2_smart
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
    transformer = SmartEventTransformerV2(config=destination_integration_v2_smart)
    mocker.patch.object(
        transformer, "get_configurable_models", return_value=[_multi_config_cm()]
    )
    mocker.patch.object(
        transformer, "get_ca_datamodel", side_effect=ReferenceDataError("down")
    )
    assert await transformer._resolve_attribute("cmonlystate", "injured") == (
        "cmonlystate",
        "Injured",
    )


def _two_attribute_cm():
    xml = (
        '<ConfigurableModel xmlns="http://www.smartconservationsoftware.org/xml/1.0/configurablemodel">'
        '<name value="Test CM" language_code="en"/><nodes/>'
        '<attributeConfig id="cfg-1" attributeKey="cmonlystate" isDefault="true">'
        '<listItem keyRef="Healthy" isActive="true"/></attributeConfig>'
        '<attributeConfig id="cfg-2" attributeKey="cmonlyother" isDefault="true">'
        '<listItem keyRef="Injured" isActive="true"/></attributeConfig>'
        "</ConfigurableModel>"
    )
    cm = ConfigurableDataModel()
    cm.load(xml)
    return cm


@pytest.mark.asyncio
async def test_smart_transformer_v2_fetches_ca_datamodel_once_after_outage(
    mocker, mock_smart_async_client_class, destination_integration_v2_smart
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
    transformer = SmartEventTransformerV2(config=destination_integration_v2_smart)
    mocker.patch.object(
        transformer, "get_configurable_models", return_value=[_two_attribute_cm()]
    )
    get_ca = mocker.patch.object(
        transformer, "get_ca_datamodel", side_effect=ReferenceDataError("down")
    )
    assert await transformer._resolve_attribute("cmonlystate", "healthy") == (
        "cmonlystate",
        "Healthy",
    )
    assert await transformer._resolve_attribute("cmonlyother", "injured") == (
        "cmonlyother",
        "Injured",
    )
    assert get_ca.call_count == 1


def test_smart_transformer_v2_reuses_timezone_finder(
    mocker, mock_smart_async_client_class, destination_integration_v2_smart
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
    transformer = SmartEventTransformerV2(config=destination_integration_v2_smart)
    _timezone_finder.cache_clear()
    for _ in range(2):
        tz = transformer.guess_location_timezone(longitude=36.82, latitude=-1.29)
        assert tz.zone == "Africa/Nairobi"
    assert _timezone_finder.cache_info().misses <= 1


# Routing-level regressions: ER v2 stores choice selections as sanitized values
# (causeofdeath "natural.disease" -> "natural_disease"); SMART must receive the
# original option key in the published request.
@pytest.mark.asyncio
async def test_transform_event_v2_publishes_smart_option_key(
    mocker,
    mock_smart_async_client_class,
    animals_sign_event_v2,
    connection_v2,
    destination_integration_v2_smart,
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
    event = animals_sign_event_v2.copy(
        update={"event_details": {"species": "lion", "causeofdeath": "natural_disease"}}
    )

    transformed = await transform_observation_v2(
        observation=event,
        destination=destination_integration_v2_smart,
        provider=connection_v2.provider,
        route_configuration=None,
    )

    observation = transformed.waypoint_requests[0].properties.smartAttributes.observationGroups[0].observations[0]
    assert observation.attributes == {"species": "lion", "causeofdeath": "natural.disease"}


@pytest.mark.asyncio
async def test_transform_event_update_v2_publishes_smart_option_key(
    mocker,
    mock_smart_async_client_class,
    animals_sign_event_update_v2,
    connection_v2,
    destination_integration_v2_smart,
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
    changes = dict(animals_sign_event_update_v2.changes)
    changes["event_details"] = {"species": "puma", "causeofdeath": "natural_disease"}
    update = animals_sign_event_update_v2.copy(update={"changes": changes})

    transformed = await transform_observation_v2(
        observation=update,
        destination=destination_integration_v2_smart,
        provider=connection_v2.provider,
        route_configuration=None,
    )

    observation_update = transformed.waypoint_requests[1].properties.smartAttributes
    assert observation_update.attributes == {"species": "puma", "causeofdeath": "natural.disease"}


@pytest.mark.asyncio
async def test_transform_event_v2_resolves_from_configurable_model_during_ca_outage(
    mocker,
    mock_smart_async_client,
    mock_smart_async_client_class,
    animals_sign_event_v2,
    connection_v2,
    destination_integration_v2_smart,
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
    destination = destination_integration_v2_smart.copy(deep=True)
    for configuration in destination.configurations:
        if configuration.action.value == "push_events":
            configuration.data["configurable_models_enabled"] = ["7c9f0a3e-1b2d-4e5f-8a6b-9c0d1e2f3a4b"]
    # A CM that carries the event's category and the attribute, so neither
    # needs the CA data model.
    xml = (
        '<ConfigurableModel xmlns="http://www.smartconservationsoftware.org/xml/1.0/configurablemodel">'
        '<name value="Test CM" language_code="en"/><nodes>'
        '<node id="n-1" key="sign" categoryKey="sign" categoryHkey="animals.sign.">'
        '<name value="Sign" language_code="en"/></node></nodes>'
        '<attributeConfig id="cfg-1" attributeKey="cmonlystate" isDefault="true">'
        '<listItem keyRef="Injured" isActive="true"/></attributeConfig>'
        "</ConfigurableModel>"
    )
    cm = ConfigurableDataModel()
    cm.load(xml)
    mock_smart_async_client.get_configurable_data_model.return_value = async_return(cm)
    mock_smart_async_client.get_data_model.side_effect = Exception("CA data model down")
    event = animals_sign_event_v2.copy(update={"event_details": {"cmonlystate": "injured"}})

    transformed = await transform_observation_v2(
        observation=event,
        destination=destination,
        provider=connection_v2.provider,
        route_configuration=None,
    )

    observation = transformed.waypoint_requests[0].properties.smartAttributes.observationGroups[0].observations[0]
    assert observation.attributes == {"cmonlystate": "Injured"}


@pytest.mark.parametrize(
    "base_url,expected_api",
    [
        ("https://smart.example.org", "https://smart.example.org/server"),
        ("https://smart.example.org/server", "https://smart.example.org/server"),
        ("https://smart.example.org/server/", "https://smart.example.org/server/"),
        ("https://smart.example.org:8443", "https://smart.example.org:8443/server"),
        (
            "https://smart.example.org:443/server",
            "https://smart.example.org:443/server",
        ),
    ],
)
def test_smart_transformer_v2_builds_api_url_from_base_url(
    mocker,
    mock_smart_async_client_class,
    destination_integration_v2_smart,
    base_url,
    expected_api,
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
    destination = destination_integration_v2_smart.copy(update={"base_url": base_url})
    SmartEventTransformerV2(config=destination)
    assert mock_smart_async_client_class.call_args.kwargs["api"] == expected_api


@pytest.mark.parametrize(
    "base_url",
    [
        None,
        "",
        "   ",
        "smart.example.org/server",
        "https:///server",
        "ftp://smart.example.org",
    ],
)
def test_smart_transformer_v2_rejects_invalid_base_url(
    mocker, mock_smart_async_client_class, destination_integration_v2_smart, base_url
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
    destination = destination_integration_v2_smart.copy(update={"base_url": base_url})
    with pytest.raises(ValueError) as excinfo:
        SmartEventTransformerV2(config=destination)
    message = str(excinfo.value)
    assert "base_url" in message
    assert str(destination.id) in message
    assert destination.name in message
    assert not isinstance(excinfo.value, ReferenceDataError)
    mock_smart_async_client_class.assert_not_called()


# SMART client reuse (GUNDI-5846 / GUNDI-5850): one client and login session
# per destination config instead of one per observation, closed when replaced.
def _smart_client_factory(mocker, smart_ca_data_model, first_get_data_model=None):
    created = []

    def build(**kwargs):
        client = mocker.MagicMock()
        client.get_data_model = (
            first_get_data_model
            if first_get_data_model and not created
            else mocker.AsyncMock(return_value=smart_ca_data_model)
        )
        client.close = mocker.AsyncMock()
        created.append(client)
        return client

    client_class = mocker.MagicMock(side_effect=build)
    client_class.created = created
    return client_class


def _with_auth(destination, **auth_changes):
    destination = destination.copy(deep=True)
    for configuration in destination.configurations:
        if configuration.action.value == "auth":
            configuration.data.update(auth_changes)
    return destination


@pytest.mark.asyncio
async def test_smart_transforms_reuse_one_client_per_destination(
    mocker,
    smart_ca_data_model,
    animals_sign_event_v2,
    connection_v2,
    destination_integration_v2_smart,
):
    client_class = _smart_client_factory(mocker, smart_ca_data_model)
    mocker.patch("app.services.transformers.AsyncSmartClient", client_class)

    for _ in range(3):
        transformed = await transform_observation_v2(
            observation=animals_sign_event_v2,
            destination=destination_integration_v2_smart,
            provider=connection_v2.provider,
            route_configuration=None,
        )
        assert transformed.waypoint_requests

    assert client_class.call_count == 1
    assert not client_class.return_value.close.called


@pytest.mark.asyncio
async def test_smart_client_is_replaced_and_closed_when_destination_config_changes(
    mocker,
    smart_ca_data_model,
    animals_sign_event_v2,
    connection_v2,
    destination_integration_v2_smart,
):
    client_class = _smart_client_factory(mocker, smart_ca_data_model)
    mocker.patch("app.services.transformers.AsyncSmartClient", client_class)

    await transform_observation_v2(
        observation=animals_sign_event_v2,
        destination=destination_integration_v2_smart,
        provider=connection_v2.provider,
        route_configuration=None,
    )
    await transform_observation_v2(
        observation=animals_sign_event_v2,
        destination=_with_auth(destination_integration_v2_smart, password="rotated"),
        provider=connection_v2.provider,
        route_configuration=None,
    )

    assert client_class.call_count == 2
    assert client_class.call_args.kwargs["password"] == "rotated"
    old_client, new_client = client_class.created
    old_client.close.assert_awaited_once()
    assert not new_client.close.called


@pytest.mark.asyncio
async def test_smart_client_is_replaced_and_closed_after_ttl(
    mocker,
    smart_ca_data_model,
    smart_client_pool,
    animals_sign_event_v2,
    connection_v2,
    destination_integration_v2_smart,
):
    client_class = _smart_client_factory(mocker, smart_ca_data_model)
    mocker.patch("app.services.transformers.AsyncSmartClient", client_class)
    clock = mocker.patch(
        "app.services.smart_clients.time.monotonic", return_value=1000.0
    )

    await transform_observation_v2(
        observation=animals_sign_event_v2,
        destination=destination_integration_v2_smart,
        provider=connection_v2.provider,
        route_configuration=None,
    )
    clock.return_value = 1000.0 + smart_client_pool.ttl
    await transform_observation_v2(
        observation=animals_sign_event_v2,
        destination=destination_integration_v2_smart,
        provider=connection_v2.provider,
        route_configuration=None,
    )

    assert client_class.call_count == 2
    old_client, new_client = client_class.created
    old_client.close.assert_awaited_once()
    assert not new_client.close.called


@pytest.mark.asyncio
async def test_smart_data_model_failure_is_briefly_cached(
    mocker,
    smart_ca_data_model,
    smart_client_pool,
    animals_sign_event_v2,
    connection_v2,
    destination_integration_v2_smart,
):
    failing = mocker.AsyncMock(
        side_effect=Exception("Failed to login to SMART Connect")
    )
    client_class = _smart_client_factory(
        mocker, smart_ca_data_model, first_get_data_model=failing
    )
    mocker.patch("app.services.transformers.AsyncSmartClient", client_class)
    clock = mocker.patch(
        "app.services.smart_clients.time.monotonic", return_value=1000.0
    )

    for _ in range(5):
        with pytest.raises(ReferenceDataError):
            await transform_observation_v2(
                observation=animals_sign_event_v2,
                destination=destination_integration_v2_smart,
                provider=connection_v2.provider,
                route_configuration=None,
            )

    # One attempt against SMART, then fail fast; the session that failed is closed.
    assert failing.await_count == 1
    failed_client = client_class.created[0]
    failed_client.close.assert_awaited_once()

    # Once the failure expires, a fresh client tries again.
    clock.return_value = 1000.0 + smart_client_pool.failure_ttl
    transformed = await transform_observation_v2(
        observation=animals_sign_event_v2,
        destination=destination_integration_v2_smart,
        provider=connection_v2.provider,
        route_configuration=None,
    )
    assert transformed.waypoint_requests


@pytest.mark.asyncio
async def test_smart_client_pool_is_bounded(mocker, smart_ca_data_model):
    from app.services.smart_clients import SmartClientPool

    pool = SmartClientPool(ttl=60, max_size=2, failure_ttl=30)
    client_class = _smart_client_factory(mocker, smart_ca_data_model)
    for destination_id in range(5):
        client = pool.acquire(
            client_class,
            destination_id=destination_id,
            api="https://smart.example.org/server",
            username="u",
            password="p",
            version="7.5",
        )
        await pool.release(client)

    clients = client_class.created
    assert len(clients) == 5
    assert sum(1 for c in clients if not c.close.called) == 2


@pytest.mark.asyncio
async def test_smart_transforms_do_not_log_in_on_data_model_cache_hit(
    mocker,
    smart_ca_data_model,
    animals_sign_event_v2,
    connection_v2,
    destination_integration_v2_smart,
):
    requests, logins = [], []

    def smart_connect(request):
        requests.append(request)
        path = request.url.path
        if path.endswith("/connect/home"):
            return httpx.Response(
                200, headers={"set-cookie": "JSESSIONID=preauth; Path=/"}
            )
        if path.endswith("/j_security_check"):
            logins.append(request)
            return httpx.Response(
                303, headers={"set-cookie": "JSESSIONID=auth; Path=/"}
            )
        return httpx.Response(404)

    mocker.patch(
        "smartconnect.async_client.httpx.AsyncHTTPTransport",
        side_effect=lambda **kwargs: httpx.MockTransport(smart_connect),
    )
    # The data model is served from smartconnect's Redis cache, as in prod.
    smart_cache = mocker.MagicMock()
    smart_cache.cache.get.return_value = json.dumps(
        smart_ca_data_model.export_as_dict()
    )
    mocker.patch("smartconnect.async_client.cache", smart_cache)

    for _ in range(3):
        transformed = await transform_observation_v2(
            observation=animals_sign_event_v2,
            destination=destination_integration_v2_smart,
            provider=connection_v2.provider,
            route_configuration=None,
        )
        assert transformed.waypoint_requests

    # smartconnect-client >= 1.16 serves a cached data model without logging
    # in (<= 1.15 logged in once per session first), and the pooled session
    # means nothing else talks to SMART here either.
    assert logins == []
    assert requests == []
