import pytest
import pytz
from datetime import datetime
from app.conftest import async_return
from app.core.gundi import URN_GUNDI_PREFIX, URN_GUNDI_INTSRC_FORMAT
from app.services.transformers import (
    transform_observation,
    extract_fields_from_message,
    convert_observation_to_cdip_schema,
)


@pytest.mark.asyncio
async def test_movebank_transformer(
    outbound_configuration_default, unprocessed_observation_position
):
    raw_observation, _ = extract_fields_from_message(unprocessed_observation_position)
    observation = convert_observation_to_cdip_schema(
        raw_observation, gundi_version="v1"
    )
    # Set Movebank values
    outbound_configuration_default.type_slug = "movebank"
    transformed_observation = await transform_observation(
        stream_type=observation.observation_type,
        config=outbound_configuration_default,
        observation=observation,
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
    # Check transformed_observation values
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    assert (
        bool(datetime.strptime(transformed_observation["recorded_at"], date_format))
        is True
    )
    assert transformed_observation["recorded_at"] == observation.recorded_at.astimezone(
        tz=pytz.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert (
        transformed_observation["tag_id"]
        == f"{outbound_configuration_default.inbound_type_slug}.{observation.device_id}.{observation.integration_id}"
    )
    assert (
        transformed_observation["gundi_urn"]
        == f"{URN_GUNDI_PREFIX}v1.{URN_GUNDI_INTSRC_FORMAT}.{observation.integration_id}.{observation.device_id}"
    )


@pytest.mark.asyncio
async def test_smart_transformer_with_er_event(
    mocker,
    mock_gundi_client,
    mock_smart_async_client_class,
    smart_outbound_configuration_kafka,
    unprocessed_observation_er_event,
):
    mocker.patch(
        "app.transform_service.smartconnect_transformers.AsyncSmartClient",
        mock_smart_async_client_class,
    )

    # Mock external dependencies
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [smart_outbound_configuration_kafka]
    )

    raw_observation, _ = extract_fields_from_message(unprocessed_observation_er_event)
    assert raw_observation

    event = convert_observation_to_cdip_schema(raw_observation, gundi_version="v1")
    assert event

    transformed_observation = await transform_observation(
        stream_type=event.observation_type,
        config=smart_outbound_configuration_kafka,
        observation=event,
    )
    assert transformed_observation
    assert isinstance(transformed_observation, dict)
    assert "waypoint_requests" in transformed_observation
    assert len(transformed_observation["waypoint_requests"]) == 1
    waypoint = transformed_observation["waypoint_requests"][0]
    assert waypoint.get("type") == "Feature"
    assert waypoint.get("geometry") == {
        "coordinates": [event.location.longitude, event.location.latitude]
    }
    waypoint_properties = waypoint.get("properties")
    assert waypoint_properties.get("smartDataType") == "integrateincident"
    assert waypoint_properties.get("smartFeatureType") == "waypoint/new"
    smart_attributes = waypoint_properties.get("smartAttributes")
    assert smart_attributes.get("incidentId") == f"ER-{event.serial_number}"
    assert smart_attributes.get("incidentUuid") == str(event.id)
    observations_groups = smart_attributes.get("observationGroups", [])
    assert len(observations_groups) == 1
    observations = observations_groups[0].get("observations", [])
    assert len(observations) == 1
    observation = observations[0]
    assert observation.get("category") == "animals.sign"
    assert observation.get("observationUuid") == str(event.id)


@pytest.mark.asyncio
async def test_smart_transformer_with_er_patrol(
    mocker,
    mock_gundi_client,
    mock_smart_async_client_class,
    smart_outbound_configuration_kafka,
    unprocessed_observation_er_patrol,
):
    mocker.patch(
        "app.transform_service.smartconnect_transformers.AsyncSmartClient",
        mock_smart_async_client_class,
    )

    # Mock external dependencies
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [smart_outbound_configuration_kafka]
    )

    raw_observation, _ = extract_fields_from_message(unprocessed_observation_er_patrol)
    assert raw_observation

    patrol = convert_observation_to_cdip_schema(raw_observation, gundi_version="v1")
    assert patrol

    transformed_observation = await transform_observation(
        stream_type=patrol.observation_type,
        config=smart_outbound_configuration_kafka,
        observation=patrol,
    )
    assert transformed_observation
    assert isinstance(transformed_observation, dict)
    assert "patrol_requests" in transformed_observation
    assert len(transformed_observation["patrol_requests"]) == 1
    patrol_request = transformed_observation["patrol_requests"][0]
    assert patrol_request.get("type") == "Feature"
    patrol_properties = patrol_request.get("properties")
    assert patrol_properties.get("smartDataType") == "patrol"
    assert patrol_properties.get("smartFeatureType") == "patrol/new"
    smart_attributes = patrol_properties.get("smartAttributes")
    assert smart_attributes.get("patrolId") == f"ER-{patrol.serial_number}"
    assert smart_attributes.get("patrolUuid") == str(patrol.id)
