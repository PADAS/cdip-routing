import pytest
from app.transform_service.services import transform_observation
from app.transform_service import helpers
from app.subscribers.services import extract_fields_from_message, convert_observation_to_cdip_schema


def test_movebank_transformer(
    outbound_configuration_default,
    unprocessed_observation_position
):
    raw_observation, _ = extract_fields_from_message(unprocessed_observation_position)
    observation = convert_observation_to_cdip_schema(raw_observation, gundi_version="v1")
    # Set Movebank values
    outbound_configuration_default.type_slug = "movebank"
    transformed_observation = transform_observation(
        stream_type=observation.observation_type,
        config=outbound_configuration_default,
        observation=observation
    )
    # Check dictionary schema
    expected_keys = ["recorded_at", "tag_id", "lon", "lat", "sensor_type", "tag_manufacturer_id", "gundi_urn"]
    for key in transformed_observation.keys():
        assert key in expected_keys

    assert transformed_observation["tag_id"] == \
           f'{outbound_configuration_default.inbound_type_slug}.{observation.device_id}.{observation.integration_id}'
    assert transformed_observation["gundi_urn"] == \
           f'{helpers.URN_GUNDI_PREFIX}v1.{helpers.URN_GUNDI_INTSRC_FORMAT}.{observation.integration_id}.{observation.device_id}'
