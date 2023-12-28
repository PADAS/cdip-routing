import pytest
import pytz
from datetime import datetime

from app.transform_service.services import transform_observation
from app.transform_service import helpers
from app.subscribers.services import extract_fields_from_message, convert_observation_to_cdip_schema


@pytest.mark.asyncio
async def test_movebank_transformer(
    outbound_configuration_default,
    unprocessed_observation_position
):
    raw_observation, _ = extract_fields_from_message(unprocessed_observation_position)
    observation = convert_observation_to_cdip_schema(raw_observation, gundi_version="v1")
    # Set Movebank values
    outbound_configuration_default.type_slug = "movebank"
    transformed_observation = await transform_observation(
        stream_type=observation.observation_type,
        config=outbound_configuration_default,
        observation=observation
    )
    # Check dictionary schema
    expected_keys = ["recorded_at", "tag_id", "lon", "lat", "sensor_type", "tag_manufacturer_name", "gundi_urn"]
    for key in transformed_observation.keys():
        assert key in expected_keys
    # Check transformed_observation values
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    assert bool(datetime.strptime(transformed_observation["recorded_at"], date_format)) is True
    assert transformed_observation["recorded_at"] == observation.recorded_at.astimezone(tz=pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    assert transformed_observation["tag_id"] == \
           f'{outbound_configuration_default.inbound_type_slug}.{observation.device_id}.{observation.integration_id}'
    assert transformed_observation["gundi_urn"] == \
           f'{helpers.URN_GUNDI_PREFIX}v1.{helpers.URN_GUNDI_INTSRC_FORMAT}.{observation.integration_id}.{observation.device_id}'
