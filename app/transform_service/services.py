import json
import logging
from typing import List
from uuid import UUID

import aiohttp
import requests
import walrus
from cdip_connector.core import schemas, portal_api

from app import settings
from app.core.utils import get_auth_header, create_cache_key, get_redis_db
from app.transform_service.smartconnect_transformers import SmartEREventTransformer
from app.transform_service.transformers import ERPositionTransformer, ERGeoEventTransformer, ERCameraTrapTransformer

logger = logging.getLogger(__name__)

DEFAULT_LOCATION = schemas.Location(x=0.0, y=0.0)


def get_all_outbound_configs_for_id(destinations_cache_db: walrus.Database, inbound_id: UUID) -> List[schemas.OutboundConfiguration]:

    outbound_integrations_endpoint = settings.PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT
    hashable = f'{outbound_integrations_endpoint}/{str(inbound_id)}'
    cache_key = create_cache_key(hashable)
    resp_json_bytes = destinations_cache_db.get(cache_key)

    if resp_json_bytes:
        resp_json_str = resp_json_bytes.decode('utf-8')
    else:
        headers = get_auth_header()
        resp = requests.get(url=f'{outbound_integrations_endpoint}',
                            params=dict(inbound_id=inbound_id),
                            headers=headers, verify=settings.PORTAL_SSL_VERIFY)
        resp.raise_for_status()
        resp_json = resp.json()
        resp_json_str = json.dumps(resp_json)
        # only cache if we receive results as its possible this is not mapped in portal yet
        if resp_json:
            destinations_cache_db.setex(cache_key, settings.REDIS_CHECK_SECONDS, resp_json_str)

    resp_json = json.loads(resp_json_str)
    resp_json = [resp_json] if isinstance(resp_json, dict) else resp_json
    configs, errors = schemas.get_validated_objects(resp_json, schemas.OutboundConfiguration)
    if errors:
        logger.warning(f'{len(errors)} outbound configs have validation errors. {errors}')
    if not configs:
        logger.warning(f'No destinations were found for inbound integration: {inbound_id}')
    return configs


async def update_observation_with_device_configuration(observation):
    device = await ensure_device_integration(observation.integration_id, observation.device_id)
    if device:
        if hasattr(observation, 'location') and observation.location == DEFAULT_LOCATION:
            if device.additional and device.additional.location:
                default_location = device.additional.location
                observation.location = default_location
            else:
                logger.warning(
                    f"No default location found for device {observation.device_id} with unspecified location")

        # add admin portal configured name to title for water meter geo events
        if isinstance(observation, schemas.GeoEvent) and observation.event_type == 'water_meter_rep':
            if device and device.name:
                observation.title += f' - {device.name}'

        # add admin portal configured subject type
        if isinstance(observation, schemas.Position) and not observation.subject_type:
            if device and device.subject_type:
                observation.subject_type = device.subject_type

    return observation


async def ensure_device_integration(integration_id: str, device_id: str):
    cache_db = get_redis_db()

    cache_key = f'device_detail.{integration_id}.{device_id}'
    resp_json_bytes = cache_db.get(cache_key)

    device = None
    extra_dict = dict(needs_attention=True,
                      integration_id=str(integration_id),
                      device_id=device_id)

    if resp_json_bytes:
        resp_json_str = resp_json_bytes.decode('utf-8')
    else:
        portal = portal_api.PortalApi()

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30),
                                         connector=aiohttp.TCPConnector(ssl=False)) as sess:
            try:
                resp_json = await portal.ensure_device(sess, str(integration_id), device_id)
            except Exception as e:
                logger.exception('Error when posting device to Portal.', extra=extra_dict)
                return None
            else:
                resp_json_str = json.dumps(resp_json)
                cache_db.setex(cache_key, settings.REDIS_CHECK_SECONDS, resp_json_str)
    try:
        resp_json = json.loads(resp_json_str)
        device = schemas.Device.parse_obj(resp_json)
    except Exception as e:
        logger.exception(f"Exception {e} occurred parsing response from portal.ensure_device", extra=extra_dict)
    return device


class TransformerNotFound(Exception):
    pass


def transform_observation(stream_type: str,
            config: schemas.OutboundConfiguration,
            observation) -> dict:

    transformer = None

    # todo: need a better way than this to build the correct components.
    if (stream_type == schemas.StreamPrefixEnum.position
            and config.type_slug == schemas.DestinationTypes.EarthRanger.value):
        transformer = ERPositionTransformer
    elif (stream_type == schemas.StreamPrefixEnum.geoevent
          and config.type_slug == schemas.DestinationTypes.EarthRanger.value):
        transformer = ERGeoEventTransformer
    elif (stream_type == schemas.StreamPrefixEnum.camera_trap
          and config.type_slug == schemas.DestinationTypes.EarthRanger.value):
        transformer = ERCameraTrapTransformer
    elif (stream_type == schemas.StreamPrefixEnum.geoevent
        and config.type_slug == schemas.DestinationTypes.SmartConnect.value):
        transformer = SmartEREventTransformer(config=config, ca_datamodel=None)

    if transformer:
        return transformer.transform(observation)
    else:
        raise TransformerNotFound(f'No dispatcher found for {stream_type} dest: {config.type_slug}')
