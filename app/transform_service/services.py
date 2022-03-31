import json
import logging
from typing import List
from uuid import UUID

import aiohttp
import requests
import walrus
from cdip_connector.core import schemas, portal_api

from app import settings
from app.core.local_logging import ExtraKeys
from app.core.utils import get_auth_header, create_cache_key, get_redis_db
from app.transform_service.smartconnect_transformers import SmartEREventTransformer
from app.transform_service.transformers import ERPositionTransformer, ERGeoEventTransformer, ERCameraTrapTransformer, \
    WPSWatchCameraTrapTransformer

logger = logging.getLogger(__name__)

DEFAULT_LOCATION = schemas.Location(x=0.0, y=0.0)


def get_all_outbound_configs_for_id(destinations_cache_db: walrus.Database, inbound_id: UUID) -> List[schemas.OutboundConfiguration]:

    outbound_integrations_endpoint = settings.PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT

    try:
        headers = get_auth_header()
        resp = requests.get(url=f'{outbound_integrations_endpoint}',
                            params=dict(inbound_id=inbound_id),
                            headers=headers, verify=settings.PORTAL_SSL_VERIFY,
                            timeout=(3.1, 20))
    except:
        logger.exception('Failed to get outbound integrations for inbound_id', extra={'inbound_integration_id': inbound_id})
        raise

    if resp.status_code == 200:
        try:
            resp_json = resp.json()
        except json.decoder.JSONDecodeError as jde:
            logger.error('Failed decoding response for OutboundConfig for Inbound(%s), response text: %s', inbound_id,
                         resp.text, extra={'inbound_integration_id': inbound_id})
            raise
        else:
            resp_json = [resp_json] if isinstance(resp_json, dict) else resp_json
            configs, errors = schemas.get_validated_objects(resp_json, schemas.OutboundConfiguration)
            return configs

    raise Exception(f'Failed to get outbound integrations for inbound_id {inbound_id}')


async def update_observation_with_device_configuration(observation):
    device = await ensure_device_integration(observation.integration_id, observation.device_id)
    if device:
        if hasattr(observation, 'location') and observation.location == DEFAULT_LOCATION:
            if device.additional and device.additional.location:
                default_location = device.additional.location
                observation.location = default_location
            else:
                logger.warning(f"No default location found for device with unspecified location",
                               extra={ExtraKeys.DeviceId: observation.device_id})

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
    cached = cache_db.get(cache_key)

    try:
        if cached:
            device = schemas.Device.parse_raw(cached)
            logger.debug('Using cached Device %s', device.device_id)
            return device
    except:
        pass

    logger.debug('Cache miss forgithub Device %s', device_id)

    extra_dict = {ExtraKeys.AttentionNeeded: True,
                  ExtraKeys.InboundIntId: str(integration_id),
                  ExtraKeys.DeviceId: device_id}

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30),
                                     connector=aiohttp.TCPConnector(ssl=False)) as sess:
        try:
            portal = portal_api.PortalApi()
            device_data = await portal.ensure_device(sess, str(integration_id), device_id)

            if device_data:
                # temporary hack to refit response to Device schema.
                device_data['inbound_configuration'] = device_data.get('inbound_configuration',{}).get('id', None)
                device = schemas.Device.parse_obj(device_data)
                cache_db.setex(cache_key, 60, device.json())
        except Exception as e:
            logger.exception('Error when posting device to Portal.', extra={**extra_dict,
                                                                            ExtraKeys.Error: e,
                                                                            "device_id": device_id})
            return None


class TransformerNotFound(Exception):
    pass


def transform_observation(stream_type: str,
            config: schemas.OutboundConfiguration,
            observation) -> dict:

    transformer = None
    extra_dict = {ExtraKeys.InboundIntId: observation.integration_id,
                  ExtraKeys.OutboundIntId: config.id,
                  ExtraKeys.StreamType: stream_type}

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
    elif (stream_type == schemas.StreamPrefixEnum.camera_trap
          and config.type_slug == schemas.DestinationTypes.WPSWatch.value):
        transformer = WPSWatchCameraTrapTransformer
    elif ((stream_type == schemas.StreamPrefixEnum.geoevent or
           stream_type == schemas.StreamPrefixEnum.earthranger_event)
          and config.type_slug == schemas.DestinationTypes.SmartConnect.value):
        transformer = SmartEREventTransformer(config=config, ca_datamodel=None)

    if transformer:
        return transformer.transform(observation)
    else:
        logger.error('No transformer found for stream type', extra={**extra_dict,
                                                                   ExtraKeys.Provider: config.type_slug})
        raise TransformerNotFound(f'No transformer found for {stream_type} dest: {config.type_slug}')
