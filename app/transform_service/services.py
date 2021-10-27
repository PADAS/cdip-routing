import json
import logging
from typing import List
from uuid import UUID

import requests
import walrus
from cdip_connector.core import schemas
from pydantic import parse_obj_as

from app import settings
from app.core.utils import get_auth_header, create_cache_key, get_redis_db
from app.transform_service.smartconnect_transformers import SmartEREventTransformer
from app.transform_service.transformers import ERPositionTransformer, ERGeoEventTransformer, ERCameraTrapTransformer

logger = logging.getLogger(__name__)


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
        destinations_cache_db.setex(cache_key, settings.REDIS_CHECK_SECONDS, resp_json_str)

    resp_json = json.loads(resp_json_str)
    resp_json = [resp_json] if isinstance(resp_json, dict) else resp_json
    configs, errors = schemas.get_validated_objects(resp_json, schemas.OutboundConfiguration)
    if errors:
        logger.warning(f'{len(errors)} outbound configs have validation errors. {errors}')
    if not configs:
        logger.warning(f'No destinations were found for inbound integration: {inbound_id}')
    return configs


def get_device_detail(integration_id : UUID, device_id: UUID) -> schemas.Device:
    '''
    Get device detail based on inbound integration id and external_id when device.id not available
    :param integration_id: inbound integration configruation id
    :param device_id: external device id
    :return: device
    '''
    cache_db = get_redis_db()
    integration_device_list_endpoint = f'{settings.PORTAL_API_ENDPOINT}/devices/{str(integration_id)}'
    cache_key = create_cache_key(integration_device_list_endpoint)
    resp_json_bytes = cache_db.get(cache_key)

    if resp_json_bytes:
        resp_json_str = resp_json_bytes.decode('utf-8')
    else:
        headers = get_auth_header()
        resp = requests.get(url=f'{integration_device_list_endpoint}',
                            headers=headers, verify=settings.PORTAL_SSL_VERIFY)
        resp.raise_for_status()
        resp_json = resp.json()
        resp_json_str = json.dumps(resp_json)
        cache_db.setex(cache_key, settings.REDIS_CHECK_SECONDS, resp_json_str)

    resp_json = json.loads(resp_json_str)
    resp_json = [resp_json] if isinstance(resp_json, dict) else resp_json
    devices = parse_obj_as(List[schemas.Device], resp_json)
    matching_devices = [d for d in devices if d.external_id == device_id]
    device = None
    if len(matching_devices) > 0:
        # assume only one match on external_id
        device = matching_devices[0]
    return device


def apply_pre_transformation_rules(observation):
    # query portal for configured location if observation location is set to default_location
    if hasattr(observation, 'location') and observation.location == schemas.Location.default_location():
        device = get_device_detail(observation.integration_id, observation.device_id)
        if device:
            default_location = device.additional.location
            observation.location = default_location
    return observation


class TransformerNotFound(Exception):
    pass


def transform_observation(stream_type: str,
            config: schemas.OutboundConfiguration,
            observation) -> dict:

    transformer = None

    # todo: need a better way than this to build the correct components.
    if ((stream_type == schemas.StreamPrefixEnum.position or stream_type == schemas.StreamPrefixEnum.observation)
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
        observation = apply_pre_transformation_rules(observation)
        return transformer.transform(observation)
    else:
        raise TransformerNotFound(f'No dispatcher found for {stream_type} dest: {config.type_slug}')
