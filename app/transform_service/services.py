import json
import logging
from typing import List
from uuid import UUID

import requests
import walrus
from cdip_connector.core import schemas

from app import settings
from app.core.utils import get_auth_header, create_cache_key
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
    return configs


class TransformerNotFound(Exception):
    pass


def transform_observation(stream_type: schemas.StreamPrefixEnum,
            config: schemas.OutboundConfiguration,
            observation) -> dict:

    transformer = None

    # todo: need a better way than this to build the correct components.
    if (stream_type == schemas.StreamPrefixEnum.position
            and config.type_slug in (schemas.DestinationTypes.EarthRanger.value, 'earthranger')):
        transformer = ERPositionTransformer
    elif (stream_type == schemas.StreamPrefixEnum.geoevent
          and config.type_slug == schemas.DestinationTypes.EarthRanger.value):
        transformer = ERGeoEventTransformer
    elif (stream_type == schemas.StreamPrefixEnum.camera_trap
          and config.type_slug == schemas.DestinationTypes.EarthRanger.value):
        transformer = ERCameraTrapTransformer
    if transformer:
        return transformer.transform(observation)
    else:
        raise TransformerNotFound(f'No dispatcher found for {stream_type} dest: {config.type_slug}')
