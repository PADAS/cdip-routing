import json
import logging
from hashlib import md5
from typing import List, Dict
from uuid import UUID

import requests
import walrus
from cdip_connector.core import schemas

import settings
import transformers

logger = logging.getLogger(__name__)


def get_all_outbound_configs_for_id(db: walrus.Database, inbound_id: UUID) -> List[schemas.OutboundConfiguration]:
    hash = md5(str(inbound_id).encode('utf-8')).hexdigest()
    resp_json_bytes = db.get(hash)
    if resp_json_bytes:
        resp_json_str = resp_json_bytes.decode('utf-8')
    else:
        headers = get_auth_header()
        resp = requests.get(url=f'{settings.PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT}',
                            params=dict(inbound_id=inbound_id),
                            headers=headers)
        resp.raise_for_status()
        resp_json = resp.json()
        resp_json_str = json.dumps(resp_json)
        db.setex(hash, settings.REDIS_CHECK_SECONDS, resp_json_str)
    resp_json = json.loads(resp_json_str)
    resp_json = [resp_json] if isinstance(resp_json, dict) else resp_json
    configs, errors = schemas.get_validated_objects(resp_json, schemas.OutboundConfiguration)
    if errors:
        logger.warning(f'{len(errors)} outbound configs have validation errors. {errors}')
    return configs


class TransformerNotFound(Exception):
    pass


def process(stream_type: schemas.StreamPrefixEnum,
            config: schemas.OutboundConfiguration,
            observation) -> dict:

    transformer = None

    # todo: need a better way than this to build the correct components.
    if (stream_type == schemas.StreamPrefixEnum.position
            and config.type_slug == schemas.DestinationTypes.EarthRanger.value):
        transformer = transformers.ERPositionTransformer
    elif (stream_type == schemas.StreamPrefixEnum.geoevent
          and config.type_slug == schemas.DestinationTypes.EarthRanger.value):
        transformer = transformers.ERGeoEventTransformer
    if transformer:
        return transformer.transform(observation)
    else:
        raise TransformerNotFound(f'No dispatcher found for {stream_type} dest: {config.type_slug}')