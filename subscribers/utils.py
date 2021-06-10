import json
import logging
from hashlib import md5
from uuid import UUID

import requests
from cdip_connector.core import schemas

import core
import settings

logger = logging.getLogger(__name__)


class DispatcherNotFound(Exception):
    pass


def get_outbound_config_detail(outbound_id: UUID) -> schemas.OutboundConfiguration:
    headers = core.utils.get_auth_header()
    db = core.utils.get_redis_db()
    hash = md5(str(outbound_id).encode('utf-8')).hexdigest()
    resp_json_bytes = db.get(hash)
    if resp_json_bytes:
        resp_json_str = resp_json_bytes.decode('utf-8')
    else:
        resp = requests.get(url=f'{settings.PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT}/{outbound_id}',
                            headers=headers)
        resp.raise_for_status()
        resp_json = resp.json()
        resp_json_str = json.dumps(resp_json)
        # TODO only cache response if response code is good?
        db.setex(hash, settings.REDIS_CHECK_SECONDS, resp_json_str)
    resp_json = json.loads(resp_json_str)
    resp_json = [resp_json] if isinstance(resp_json, dict) else resp_json
    configs, errors = schemas.get_validated_objects(resp_json, schemas.OutboundConfiguration)
    if errors:
        logger.warning(f'{len(errors)} outbound configs have validation errors. {errors}')
    if len(configs) > 0:
        return configs[0]
    else:
        logger.warning(f'No destinations were found for outbound config id: {str(outbound_id)}')
        return None


def dispatch_transformed_observation(stream_type: schemas.StreamPrefixEnum,
                                     outbound_config_id: str,
                                     observation) -> dict:
    config = get_outbound_config_detail(outbound_config_id)
    if stream_type == schemas.StreamPrefixEnum.position:
        print(f'observation: {observation}')
        print(f'config: {config}')


    dispatcher = None

    # TODO: Refactor this section once Chris completes metadata on device group
    # if stream_type == schemas.StreamPrefixEnum.position:
    #     dispatcher = dispatchers.ERPositionDispatcher(config)
    # elif stream_type == schemas.StreamPrefixEnum.geoevent:
    #     dispatcher = dispatchers.ERGeoEventDispatcher(config)
    # if dispatcher:
    #     dispatcher.send(observation)
    # else:
    #     raise DispatcherNotFound(f'No dispatcher found for {stream_type} dest: {config.type_slug}')