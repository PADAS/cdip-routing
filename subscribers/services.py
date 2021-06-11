import json
import logging
import sys
from hashlib import md5
from uuid import UUID

import requests
from cdip_connector.core import schemas
from requests import HTTPError

import settings
from core.utils import get_auth_header, get_redis_db

logger = logging.getLogger(__name__)


class DispatcherNotFound(Exception):
    pass


def create_outbound_config_cache_key(endpoint):
    return md5(str(endpoint).encode('utf-8')).hexdigest()


def post_message_to_transform_service(observation_type, observation, message_id):
    print(f"Received observation: {observation}")
    if observation_type == schemas.StreamPrefixEnum.position:
        response = requests.post(settings.TRANSFORM_SERVICE_POSITIONS_ENDPOINT, json=observation)
    else:
        logger.warning(f'Observation: {observation} type: {observation_type} is not supported')
        # TODO how to handle unsupported observation types
    if not response.ok:
        # TODO how to handle bad Transform Service responses ?
        logger.error(f"Transform Service Error response: {response} "
                     f"while processing: {message_id} "
                     f"observation: {observation}")


def get_outbound_config_detail(outbound_id: UUID) -> schemas.OutboundConfiguration:
    outbound_integrations_endpoint = f'{settings.PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT}/{str(outbound_id)}'
    headers = get_auth_header()
    try:
        outboundconfig_detail_cache_db = get_redis_db()
        stream_key = create_outbound_config_cache_key(outbound_integrations_endpoint)
        resp_json_bytes = outboundconfig_detail_cache_db.get(stream_key)
    except:
        e = sys.exc_info()[0]
        logger.exception(f'exception: {e} occurred while interacting with REDIS')
    if resp_json_bytes:
        resp_json_str = resp_json_bytes.decode('utf-8')
    else:
        try:
            resp = requests.get(url=outbound_integrations_endpoint,
                                headers=headers)
            resp.raise_for_status()
            resp_json = resp.json()
            resp_json_str = json.dumps(resp_json)
            outboundconfig_detail_cache_db.setex(stream_key, settings.REDIS_CHECK_SECONDS, resp_json_str)
        except HTTPError:
            logger.error(f"Bad response from portal API {resp} obtaining configuration detail for id: {outbound_id}")
    if resp_json_str:
        resp_json = json.loads(resp_json_str)
        resp_json = [resp_json] if isinstance(resp_json, dict) else resp_json
        # TODO: Figure out why package is expecting inbound_type_slug when not preset in portal response
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

    # TODO: Refactor this section once Chris completes metadata on device group
    # if stream_type == schemas.StreamPrefixEnum.position:
    #     dispatcher = dispatchers.ERPositionDispatcher(config)
    # elif stream_type == schemas.StreamPrefixEnum.geoevent:
    #     dispatcher = dispatchers.ERGeoEventDispatcher(config)
    # if dispatcher:
    #     dispatcher.send(observation)
    # else:
    #     raise DispatcherNotFound(f'No dispatcher found for {stream_type} dest: {config.type_slug}')