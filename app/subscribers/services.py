import json
import logging
from uuid import UUID

import requests
from cdip_connector.core import schemas
from requests import HTTPError

from app import settings
from app.core.utils import get_auth_header, get_redis_db, create_cache_key
from app.transform_service.dispatchers import ERPositionDispatcher, ERGeoEventDispatcher

logger = logging.getLogger(__name__)


def post_message_to_transform_service(observation_type, observation, message_id):
    logger.debug(f"Received observation: {observation}")
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
    cache_key = create_cache_key(outbound_integrations_endpoint)
    cdip_portal_api_cache_db = get_redis_db()
    resp_json_bytes = cdip_portal_api_cache_db.get(cache_key)

    if resp_json_bytes:
        resp_json_str = resp_json_bytes.decode('utf-8')
    else:
        try:
            headers = get_auth_header()
            resp = requests.get(url=outbound_integrations_endpoint,
                                headers=headers)
            resp.raise_for_status()
            resp_json = resp.json()
            resp_json_str = json.dumps(resp_json)
            cdip_portal_api_cache_db.setex(cache_key, settings.REDIS_CHECK_SECONDS, resp_json_str)
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
        logger.debug(f'observation: {observation}')
        logger.debug(f'config: {config}')

    if config:
        if stream_type == schemas.StreamPrefixEnum.position:
            dispatcher = ERPositionDispatcher(config)
        elif stream_type == schemas.StreamPrefixEnum.geoevent:
            dispatcher = ERGeoEventDispatcher(config)
        if dispatcher:
            dispatcher.send(observation)
        else:
            logger.error(f'No dispatcher found for {stream_type} dest: {config.type_slug}')
    else:
        logger.error(f'No config detail found for {outbound_config_id}')
