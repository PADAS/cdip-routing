import json
import logging
from uuid import UUID

import requests
from cdip_connector.core import schemas
from requests import HTTPError

from app import settings
from app.core.utils import get_auth_header, get_redis_db, create_cache_key
from app.transform_service.dispatchers import ERPositionDispatcher, ERGeoEventDispatcher, ERCameraTrapDispatcher
from app.transform_service.services import transform_observation

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


def post_to_admin_portal(endpoint, response_schema: schemas.BaseModel):
    cache_key = create_cache_key(endpoint)
    cdip_portal_api_cache_db = get_redis_db()
    resp_json_bytes = cdip_portal_api_cache_db.get(cache_key)
    resp_json_str = None

    if resp_json_bytes:
        resp_json_str = resp_json_bytes.decode('utf-8')
    else:
        try:
            headers = get_auth_header()
            resp = requests.get(url=endpoint,
                                headers=headers)
            resp.raise_for_status()
            resp_json = resp.json()
            resp_json_str = json.dumps(resp_json)
            cdip_portal_api_cache_db.setex(cache_key, settings.REDIS_CHECK_SECONDS, resp_json_str)
        except HTTPError:
            logger.error(f"Bad response from portal API {resp} for endpoint: {endpoint}")
    if resp_json_str:
        resp_json = json.loads(resp_json_str)
        resp_json = [resp_json] if isinstance(resp_json, dict) else resp_json
        configs, errors = schemas.get_validated_objects(resp_json, response_schema)
    if errors:
        logger.warning(f'{len(errors)} outbound configs have validation errors. {errors}')
    if len(configs) > 0:
        return configs[0]
    else:
        logger.warning(f'No valid response objects received from endpoint: {endpoint}')
        return None


def get_outbound_config_detail(outbound_id: UUID) -> schemas.OutboundConfiguration:

    outbound_integrations_endpoint = f'{settings.PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT}/{str(outbound_id)}'
    return post_to_admin_portal(outbound_integrations_endpoint, schemas.OutboundConfiguration)


def get_inbound_integration_detail(integration_id: UUID) -> schemas.IntegrationInformation:

    inbound_integrations_endpoint = f'{settings.PORTAL_INBOUND_INTEGRATIONS_ENDPOINT}/{str(integration_id)}'
    return  post_to_admin_portal(inbound_integrations_endpoint, schemas.IntegrationInformation)


def dispatch_transformed_observation(stream_type: schemas.StreamPrefixEnum,
                                     outbound_config_id: str,
                                     inbound_int_id: str,
                                     observation) -> dict:

    config = get_outbound_config_detail(outbound_config_id)
    inbound_integration = get_inbound_integration_detail(inbound_int_id)
    provider = inbound_integration.provider

    if stream_type == schemas.StreamPrefixEnum.position:
        logger.debug(f'observation: {observation}')
        logger.debug(f'config: {config}')

    if config:
        if stream_type == schemas.StreamPrefixEnum.position:
            dispatcher = ERPositionDispatcher(config, provider)
        elif stream_type == schemas.StreamPrefixEnum.geoevent:
            dispatcher = ERGeoEventDispatcher(config, provider)
        elif stream_type == schemas.StreamPrefixEnum.camera_trap:
            dispatcher = ERCameraTrapDispatcher(config, provider)
        if dispatcher:
            dispatcher.send(observation)
        else:
            logger.error(f'No dispatcher found for {stream_type} dest: {config.type_slug}')
    else:
        logger.error(f'No config detail found for {outbound_config_id}')


def convert_observation_to_cdip_schema(observation, schema: schemas):
    # method requires a list
    observations = [observation]
    observations, errors = schemas.get_validated_objects(observations, schema)
    if len(observations) > 0:
        return observations[0]
    else:
        logger.warning(f'unable to validate position: {observation} errors: {errors}')
        return None


def convert_observation_to_position(observation):
    positions = [observation]
    positions, errors = schemas.get_validated_objects(positions, schemas.Position)
    if len(positions) > 0:
        return positions[0]
    else:
        logger.warning(f'unable to validate position: {observation} errors: {errors}')
        return None


def convert_observation_to_cameratrap(observation):
    payloads = [observation]
    cameratrap_payloads, errors = schemas.get_validated_objects(payloads, schemas.CameraTrap)
    if len(cameratrap_payloads) > 0:
        return cameratrap_payloads[0]
    else:
        logger.warning(f'unable to validate position: {observation} errors: {errors}')
        return None


def create_message(attributes, observation):
    message = {'attributes': attributes,
               'data': observation}
    return message


def create_transformed_message(observation, destination, prefix: schemas.StreamPrefixEnum):
    transformed_observation = transform_observation(prefix, destination, observation)
    logger.debug(f'Transformed observation: {transformed_observation}')

    # observation_type may no longer be needed as topics are now specific to observation type
    attributes = {'observation_type': prefix.value,
                  'outbound_config_id': str(destination.id),
                  'integration_id': observation.integration_id}

    transformed_message = create_message(attributes, transformed_observation)

    jsonified_data = json.dumps(transformed_message, default=str)
    return jsonified_data


def extract_fields_from_message(message):
    decoded_message = json.loads(message.decode('utf-8'))
    if decoded_message:
        observation = decoded_message.get('data')
        attributes = decoded_message.get('attributes')
    else:
        logger.warning(f'message: {message} contained no payload')
        return None, None
    return observation, attributes


def get_key_for_transformed_observation(current_key: bytes, destination_id: UUID):
    # caller must provide key and destination_id must be present in order to create for transformed observation
    if current_key is None or destination_id is None:
        return current_key
    else:
        new_key = f"{current_key.decode('utf-8')}.{str(destination_id)}"
        return new_key.encode('utf-8')



