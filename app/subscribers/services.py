import json
import logging
from uuid import UUID

import requests
from cdip_connector.core import schemas

from app import settings
from app.core.utils import get_auth_header, get_redis_db, create_cache_key
from app.transform_service.dispatchers import ERPositionDispatcher, ERGeoEventDispatcher, ERCameraTrapDispatcher, \
    SmartConnectEREventDispatcher
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


# def post_to_admin_portal(endpoint, response_schema: schemas.BaseModel):
#     cache_key = create_cache_key(endpoint)
#     cdip_portal_api_cache_db = get_redis_db()
#     resp_json_bytes = cdip_portal_api_cache_db.get(cache_key)
#     resp_json_str = None

#     if resp_json_bytes:
#         resp_json_str = resp_json_bytes.decode('utf-8')
#     else:
#         try:
#             headers = get_auth_header()
#             resp = requests.get(url=endpoint,
#                                 headers=headers, verify=settings.PORTAL_SSL_VERIFY)
#             resp.raise_for_status()
#             resp_json = resp.json()
#             resp_json_str = json.dumps(resp_json)
#             cdip_portal_api_cache_db.setex(cache_key, settings.REDIS_CHECK_SECONDS, resp_json_str)
#         except HTTPError:
#             logger.error(f"Bad response from portal API {resp} for endpoint: {endpoint}")
#     if resp_json_str:
#         resp_json = json.loads(resp_json_str)
#         resp_json = [resp_json] if isinstance(resp_json, dict) else resp_json
#         configs, errors = schemas.get_validated_objects(resp_json, response_schema)
#     if errors:
#         logger.warning(f'{len(errors)} outbound configs have validation errors. {errors}')
#     if len(configs) > 0:
#         return configs[0]
#     else:
#         logger.warning(f'No valid response objects received from endpoint: {endpoint}')
#         return None


def get_outbound_config_detail(outbound_id: UUID) -> schemas.OutboundConfiguration:

    outbound_integrations_endpoint = f'{settings.PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT}/{str(outbound_id)}'

    cache_key = create_cache_key(outbound_integrations_endpoint)
    cache = get_redis_db()
    cached_json = cache.get(cache_key)
    cached_json = cached_json.decode('utf-8') if cached_json else None

    if cached_json:
        return schemas.OutboundConfiguration.parse_raw(cached_json)
    else:
        headers = get_auth_header()
        response = requests.get(url=outbound_integrations_endpoint, verify=settings.PORTAL_SSL_VERIFY, headers=headers)

        if response.ok:
            value = schemas.OutboundConfiguration.parse_obj(response.json())
            cache.setex(cache_key, settings.PORTAL_CONFIG_OBJECT_CACHE_TTL, value.json())
            return value
        else:
            raise Exception(f'No Outbound configuration found for id: {outbound_id}')


def get_inbound_integration_detail(integration_id: UUID) -> schemas.IntegrationInformation:

    inbound_integrations_endpoint = f'{settings.PORTAL_INBOUND_INTEGRATIONS_ENDPOINT}/{str(integration_id)}'
    cache_key = create_cache_key(inbound_integrations_endpoint)
    cache = get_redis_db()
    cached_json = cache.get(cache_key)
    cached_json = cached_json.decode('utf-8') if cached_json else None

    if cached_json:
        return schemas.IntegrationInformation.parse_raw(cached_json)
    else:

        headers = get_auth_header()
        response = requests.get(url=inbound_integrations_endpoint, verify=settings.PORTAL_SSL_VERIFY, headers=headers)

        if response.ok:

            value = schemas.IntegrationInformation.parse_obj(response.json())
            cache.setex(cache_key, settings.PORTAL_CONFIG_OBJECT_CACHE_TTL, value.json())
            return value
        else:
            raise Exception(f'No Inbound configuration found for id: {integration_id}')

# class SmartConnectEREventLoader(ERLoader):

#     def __init__(self, config:schemas.OutboundConfiguration):
#         super().__init__(config)
#         self.smartconnect_client = SmartClient(api=config.endpoint, username=config.login, password=config.password)

#         self.ca_uuid = config.additional.get('ca_uuid', None)
#     def send(self, messages: List[IndependentIncident]):
#         results = []
#         for m in messages:
#             try:

#                 result = self.smartconnect_client.add_independent_incident(incident=m, ca_uuid=self.ca_uuid)
#                 logger.info(f'Posted IndependentIncident {m} to Smart Connect. Result is: {result}')

#             except Exception as ex:
#                 # todo: propagate exceptions back to caller
#                 logger.exception(f'exception raised sending to dest {ex}')

#         return results



def dispatch_transformed_observation(stream_type: str,
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
        if stream_type == schemas.StreamPrefixEnum.position or stream_type == schemas.StreamPrefixEnum.observation:
            dispatcher = ERPositionDispatcher(config, provider)
        elif stream_type == schemas.StreamPrefixEnum.geoevent and \
            config.type_slug == schemas.DestinationTypes.SmartConnect.value:
                dispatcher = SmartConnectEREventDispatcher(config)
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


def create_message(attributes, observation):
    message = {'attributes': attributes,
               'data': observation}
    return message


def create_transformed_message(observation, destination, prefix: str):
    transformed_observation = transform_observation(prefix, destination, observation)
    logger.debug(f'Transformed observation: {transformed_observation}')

    # observation_type may no longer be needed as topics are now specific to observation type
    attributes = {'observation_type': prefix,
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



