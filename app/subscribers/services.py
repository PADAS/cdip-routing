import asyncio
import json
import logging
from datetime import datetime, timedelta
from uuid import UUID

import requests
from cdip_connector.core import schemas, routing

from app import settings
from app.core.local_logging import ExtraKeys
from app.core.utils import get_auth_header, get_redis_db, create_cache_key
from app.transform_service.dispatchers import ERPositionDispatcher, ERGeoEventDispatcher, ERCameraTrapDispatcher, \
    SmartConnectEREventDispatcher, WPSWatchCameraTrapDispatcher
from app.transform_service.services import transform_observation

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = (3.1, 20)


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
        try:
            headers = get_auth_header()
            response = requests.get(url=outbound_integrations_endpoint,
                                    verify=settings.PORTAL_SSL_VERIFY,
                                    headers=headers,
                                    timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            value = schemas.OutboundConfiguration.parse_obj(response.json())
            if value:
                cache.setex(cache_key, settings.PORTAL_CONFIG_OBJECT_CACHE_TTL, value.json())
            return value
        except Exception as e:
            logger.error('Portal returned bad response during request for outbound config detail',
                         extra={ExtraKeys.AttentionNeeded: True,
                                ExtraKeys.OutboundIntId: outbound_id,
                                ExtraKeys.Url: response.request,
                                ExtraKeys.Error: e})


def get_inbound_integration_detail(integration_id: UUID) -> schemas.IntegrationInformation:

    inbound_integrations_endpoint = f'{settings.PORTAL_INBOUND_INTEGRATIONS_ENDPOINT}/{str(integration_id)}'
    cache_key = create_cache_key(inbound_integrations_endpoint)
    cache = get_redis_db()
    cached_json = cache.get(cache_key)
    cached_json = cached_json.decode('utf-8') if cached_json else None

    if cached_json:
        return schemas.IntegrationInformation.parse_raw(cached_json)
    else:
        try:
            headers = get_auth_header()
            response = requests.get(url=inbound_integrations_endpoint,
                                    verify=settings.PORTAL_SSL_VERIFY,
                                    headers=headers,
                                    timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            value = schemas.IntegrationInformation.parse_obj(response.json())
            if value:
                cache.setex(cache_key, settings.PORTAL_CONFIG_OBJECT_CACHE_TTL, value.json())
            return value
        except Exception as e:
            logger.error('Portal returned bad response during request for inbound config detail',
                         extra={ExtraKeys.AttentionNeeded: True,
                                ExtraKeys.InboundIntId: integration_id,
                                ExtraKeys.Url: response.request,
                                ExtraKeys.Error: e})

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
    extra_dict = {ExtraKeys.InboundIntId: inbound_int_id,
                  ExtraKeys.OutboundIntId: outbound_config_id,
                  ExtraKeys.StreamType: stream_type}

    if config:
        if stream_type == schemas.StreamPrefixEnum.position:
            dispatcher = ERPositionDispatcher(config, provider)
        elif (stream_type == schemas.StreamPrefixEnum.geoevent or
              stream_type == schemas.StreamPrefixEnum.earthranger_event) and \
                config.type_slug == schemas.DestinationTypes.SmartConnect.value:
                dispatcher = SmartConnectEREventDispatcher(config)
        elif stream_type == schemas.StreamPrefixEnum.geoevent:
            dispatcher = ERGeoEventDispatcher(config, provider)
        elif stream_type == schemas.StreamPrefixEnum.camera_trap and \
                config.type_slug == schemas.DestinationTypes.EarthRanger.value:
            dispatcher = ERCameraTrapDispatcher(config, provider)
        elif stream_type == schemas.StreamPrefixEnum.camera_trap and \
                config.type_slug == schemas.DestinationTypes.WPSWatch.value:
            dispatcher = WPSWatchCameraTrapDispatcher(config)

        if dispatcher:
            dispatcher.send(observation)
        else:
            extra_dict[ExtraKeys.Provider] = config.type_slug
            logger.error(f'No dispatcher found', extra={**extra_dict,
                                                        ExtraKeys.Provider: config.type_slug,
                                                        ExtraKeys.AttentionNeeded: True})
    else:
        logger.error(f'No outbound config detail found', extra={**extra_dict,
                                                                ExtraKeys.AttentionNeeded: True})


def convert_observation_to_cdip_schema(observation):
    schema = schemas.models_by_stream_type[observation.get('observation_type')]
    # method requires a list
    observations = [observation]
    observations, errors = schemas.get_validated_objects(observations, schema)
    if len(observations) > 0:
        return observations[0]
    else:
        logger.error(f'unable to validate observation', extra={'observation': observation,
                                                               ExtraKeys.Error: errors})
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
                  'device_id': observation.device_id,
                  'outbound_config_id': str(destination.id),
                  'integration_id': observation.integration_id}

    transformed_message = create_message(attributes, transformed_observation)

    jsonified_data = json.dumps(transformed_message, default=str)
    return jsonified_data


def create_retry_transformed_message(transformed_observation, attributes):
    retry_transformed_message = create_message(attributes, transformed_observation)
    jsonified_data = json.dumps(retry_transformed_message, default=str)
    return jsonified_data


def update_attributes_for_retry(attributes):

    retry_topic = attributes.get('retry_topic')
    retry_attempt = attributes.get('retry_attempt')
    retry_at = None

    if not retry_topic:
        # first failure, initialize
        retry_topic = routing.TopicEnum.observations_transformed_retry_short.value
        retry_attempt = 1
        retry_at = datetime.utcnow() + timedelta(minutes=settings.RETRY_SHORT_DELAY_MINUTES)
    elif retry_topic == routing.TopicEnum.observations_transformed_retry_short.value:
        if retry_attempt < settings.RETRY_SHORT_ATTEMPTS:
            retry_attempt += 1
            retry_at = datetime.utcnow() + timedelta(minutes=settings.RETRY_SHORT_DELAY_MINUTES)
        else:
            retry_topic = routing.TopicEnum.observations_transformed_retry_long.value
            retry_attempt = 1
            retry_at = datetime.utcnow() + timedelta(minutes=settings.RETRY_LONG_DELAY_MINUTES)
    elif retry_topic == routing.TopicEnum.observations_transformed_retry_long.value:
        if retry_attempt < settings.RETRY_LONG_ATTEMPTS:
            retry_attempt += 1
            retry_at = datetime.utcnow() + timedelta(minutes=settings.RETRY_LONG_DELAY_MINUTES)
        else:
            retry_topic = routing.TopicEnum.observations_transformed_deadletter.value

    attributes['retry_topic'] = retry_topic
    attributes['retry_attempt'] = retry_attempt
    if retry_at:
        attributes['retry_at'] = retry_at.isoformat()

    return attributes


async def wait_until_retry_at(retry_at: datetime):
    now = datetime.utcnow()
    wait_time_seconds = (retry_at - now).total_seconds()
    if wait_time_seconds > 0:
        logger.info(f'Waiting to re process transformed observation',
                    extra=dict(retry_at=retry_at,
                               wait_time_seconds=wait_time_seconds))
        await asyncio.sleep(wait_time_seconds)
    else:
        logger.info(f'Sending retry immediately.', extra=dict(retry_at=retry_at,
                                                              actual_delay_seconds=wait_time_seconds))


def extract_fields_from_message(message):
    decoded_message = json.loads(message.decode('utf-8'))
    if decoded_message:
        observation = decoded_message.get('data')
        attributes = decoded_message.get('attributes')
        if not observation:
            logger.warning(f'No observation was obtained from {decoded_message}')
        if not attributes:
            logger.warning(f'No attributes were obtained from {decoded_message}')
    else:
        logger.warning(f'message contained no payload', extra={'message': message})
        return None, None
    return observation, attributes


def get_key_for_transformed_observation(current_key: bytes, destination_id: UUID):
    # caller must provide key and destination_id must be present in order to create for transformed observation
    if current_key is None or destination_id is None:
        return current_key
    else:
        new_key = f"{current_key.decode('utf-8')}.{str(destination_id)}"
        return new_key.encode('utf-8')



