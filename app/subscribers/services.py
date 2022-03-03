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


def get_outbound_config_detail(outbound_id: UUID) -> schemas.OutboundConfiguration:

    outbound_integrations_endpoint = f'{settings.PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT}/{str(outbound_id)}'

    try:
        headers = get_auth_header()
        response = requests.get(url=outbound_integrations_endpoint,
                                verify=settings.PORTAL_SSL_VERIFY,
                                headers=headers,
                                timeout=DEFAULT_TIMEOUT)
        if response.ok:
            return schemas.OutboundConfiguration.parse_obj(response.json())

        raise ValueError('Request for OutboundIntegration(%s) returned status: %s, text:%s', outbound_id, response.status_code, response.text)

    except Exception as e:
        logger.exception('Portal returned bad response during request for outbound config detail',
                     extra={ExtraKeys.AttentionNeeded: True,
                            ExtraKeys.OutboundIntId: outbound_id,
                            ExtraKeys.Url: response.request,
                            ExtraKeys.Error: e})
        raise


def get_inbound_integration_detail(integration_id: UUID) -> schemas.IntegrationInformation:

    inbound_integrations_endpoint = f'{settings.PORTAL_INBOUND_INTEGRATIONS_ENDPOINT}/{str(integration_id)}'
    try:
        headers = get_auth_header()
        response = requests.get(url=inbound_integrations_endpoint,
                                verify=settings.PORTAL_SSL_VERIFY,
                                headers=headers,
                                timeout=DEFAULT_TIMEOUT)

        if response.ok:
            return schemas.IntegrationInformation.parse_obj(response.json())

        raise ValueError('Request for InboundIntegration(%s)) returned status: %s, text:%s', integration_id, response.status_code, response.text)

    except Exception as e:
        logger.exception('Portal returned bad response during request for inbound config detail',
                     extra={ExtraKeys.AttentionNeeded: True,
                            ExtraKeys.InboundIntId: integration_id,
                            ExtraKeys.Url: response.request,
                            ExtraKeys.Error: e})
        raise


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
        elif stream_type == schemas.StreamPrefixEnum.geoevent and \
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



