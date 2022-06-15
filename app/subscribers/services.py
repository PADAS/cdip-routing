import asyncio
import json
import logging
from datetime import datetime, timedelta
from uuid import UUID

import requests
from cdip_connector.core import schemas, routing, cdip_settings

from app import settings
from app.core.local_logging import ExtraKeys
from app.core.utils import (
    get_auth_header,
    get_redis_db,
    create_cache_key,
    ReferenceDataError,
    DispatcherException,
)
from app.transform_service.dispatchers import (
    ERPositionDispatcher,
    ERGeoEventDispatcher,
    ERCameraTrapDispatcher,
    WPSWatchCameraTrapDispatcher,
    SmartConnectDispatcher,
)
from app.transform_service.services import transform_observation

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = (3.1, 20)

_cache_ttl = settings.PORTAL_CONFIG_OBJECT_CACHE_TTL
_cache_db = get_redis_db()


def get_outbound_config_detail(outbound_id: UUID) -> schemas.OutboundConfiguration:

    if not outbound_id:
        raise ValueError("integration_id must not be None")

    extra_dict = {
        ExtraKeys.AttentionNeeded: True,
        ExtraKeys.OutboundIntId: str(outbound_id),
    }

    cache_key = f"outbound_detail.{outbound_id}"
    cached = _cache_db.get(cache_key)

    if cached:
        config = schemas.OutboundConfiguration.parse_raw(cached)
        logger.debug(
            "Using cached outbound integration detail",
            extra={**extra_dict, "outbound_detail": config},
        )
        return config

    logger.debug(f"Cache miss for outbound integration detail", extra={**extra_dict})

    outbound_integrations_endpoint = (
        f"{settings.PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT}/{str(outbound_id)}"
    )

    headers = get_auth_header()
    response = requests.get(
        url=outbound_integrations_endpoint,
        verify=cdip_settings.CDIP_ADMIN_SSL_VERIFY,
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )
    if response.status_code == 200:
        try:
            resp_json = response.json()
        except json.decoder.JSONDecodeError as jde:
            logger.error(
                f"Failed decoding response for Outbound Integration Detail",
                extra={**extra_dict, "resp_text": response.text},
            )
            raise ReferenceDataError(jde.msg)
        else:
            config = schemas.OutboundConfiguration.parse_obj(resp_json)
            if config:  # don't cache empty response
                _cache_db.setex(cache_key, _cache_ttl, config.json())
            return config

    else:
        logger.exception(
            "Portal returned bad response during request for outbound config detail",
            extra={
                ExtraKeys.AttentionNeeded: True,
                ExtraKeys.OutboundIntId: outbound_id,
                ExtraKeys.Url: outbound_integrations_endpoint,
                ExtraKeys.StatusCode: response.status_code,
            },
        )

        raise ReferenceDataError(
            f"Request for OutboundIntegration({outbound_id}) returned bad response"
        )


def get_inbound_integration_detail(
    integration_id: UUID,
) -> schemas.IntegrationInformation:

    if not integration_id:
        raise ValueError("integration_id must not be None")

    extra_dict = {
        ExtraKeys.AttentionNeeded: True,
        ExtraKeys.InboundIntId: str(integration_id),
    }

    cache_key = f"inbound_detail.{integration_id}"
    cached = _cache_db.get(cache_key)

    if cached:
        config = schemas.IntegrationInformation.parse_raw(cached)
        logger.debug(
            "Using cached inbound integration detail",
            extra={**extra_dict, "integration_detail": config},
        )
        return config

    logger.debug(f"Cache miss for inbound integration detai", extra={**extra_dict})

    inbound_integrations_endpoint = (
        f"{settings.PORTAL_INBOUND_INTEGRATIONS_ENDPOINT}/{str(integration_id)}"
    )

    headers = get_auth_header()
    response = requests.get(
        url=inbound_integrations_endpoint,
        verify=cdip_settings.CDIP_ADMIN_SSL_VERIFY,
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )

    if response.status_code == 200:
        try:
            resp_json = response.json()
        except json.decoder.JSONDecodeError as jde:
            logger.error(
                f"Failed decoding response for InboundIntegration Detail",
                extra={**extra_dict, "resp_text": response.text},
            )
            raise ReferenceDataError(jde.msg)
        else:
            config = schemas.IntegrationInformation.parse_obj(resp_json)
            if config:  # don't cache empty response
                _cache_db.setex(cache_key, _cache_ttl, config.json())
            return config
    else:
        logger.exception(
            "Portal returned bad response during request for inbound config detail",
            extra={
                ExtraKeys.AttentionNeeded: True,
                ExtraKeys.InboundIntId: integration_id,
                ExtraKeys.Url: response.request,
                ExtraKeys.StatusCode: response.status_code,
            },
        )

        raise ReferenceDataError(f"Request for InboundIntegration({integration_id})")


def dispatch_transformed_observation(
    *, stream_type: str, outbound_config_id: str, inbound_int_id: str, observation, span
) -> dict:

    if not outbound_config_id or not inbound_int_id:
        logger.error(
            "dispatch_transformed_observation - value error.",
            extra={
                "outbound_config_id": outbound_config_id,
                "inbound_int_id": inbound_int_id,
                "observation": observation,
                "stream_type": stream_type,
            },
        )
    span.add_event(name="begin_get_outbound_config_detail")
    config = get_outbound_config_detail(outbound_config_id)
    span.add_event(name="end_get_outbound_config_detail")
    span.add_event(name="begin_get_inbound_integration_detail")
    inbound_integration = get_inbound_integration_detail(inbound_int_id)
    span.add_event(name="end_get_inbound_integration_detail")
    provider = inbound_integration.provider
    extra_dict = {
        ExtraKeys.InboundIntId: inbound_int_id,
        ExtraKeys.OutboundIntId: outbound_config_id,
        ExtraKeys.StreamType: stream_type,
    }

    span.add_event(name="begin_dispatch")
    if config:
        if stream_type == schemas.StreamPrefixEnum.position:
            dispatcher = ERPositionDispatcher(config, provider)
        elif (
            stream_type == schemas.StreamPrefixEnum.earthranger_patrol
            or stream_type == schemas.StreamPrefixEnum.earthranger_event
            or stream_type == schemas.StreamPrefixEnum.geoevent
        ) and config.type_slug == schemas.DestinationTypes.SmartConnect.value:
            dispatcher = SmartConnectDispatcher(config)
        elif (
            stream_type == schemas.StreamPrefixEnum.geoevent
            and config.type_slug == schemas.DestinationTypes.EarthRanger.value
        ):
            dispatcher = ERGeoEventDispatcher(config, provider)
        elif (
            stream_type == schemas.StreamPrefixEnum.camera_trap
            and config.type_slug == schemas.DestinationTypes.EarthRanger.value
        ):
            dispatcher = ERCameraTrapDispatcher(config, provider)
        elif (
            stream_type == schemas.StreamPrefixEnum.camera_trap
            and config.type_slug == schemas.DestinationTypes.WPSWatch.value
        ):
            dispatcher = WPSWatchCameraTrapDispatcher(config)

        if dispatcher:
            try:
                dispatcher.send(observation)
                span.add_event(name="end_dispatch")
            except Exception as e:
                logger.error(
                    f"Exception occurred dispatching observation",
                    extra={
                        **extra_dict,
                        ExtraKeys.Provider: config.type_slug,
                        ExtraKeys.AttentionNeeded: True,
                    },
                )
                raise DispatcherException("Exception occurred dispatching observation")
        else:
            extra_dict[ExtraKeys.Provider] = config.type_slug
            logger.error(
                f"No dispatcher found",
                extra={
                    **extra_dict,
                    ExtraKeys.Provider: config.type_slug,
                    ExtraKeys.AttentionNeeded: True,
                },
            )
            raise Exception("No dispatcher found")
    else:
        logger.error(
            f"No outbound config detail found",
            extra={**extra_dict, ExtraKeys.AttentionNeeded: True},
        )
        raise ReferenceDataError


def convert_observation_to_cdip_schema(observation):
    schema = schemas.models_by_stream_type[observation.get("observation_type")]
    # method requires a list
    observations = [observation]
    observations, errors = schemas.get_validated_objects(observations, schema)
    if len(observations) > 0:
        return observations[0]
    else:
        logger.error(
            f"unable to validate observation",
            extra={"observation": observation, ExtraKeys.Error: errors},
        )
        raise Exception("unable to validate observation")


def create_message(attributes, observation):
    message = {"attributes": attributes, "data": observation}
    return message


def create_transformed_message(*, observation, destination, prefix: str, trace_carrier):
    transformed_observation = transform_observation(
        stream_type=prefix, config=destination, observation=observation
    )
    if not transformed_observation:
        return None
    logger.debug(f"Transformed observation: {transformed_observation}")

    attributes = {
        ExtraKeys.StreamType: prefix,
        ExtraKeys.DeviceId: observation.device_id,
        ExtraKeys.OutboundIntId: str(destination.id),
        ExtraKeys.InboundIntId: observation.integration_id,
        ExtraKeys.Carrier: trace_carrier
    }

    transformed_message = create_message(attributes, transformed_observation)

    jsonified_data = json.dumps(transformed_message, default=str)
    return jsonified_data


def create_retry_message(observation, attributes):
    retry_transformed_message = create_message(attributes, observation)
    jsonified_data = json.dumps(retry_transformed_message, default=str)
    return jsonified_data


def update_attributes_for_transformed_retry(attributes):

    retry_topic = attributes.get("retry_topic")
    retry_attempt = attributes.get("retry_attempt")
    retry_at = None

    if not retry_topic:
        # first failure, initialize
        retry_topic = routing.TopicEnum.observations_transformed_retry_short.value
        retry_attempt = 1
        retry_at = datetime.utcnow() + timedelta(
            minutes=settings.RETRY_SHORT_DELAY_MINUTES
        )
    elif retry_topic == routing.TopicEnum.observations_transformed_retry_short.value:
        if retry_attempt < settings.RETRY_SHORT_ATTEMPTS:
            retry_attempt += 1
            retry_at = datetime.utcnow() + timedelta(
                minutes=settings.RETRY_SHORT_DELAY_MINUTES
            )
        else:
            retry_topic = routing.TopicEnum.observations_transformed_retry_long.value
            retry_attempt = 1
            retry_at = datetime.utcnow() + timedelta(
                minutes=settings.RETRY_LONG_DELAY_MINUTES
            )
    elif retry_topic == routing.TopicEnum.observations_transformed_retry_long.value:
        if retry_attempt < settings.RETRY_LONG_ATTEMPTS:
            retry_attempt += 1
            retry_at = datetime.utcnow() + timedelta(
                minutes=settings.RETRY_LONG_DELAY_MINUTES
            )
        else:
            retry_topic = routing.TopicEnum.observations_transformed_deadletter.value

    attributes["retry_topic"] = retry_topic
    attributes["retry_attempt"] = retry_attempt
    if retry_at:
        attributes["retry_at"] = retry_at.isoformat()

    return attributes


def update_attributes_for_unprocessed_retry(attributes):

    retry_topic = attributes.get("retry_topic")
    retry_attempt = attributes.get("retry_attempt")
    retry_at = None

    if not retry_topic:
        # first failure, initialize
        retry_topic = routing.TopicEnum.observations_unprocessed_retry_short.value
        retry_attempt = 1
        retry_at = datetime.utcnow() + timedelta(
            minutes=settings.RETRY_SHORT_DELAY_MINUTES
        )
    elif retry_topic == routing.TopicEnum.observations_unprocessed_retry_short.value:
        if retry_attempt < settings.RETRY_SHORT_ATTEMPTS:
            retry_attempt += 1
            retry_at = datetime.utcnow() + timedelta(
                minutes=settings.RETRY_SHORT_DELAY_MINUTES
            )
        else:
            retry_topic = routing.TopicEnum.observations_unprocessed_retry_long.value
            retry_attempt = 1
            retry_at = datetime.utcnow() + timedelta(
                minutes=settings.RETRY_LONG_DELAY_MINUTES
            )
    elif retry_topic == routing.TopicEnum.observations_unprocessed_retry_long.value:
        if retry_attempt < settings.RETRY_LONG_ATTEMPTS:
            retry_attempt += 1
            retry_at = datetime.utcnow() + timedelta(
                minutes=settings.RETRY_LONG_DELAY_MINUTES
            )
        else:
            retry_topic = routing.TopicEnum.observations_unprocessed_deadletter.value

    attributes["retry_topic"] = retry_topic
    attributes["retry_attempt"] = retry_attempt
    if retry_at:
        attributes["retry_at"] = retry_at.isoformat()

    return attributes


async def wait_until_retry_at(retry_at: datetime):
    now = datetime.utcnow()
    wait_time_seconds = (retry_at - now).total_seconds()
    if wait_time_seconds > 0:
        logger.info(
            f"Waiting to re process observation",
            extra=dict(retry_at=retry_at, wait_time_seconds=wait_time_seconds),
        )
        await asyncio.sleep(wait_time_seconds)
    else:
        logger.info(
            f"Sending retry immediately.",
            extra=dict(retry_at=retry_at, actual_delay_seconds=wait_time_seconds),
        )


def extract_fields_from_message(message):
    decoded_message = json.loads(message.decode("utf-8"))
    if decoded_message:
        observation = decoded_message.get("data")
        attributes = decoded_message.get("attributes")
        if not observation:
            logger.warning(f"No observation was obtained from {decoded_message}")
        if not attributes:
            logger.debug(f"No attributes were obtained from {decoded_message}")
    else:
        logger.warning(f"message contained no payload", extra={"message": message})
        return None, None
    return observation, attributes


def get_key_for_transformed_observation(current_key: bytes, destination_id: UUID):
    # caller must provide key and destination_id must be present in order to create for transformed observation
    if current_key is None or destination_id is None:
        return current_key
    else:
        new_key = f"{current_key.decode('utf-8')}.{str(destination_id)}"
        return new_key.encode("utf-8")


if __name__ == "__main__":

    c = get_outbound_config_detail("34891e4d-0170-4937-917d-46e79fdee082")
    print(c)
