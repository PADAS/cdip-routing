import asyncio
import json
import aiohttp
import logging
from datetime import datetime, timedelta
from uuid import UUID
import httpx
from cdip_connector.core import routing
from gundi_core import schemas
from app import settings
from app.core.local_logging import ExtraKeys
from app.core.utils import (
    get_redis_db,
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
from gundi_client import PortalApi
from app.settings import DEFAULT_REQUESTS_TIMEOUT


logger = logging.getLogger(__name__)

connect_timeout, read_timeout = settings.DEFAULT_REQUESTS_TIMEOUT
_portal = PortalApi(connect_timeout=connect_timeout, data_timeout=read_timeout)
_cache_ttl = settings.PORTAL_CONFIG_OBJECT_CACHE_TTL
_cache_db = get_redis_db()


async def get_outbound_config_detail(
    outbound_id: UUID,
) -> schemas.OutboundConfiguration:
    if not outbound_id:
        raise ValueError("integration_id must not be None")

    extra_dict = {
        ExtraKeys.AttentionNeeded: True,
        ExtraKeys.OutboundIntId: str(outbound_id),
    }

    cache_key = f"outbound_detail.{outbound_id}"
    cached = await _cache_db.get(cache_key)

    if cached:
        config = schemas.OutboundConfiguration.parse_raw(cached)
        logger.debug(
            "Using cached outbound integration detail",
            extra={
                **extra_dict,
                ExtraKeys.AttentionNeeded: False,
                "outbound_detail": config,
            },
        )
        return config

    logger.debug(f"Cache miss for outbound integration detail", extra={**extra_dict})

    connect_timeout, read_timeout = DEFAULT_REQUESTS_TIMEOUT
    timeout_settings = aiohttp.ClientTimeout(
        sock_connect=connect_timeout, sock_read=read_timeout
    )
    async with _portal:
        try:
            response = await _portal.get_outbound_integration(
                integration_id=str(outbound_id)
            )
        except httpx.HTTPStatusError as e:
            error = f"HTTPStatusError: {e.response.status_code}, {e.response.text}"
            message = (
                f"Failed to get outbound details for outbound_id {outbound_id}: {error}"
            )
            target_url = str(e.request.url)
            logger.exception(
                message,
                extra={
                    **extra_dict,
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.Url: target_url,
                },
            )
            # Raise again so it's retried later
            raise ReferenceDataError(message)
        except httpx.HTTPError as e:
            error = f"HTTPError: {e}"
            message = (
                f"Failed to get outbound details for outbound_id {outbound_id}: {error}"
            )
            target_url = str(e.request.url)
            logger.exception(
                message,
                extra={
                    **extra_dict,
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.Url: target_url,
                },
            )
            # Raise again so it's retried later
            raise ReferenceDataError(message)
        else:
            try:
                config = schemas.OutboundConfiguration.parse_obj(response)
            except Exception:
                logger.error(
                    f"Failed decoding response for Outbound Integration Detail",
                    extra={**extra_dict, "resp_text": response},
                )
                raise ReferenceDataError(
                    "Failed decoding response for Outbound Integration Detail"
                )
            else:
                if config:  # don't cache empty response
                    await _cache_db.setex(cache_key, _cache_ttl, config.json())
                return config


async def get_inbound_integration_detail(
    integration_id: UUID,
) -> schemas.IntegrationInformation:
    if not integration_id:
        raise ValueError("integration_id must not be None")

    extra_dict = {
        ExtraKeys.AttentionNeeded: True,
        ExtraKeys.InboundIntId: str(integration_id),
    }

    cache_key = f"inbound_detail.{integration_id}"
    cached = await _cache_db.get(cache_key)

    if cached:
        config = schemas.IntegrationInformation.parse_raw(cached)
        logger.debug(
            "Using cached inbound integration detail",
            extra={**extra_dict, "integration_detail": config},
        )
        return config

    logger.debug(f"Cache miss for inbound integration detai", extra={**extra_dict})

    connect_timeout, read_timeout = DEFAULT_REQUESTS_TIMEOUT
    timeout_settings = aiohttp.ClientTimeout(
        sock_connect=connect_timeout, sock_read=read_timeout
    )
    async with _portal:
        try:
            response = await _portal.get_inbound_integration(
                integration_id=str(integration_id)
            )
        except httpx.HTTPStatusError as e:
            error = f"HTTPStatusError: {e.response.status_code}, {e.response.text}"
            message = f"Failed to get inbound details for integration_id {integration_id}: {error}"
            target_url = str(e.request.url)
            logger.exception(
                message,
                extra={
                    **extra_dict,
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.Url: target_url,
                },
            )
            # Raise again so it's retried later
            raise ReferenceDataError(message)
        except httpx.HTTPError as e:
            error = f"HTTPError: {e}"
            message = f"Failed to get inbound details for integration_id {integration_id}: {error}"
            target_url = str(e.request.url)
            logger.exception(
                message,
                extra={
                    **extra_dict,
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.Url: target_url,
                },
            )
            # Raise again so it's retried later
            raise ReferenceDataError(message)
        else:
            try:
                config = schemas.IntegrationInformation.parse_obj(response)
            except Exception:
                logger.error(
                    f"Failed decoding response for InboundIntegration Detail",
                    extra={**extra_dict, "resp_text": response},
                )
                raise ReferenceDataError(
                    "Failed decoding response for InboundIntegration Detail"
                )
            else:
                if config:  # don't cache empty response
                    await _cache_db.setex(cache_key, _cache_ttl, config.json())
                return config


async def dispatch_transformed_observation(
    stream_type: str, outbound_config_id: str, inbound_int_id: str, observation
):
    extra_dict = {
        ExtraKeys.OutboundIntId: outbound_config_id,
        ExtraKeys.InboundIntId: inbound_int_id,
        ExtraKeys.Observation: observation,
        ExtraKeys.StreamType: stream_type,
    }

    if not outbound_config_id or not inbound_int_id:
        logger.error(
            "dispatch_transformed_observation - value error.",
            extra=extra_dict,
        )

    config = await get_outbound_config_detail(outbound_config_id)
    inbound_integration = await get_inbound_integration_detail(inbound_int_id)
    provider = inbound_integration.provider

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
                await dispatcher.send(observation)
            except Exception as e:
                logger.exception(
                    f"Exception occurred dispatching observation",
                    extra={
                        **extra_dict,
                        ExtraKeys.Provider: config.type_slug,
                        ExtraKeys.AttentionNeeded: True,
                    },
                )
                raise DispatcherException(
                    f"Exception occurred dispatching observation {e}"
                )
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


def convert_observation_to_cdip_schema(observation, gundi_version="v1"):
    if gundi_version == "v2":
        schema = schemas.v2.models_by_stream_type[observation.get("observation_type")]
    else:  # Default to v1
        schema = schemas.models_by_stream_type[observation.get("observation_type")]
    return schema.parse_obj(observation)


def create_message(attributes, observation):
    message = {"attributes": attributes, "data": observation}
    return message


def build_kafka_message(*, payload, attributes):
    message = create_message(attributes, payload)
    jsonified_data = json.dumps(message, default=str)
    return jsonified_data


def build_gcp_pubsub_message(*, payload):
    binary_data = json.dumps(payload, default=str).encode("utf-8")
    return binary_data


def create_retry_message(observation, attributes):
    return build_kafka_message(payload=observation, attributes=attributes)


def update_attributes_for_transformed_retry(attributes):
    retry_topic = attributes.get("retry_topic")
    retry_attempt = attributes.get("retry_attempt")
    retry_at = None

    if settings.RETRY_SHORT_ATTEMPTS == 0 and settings.RETRY_LONG_ATTEMPTS == 0:
        # check if retry logic disabled
        retry_topic = routing.TopicEnum.observations_transformed_deadletter.value
    elif not retry_topic:
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
