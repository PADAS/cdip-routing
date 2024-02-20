import asyncio
import json
from enum import Enum

import aiohttp
import logging
from datetime import datetime, timedelta
from uuid import UUID
from gundi_core import schemas
from app import settings
from app.core.local_logging import ExtraKeys
from app.core.utils import (
    get_redis_db,
    ReferenceDataError,
)
from gundi_client import PortalApi
from app.settings import DEFAULT_REQUESTS_TIMEOUT


logger = logging.getLogger(__name__)


_cache_ttl = settings.PORTAL_CONFIG_OBJECT_CACHE_TTL
_cache_db = get_redis_db()
_portal = PortalApi()


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
    async with aiohttp.ClientSession(
        timeout=timeout_settings, raise_for_status=True
    ) as s:
        try:
            response = await _portal.get_outbound_integration(
                session=s, integration_id=str(outbound_id)
            )
        except aiohttp.ServerTimeoutError as e:
            target_url = (
                f"{settings.PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT}/{str(outbound_id)}"
            )
            logger.error(
                "Read Timeout",
                extra={**extra_dict, ExtraKeys.Url: target_url},
            )
            raise ReferenceDataError(f"Read Timeout for {target_url}")
        except aiohttp.ClientConnectionError as e:
            target_url = str(settings.PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT)
            logger.error(
                "Connection Error",
                extra={**extra_dict, ExtraKeys.Url: target_url},
            )
            raise ReferenceDataError(
                f"Failed to connect to the portal at {target_url}, {e}"
            )
        except aiohttp.ClientResponseError as e:
            target_url = str(e.request_info.url)
            logger.exception(
                "Portal returned bad response during request for outbound config detail",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.OutboundIntId: outbound_id,
                    ExtraKeys.Url: target_url,
                    ExtraKeys.StatusCode: e.status,
                },
            )
            raise ReferenceDataError(
                f"Request for OutboundIntegration({outbound_id}) returned bad response"
            )
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
    async with aiohttp.ClientSession(
        timeout=timeout_settings, raise_for_status=True
    ) as s:
        try:
            response = await _portal.get_inbound_integration(
                session=s, integration_id=str(integration_id)
            )
        except aiohttp.ServerTimeoutError as e:
            # ToDo: Try to get the url from the exception or from somewhere else
            target_url = (
                f"{settings.PORTAL_INBOUND_INTEGRATIONS_ENDPOINT}/{str(integration_id)}"
            )
            logger.error(
                "Read Timeout",
                extra={**extra_dict, ExtraKeys.Url: target_url},
            )
            raise ReferenceDataError(f"Read Timeout for {target_url}")
        except aiohttp.ClientResponseError as e:
            target_url = str(e.request_info.url)
            logger.exception(
                "Portal returned bad response during request for inbound config detail",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.InboundIntId: integration_id,
                    ExtraKeys.Url: target_url,
                    ExtraKeys.StatusCode: e.status,
                },
            )
            raise ReferenceDataError(
                f"Request for InboundIntegration({integration_id})"
            )
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
