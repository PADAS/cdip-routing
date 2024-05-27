import logging
import aiohttp
from typing import List
from uuid import UUID
import httpx
from gundi_core import schemas
from pydantic import BaseModel, parse_obj_as
from redis import exceptions as redis_exceptions
from app import settings
from app.core.local_logging import ExtraKeys
from app.core.utils import (
    get_redis_db,
    coalesce,
)
from app.core.errors import ReferenceDataError
from gundi_client import PortalApi
from gundi_client_v2 import GundiClient


logger = logging.getLogger(__name__)


DEFAULT_LOCATION = schemas.Location(x=0.0, y=0.0)
GUNDI_V1 = "v1"
GUNDI_V2 = "v2"

connect_timeout, read_timeout = settings.DEFAULT_REQUESTS_TIMEOUT
_portal = PortalApi(connect_timeout=connect_timeout, data_timeout=read_timeout)
portal_v2 = GundiClient()

_cache_ttl = settings.PORTAL_CONFIG_OBJECT_CACHE_TTL
_cache_db = get_redis_db()

URN_GUNDI_PREFIX = "urn:gundi:"
URN_GUNDI_INTSRC_FORMAT = "intsrc"
URN_GUNDI_FORMATS = {"integration_source": URN_GUNDI_INTSRC_FORMAT}


def build_gundi_urn(
    gundi_version: str,
    integration_id: UUID,
    device_id: str,
    urn_format: str = "integration_source",
):
    format_id = URN_GUNDI_FORMATS.get(urn_format, URN_GUNDI_INTSRC_FORMAT)
    return f"{URN_GUNDI_PREFIX}{gundi_version}.{format_id}.{str(integration_id)}.{device_id}"


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
        except Exception as e:
            logger.exception(
                f"Failed decoding response for Outbound Integration Detail: {e}",
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
        except Exception as e:
            logger.exception(
                f"Failed decoding response for InboundIntegration Detail: {e}",
                extra={**extra_dict, "resp_text": response},
            )
            raise ReferenceDataError(
                "Failed decoding response for InboundIntegration Detail"
            )
        else:
            if config:  # don't cache empty response
                await _cache_db.setex(cache_key, _cache_ttl, config.json())
            return config


class OutboundConfigurations(BaseModel):
    configurations: List[schemas.OutboundConfiguration]


async def get_all_outbound_configs_for_id(
    inbound_id: UUID, device_id
) -> List[schemas.OutboundConfiguration]:
    extra_dict = {
        ExtraKeys.InboundIntId: str(inbound_id),
        ExtraKeys.DeviceId: device_id,
    }

    cache_key = f"device_destinations.{inbound_id}.{device_id}"
    cached = await _cache_db.get(cache_key)

    if cached:
        configs = OutboundConfigurations.parse_raw(cached).configurations
        logger.debug(
            "Using cached destinations", extra={**extra_dict, "destinations": configs}
        )
        return configs

    logger.debug(f"Cache miss for device_destinations", extra={**extra_dict})

    try:
        resp = await _portal.get_outbound_integration_list(
            inbound_id=str(inbound_id), device_id=str(device_id)
        )
    except httpx.HTTPStatusError as e:
        error = f"HTTPStatusError: {e.response.status_code}, {e.response.text}"
        message = (
            f"Failed to get outbound integrations for inbound_id {inbound_id}: {error}"
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
            f"Failed to get outbound integrations for inbound_id {inbound_id}: {error}"
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
            resp_json = [resp] if isinstance(resp, dict) else resp
            configurations = parse_obj_as(
                List[schemas.OutboundConfiguration], resp_json
            )
        except Exception as e:
            logger.exception(
                f"Failed decoding response for OutboundConfig: {e}",
                extra={**extra_dict, "resp_text": resp},
            )
            raise ReferenceDataError("Failed decoding response for OutboundConfig")
        else:
            configs = OutboundConfigurations(configurations=configurations)
            if configurations:  # don't cache empty response
                await _cache_db.setex(cache_key, _cache_ttl, configs.json())
            return configs.configurations


async def update_observation_with_device_configuration(observation):
    device = await ensure_device_integration(
        observation.integration_id, observation.device_id
    )
    if device:
        if (
            hasattr(observation, "location")
            and observation.location == DEFAULT_LOCATION
        ):
            if device.additional and device.additional.location:
                default_location = device.additional.location
                observation.location = default_location
            else:
                logger.warning(
                    f"No default location found for device with unspecified location",
                    extra={ExtraKeys.DeviceId: observation.device_id},
                )

        # add admin portal configured name to title for water meter geo events
        if (
            isinstance(observation, schemas.GeoEvent)
            and observation.event_type == "water_meter_rep"
        ):
            if device and device.name:
                observation.title += f" - {device.name}"

        # add admin portal configured subject type
        if isinstance(observation, schemas.Position):
            observation.subject_type = coalesce(
                observation.subject_type, device.subject_type
            )
            observation.name = coalesce(observation.name, device.name)

    return observation


def create_blank_device(*, integration_id: str = None, external_id: str = None):
    # Create a placeholder Device
    return schemas.Device(
        id=UUID("{00000000-0000-0000-0000-000000000000}"),
        inbound_configuration=str(integration_id),
        external_id=external_id,
    )


async def ensure_device_integration(integration_id, device_id: str):
    extra_dict = {
        ExtraKeys.AttentionNeeded: True,
        ExtraKeys.InboundIntId: str(integration_id),
        ExtraKeys.DeviceId: device_id,
    }

    cache_key = f"device_detail.{integration_id}.{device_id}"
    cached = await _cache_db.get(cache_key)

    if cached:
        device = schemas.Device.parse_raw(cached)
        logger.info(
            "Using cached Device %s",
            device.external_id,
            extra={"integration_id": integration_id, "device_id": device_id},
        )
        return device

    logger.info(
        "Cache miss for Integration: %s, Device: %s",
        integration_id,
        device_id,
        extra={"integration_id": integration_id, "device_id": device_id},
    )

    try:
        device_data = await _portal.ensure_device(str(integration_id), device_id)
        if device_data:
            # temporary hack to refit response to Device schema.
            device_data["inbound_configuration"] = device_data.get(
                "inbound_configuration", {}
            ).get("id", None)
            device = schemas.Device.parse_obj(device_data)
        else:
            device = create_blank_device(
                integration_id=str(integration_id), external_id=device_id
            )

        if device:  # don't cache empty response
            await _cache_db.setex(cache_key, _cache_ttl, device.json())
        return device

    except Exception as e:
        logger.exception(
            "Error when posting device to Portal.",
            extra={**extra_dict, "device_id": device_id},
        )
        # raise ReferenceDataError("Error when posting device to Portal.")
        # TODO: This is a hack to aleviate load on the portal.
        return create_blank_device(
            integration_id=str(integration_id), external_id=device_id
        )


async def apply_source_configurations(*, observation, gundi_version="v1"):
    if gundi_version == "v2":
        # ToDo: Implement once we process observations
        pass
    else:  # Default to v1
        return await update_observation_with_device_configuration(observation)


async def write_to_cache_safe(key, ttl, instance, extra_dict):
    if not instance:
        logger.warning(
            f"[write_to_cache_safe]> Ignoring null instance.", extra={**extra_dict}
        )
    try:
        await _cache_db.setex(key, ttl, instance.json())
    except redis_exceptions.ConnectionError as e:
        logger.warning(
            f"ConnectionError while writing to Cache: {e}", extra={**extra_dict}
        )
    except Exception as e:
        logger.warning(
            f"Unknown Error while writing to Cache: {e}", extra={**extra_dict}
        )


async def get_connection(*, connection_id):
    connection = None
    extra_dict = {"connection_id": connection_id}
    try:
        cache_key = f"connection_detail.{connection_id}"
        cached_data = await _cache_db.get(cache_key)
        if cached_data:
            logger.debug(
                "Connection details retrieved from cache.",
                extra={**extra_dict, "cache_key": cache_key},
            )
            connection = schemas.v2.Connection.parse_raw(cached_data)
        else:  # Not in cache, retrieve it from the portal
            logger.debug(
                "Cache Miss. Retrieving connection details from the portal..",
                extra={**extra_dict, "cache_key": cache_key},
            )
            try:
                connection = await portal_v2.get_connection_details(
                    integration_id=connection_id
                )
            except Exception as e:
                logger.exception(
                    f"Error while getting connection from the portal: {e}",
                    extra={**extra_dict},
                )
            else:
                await write_to_cache_safe(
                    key=cache_key, ttl=_cache_ttl, instance=connection, extra_dict=extra_dict
                )
    except redis_exceptions.ConnectionError as e:
        logger.exception(
            f"ConnectionError while reading connection details from Cache: {e}",
            extra={**extra_dict},
        )
        connection = None
    except Exception as e:
        logger.exception(
            f"Internal Error while getting connection details: {e}",
            extra={**extra_dict},
        )
    finally:
        return connection


async def get_route(*, route_id):
    route = None
    extra_dict = {"connection_id": route_id}
    try:
        cache_key = f"route_detail.{route_id}"
        cached_data = await _cache_db.get(cache_key)
        if cached_data:
            logger.debug(
                "Route details retrieved from cache.",
                extra={**extra_dict, "cache_key": cache_key},
            )
            route = schemas.v2.Route.parse_raw(cached_data)
        else:  # Not in cache, retrieve it from the portal
            logger.debug(
                "Cache Miss. Retrieving route details from the portal..",
                extra={**extra_dict, "cache_key": cache_key},
            )
            try:
                route = await portal_v2.get_route_details(route_id=route_id)
            except Exception as e:
                logger.exception(
                    f"Error while getting route details from the portal: {e}",
                    extra={**extra_dict},
                )
            else:
                await write_to_cache_safe(
                    key=cache_key, ttl=_cache_ttl, instance=route, extra_dict=extra_dict
                )
    except redis_exceptions.ConnectionError as e:
        logger.exception(
            f"ConnectionError while reading route details from Cache: {e}",
            extra={**extra_dict},
        )
    except Exception as e:
        logger.exception(
            f"Internal Error while getting route details: {e}", extra={**extra_dict}
        )
    finally:
        return route


async def get_integration(*, integration_id):
    integration = None
    extra_dict = {"integration_id": integration_id}
    try:
        cache_key = f"integration_v2_detail.{integration_id}"
        cached_data = await _cache_db.get(cache_key)
        if cached_data:
            logger.debug(
                "Integration details retrieved from cache.",
                extra={**extra_dict, "cache_key": cache_key},
            )
            integration = schemas.v2.Integration.parse_raw(cached_data)
        else:  # Not in cache, retrieve it from the portal
            logger.debug(
                "Cache Miss. Retrieving integration details from the portal..",
                extra={**extra_dict, "cache_key": cache_key},
            )
            try:
                integration = await portal_v2.get_integration_details(
                    integration_id=integration_id
                )
            except Exception as e:
                logger.exception(
                    f"Error while getting integration details from the portal: {e}",
                    extra={**extra_dict},
                )
            else:
                await write_to_cache_safe(
                    key=cache_key, ttl=_cache_ttl, instance=integration, extra_dict=extra_dict
                )
    except redis_exceptions.ConnectionError as e:
        logger.exception(
            f"ConnectionError while reading integration details from Cache: {e}",
            extra={**extra_dict},
        )
    except Exception as e:
        logger.exception(
            f"Internal Error while getting integration details: {e}",
            extra={**extra_dict},
        )
    finally:
        return integration
