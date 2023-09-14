import json
import logging
import aiohttp
from typing import List, Union
from uuid import UUID
from gundi_core import schemas
from cdip_connector.core import cdip_settings
from gundi_core.schemas import ERPatrol, ERPatrolSegment
from pydantic import BaseModel, parse_obj_as
from redis import exceptions as redis_exceptions
from app import settings
from app.core.local_logging import ExtraKeys
from app.core.utils import (
    get_redis_db,
    is_uuid,
    ReferenceDataError,
    coalesce,
)
from app.transform_service.smartconnect_transformers import (
    SmartEventTransformer,
    SmartERPatrolTransformer,
    CAConflictException,
    IndeterminableCAException,
)
from app.transform_service.transformers import (
    ERPositionTransformer,
    ERGeoEventTransformer,
    ERCameraTrapTransformer,
    WPSWatchCameraTrapTransformer,
    EREventTransformer,
    ERAttachmentTransformer,
    FieldMappingRule,
    MBPositionTransformer,
    MBObservationTransformer
)
from gundi_client import PortalApi
from gundi_client_v2 import GundiClient


logger = logging.getLogger(__name__)

DEFAULT_LOCATION = schemas.Location(x=0.0, y=0.0)
GUNDI_V1 = "v1"
GUNDI_V2 = "v2"

_portal = PortalApi()
_cache_ttl = settings.PORTAL_CONFIG_OBJECT_CACHE_TTL
_cache_db = get_redis_db()
portal_v2 = GundiClient()  # ToDo: When do we close the client?


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
    cached = _cache_db.get(cache_key)

    if cached:
        configs = OutboundConfigurations.parse_raw(cached).configurations
        logger.debug(
            "Using cached destinations", extra={**extra_dict, "destinations": configs}
        )
        return configs

    logger.debug(f"Cache miss for device_destinations", extra={**extra_dict})

    connect_timeout, read_timeout = settings.DEFAULT_REQUESTS_TIMEOUT
    timeout_settings = aiohttp.ClientTimeout(
        sock_connect=connect_timeout, sock_read=read_timeout
    )
    async with aiohttp.ClientSession(
        timeout=timeout_settings, raise_for_status=True
    ) as s:
        try:
            resp = await _portal.get_outbound_integration_list(
                session=s, inbound_id=str(inbound_id), device_id=str(device_id)
            )
        except aiohttp.ServerTimeoutError as e:
            target_url = str(settings.PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT)
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
            raise ReferenceDataError(f"Failed to connect to the portal at {target_url}, {e}")
        except aiohttp.ClientResponseError as e:
            target_url = str(e.request_info.url)
            logger.exception(
                "Failed to get outbound integrations for inbound_id",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.InboundIntId: inbound_id,
                    ExtraKeys.DeviceId: device_id,
                    ExtraKeys.Url: target_url,
                },
            )
            raise ReferenceDataError(
                f"Failed to get outbound integrations for inbound_id {inbound_id}"
            )
        else:
            try:
                resp_json = [resp] if isinstance(resp, dict) else resp
                configurations = parse_obj_as(
                    List[schemas.OutboundConfiguration], resp_json
                )
            except Exception as e:
                logger.error(
                    f"Failed decoding response for OutboundConfig",
                    extra={**extra_dict, "resp_text": resp},
                )
                raise ReferenceDataError("Failed decoding response for OutboundConfig")
            else:
                configs = OutboundConfigurations(configurations=configurations)
                if configurations:  # don't cache empty response
                    _cache_db.setex(cache_key, _cache_ttl, configs.json())
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
    cached = _cache_db.get(cache_key)

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

    # Rely on default (read:5m). This ought to be fine here, since a busy Portal means we
    # need to wait anyway.
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=cdip_settings.CDIP_ADMIN_SSL_VERIFY),
    ) as sess:
        try:
            device_data = await _portal.ensure_device(
                sess, str(integration_id), device_id
            )

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
                _cache_db.setex(cache_key, _cache_ttl, device.json())
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


class TransformerNotFound(Exception):
    pass


def transform_observation(
    *, stream_type: str, config: schemas.OutboundConfiguration, observation
) -> dict:
    transformer = None
    extra_dict = {
        ExtraKeys.IntegrationType: config.inbound_type_slug,
        ExtraKeys.InboundIntId: observation.integration_id,
        ExtraKeys.OutboundIntId: config.id,
        ExtraKeys.StreamType: stream_type,
    }
    additional_info = {}

    # todo: need a better way than this to build the correct components.
    if (
        stream_type == schemas.StreamPrefixEnum.position
        and config.type_slug == schemas.DestinationTypes.Movebank.value
    ):
        transformer = MBPositionTransformer
        additional_info["integration_type"] = config.inbound_type_slug
        additional_info["gundi_version"] = GUNDI_V1
    elif (
        stream_type == schemas.StreamPrefixEnum.position
        and config.type_slug == schemas.DestinationTypes.EarthRanger.value
    ):
        transformer = ERPositionTransformer
    elif (
        stream_type == schemas.StreamPrefixEnum.geoevent
        and config.type_slug == schemas.DestinationTypes.EarthRanger.value
    ):
        transformer = ERGeoEventTransformer
    elif (
        stream_type == schemas.StreamPrefixEnum.camera_trap
        and config.type_slug == schemas.DestinationTypes.EarthRanger.value
    ):
        transformer = ERCameraTrapTransformer
    elif (
        stream_type == schemas.StreamPrefixEnum.camera_trap
        and config.type_slug == schemas.DestinationTypes.WPSWatch.value
    ):
        transformer = WPSWatchCameraTrapTransformer
    elif (
        stream_type == schemas.StreamPrefixEnum.geoevent
        or stream_type == schemas.StreamPrefixEnum.earthranger_event
    ) and config.type_slug == schemas.DestinationTypes.SmartConnect.value:
        observation, ca_uuid = get_ca_uuid_for_event(event=observation)
        transformer = SmartEventTransformer(config=config, ca_uuid=ca_uuid)
    elif (
        stream_type == schemas.StreamPrefixEnum.earthranger_patrol
        and config.type_slug == schemas.DestinationTypes.SmartConnect.value
    ):
        observation, ca_uuid = get_ca_uuid_for_er_patrol(patrol=observation)
        transformer = SmartERPatrolTransformer(config=config, ca_uuid=ca_uuid)
    if transformer:
        return transformer.transform(observation, **additional_info)
    else:
        logger.error(
            "No transformer found for stream type",
            extra={**extra_dict, ExtraKeys.Provider: config.type_slug},
        )
        raise TransformerNotFound(
            f"No transformer found for {stream_type} dest: {config.type_slug}"
        )


def get_ca_uuid_for_er_patrol(*, patrol: ERPatrol):

    # TODO: This includes critical assumptions, inferring the CA from the contained events.
    segment: ERPatrolSegment
    ca_uuids = set()
    for segment in patrol.patrol_segments:
        for event in segment.event_details:
            event, event_ca_uuid = get_ca_uuid_for_event(event=event)
            if event_ca_uuid:
                ca_uuids.add(event_ca_uuid)

    if len(ca_uuids) > 1:
        raise CAConflictException(
            f"Patrol events are mapped to more than one ca_uuid: {ca_uuids}"
        )

    # if no events are mapped to a ca_uuid, use the leader's ca_uuid
    if not ca_uuids:
        if not segment.leader:
            logger.warning(
                "Patrol has no reports or subject assigned to it",
                extra=dict(patrol=patrol),
            )
            raise IndeterminableCAException("Unable to determine CA uuid for patrol")
        leader_ca_uuid = segment.leader.additional.get("ca_uuid")
        ca_uuids.add(leader_ca_uuid)

    # TODO: this assumes only one ca_uuid is mapped to a patrol
    ca_uuid = ca_uuids.pop()
    return patrol, ca_uuid

import re
def get_ca_uuid_for_event(*args, event: Union[schemas.EREvent, schemas.GeoEvent]):

    assert not args, "get_ca_uuid_for_event takes only keyword args"

    uuid_pattern = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.I)

    """get ca_uuid from prefix of event_type and strip it from event_type"""
    elements = event.event_type.split("_")

    id_list = []
    nonid_list = []

    for element in elements:
        if m := uuid_pattern.match(element):
            id_list.append(m.string)
        else:
            nonid_list.append(element)

    # validation that uuid prefix exists
    ca_uuid = id_list[0] if id_list else None
    # remove ca_uuid prefix if it exists
    event.event_type = "_".join(nonid_list)
    return event, ca_uuid


########################################################################################################################
# GUNDI V2
########################################################################################################################

def get_source_id(observation, gundi_version="v1"):
    return observation.source_id if gundi_version == "v2" else observation.device_id


def get_data_provider_id(observation, gundi_version="v1"):
    return observation.data_provider_id if gundi_version == "v2" else observation.integration_id


async def apply_source_configurations(*, observation, gundi_version="v1"):
    if gundi_version == "v2":
        # ToDo: Implement once we process observations
        pass
    else:  # Default to v1
        return await update_observation_with_device_configuration(observation)


def write_to_cache_safe(key, ttl, instance, extra_dict):
    if not instance:
        logger.warning(
            f"[write_to_cache_safe]> Ignoring null instance.",
            extra={**extra_dict}
        )
    try:
        _cache_db.setex(key, ttl, instance.json())
    except redis_exceptions.ConnectionError as e:
        logger.warning(
            f"ConnectionError while writing to Cache: {e}",
            extra={**extra_dict}
        )
    except Exception as e:
        logger.warning(
            f"Unknown Error while writing to Cache: {e}",
            extra={**extra_dict}
        )


async def get_connection(*, connection_id):
    connection = None
    extra_dict = {
        "connection_id": connection_id
    }
    try:
        cache_key = f"connection_detail.{connection_id}"
        cached_data = _cache_db.get(cache_key)
        if cached_data:
            logger.debug(
                "Connection details retrieved from cache.",
                extra={
                    **extra_dict,
                    "cache_key": cache_key
                },
            )
            connection = schemas.v2.Connection.parse_raw(cached_data)
        else:  # Not in cache, retrieve it from the portal
            logger.debug(
                "Cache Miss. Retrieving connection details from the portal..",
                extra={
                    **extra_dict,
                    "cache_key": cache_key
                },
            )
            connection = await portal_v2.get_connection_details(integration_id=connection_id)
    except redis_exceptions.ConnectionError as e:
        logger.error(
            f"ConnectionError while reading connection details from Cache: {e}", extra={**extra_dict}
        )
        connection = None
    except Exception as e:
        logger.error(
            f"Internal Error while getting connection details: {e}", extra={**extra_dict}
        )
    else:
        write_to_cache_safe(
            key=cache_key,
            ttl=_cache_ttl,
            instance=connection,
            extra_dict=extra_dict
        )
    finally:
        return connection


async def get_route(*, route_id):
    route = None
    extra_dict = {
        "connection_id": route_id
    }
    try:
        cache_key = f"route_detail.{route_id}"
        cached_data = _cache_db.get(cache_key)
        if cached_data:
            logger.debug(
                "Route details retrieved from cache.",
                extra={
                    **extra_dict,
                    "cache_key": cache_key
                },
            )
            route = schemas.v2.Route.parse_raw(cached_data)
        else:  # Not in cache, retrieve it from the portal
            logger.debug(
                "Cache Miss. Retrieving route details from the portal..",
                extra={
                    **extra_dict,
                    "cache_key": cache_key
                },
            )
            route = await portal_v2.get_route_details(route_id=route_id)
    except redis_exceptions.ConnectionError as e:
        logger.error(
            f"ConnectionError while reading route details from Cache: {e}", extra={**extra_dict}
        )
    except Exception as e:
        logger.error(
            f"Internal Error while getting route details: {e}", extra={**extra_dict}
        )
    else:
        write_to_cache_safe(
            key=cache_key,
            ttl=_cache_ttl,
            instance=route,
            extra_dict=extra_dict
        )
    finally:
        return route


async def get_integration(*, integration_id):
    integration = None
    extra_dict = {
        "integration_id": integration_id
    }
    try:
        cache_key = f"integration_v2_detail.{integration_id}"
        cached_data = _cache_db.get(cache_key)
        if cached_data:
            logger.debug(
                "Integration details retrieved from cache.",
                extra={
                    **extra_dict,
                    "cache_key": cache_key
                },
            )
            integration = schemas.v2.Integration.parse_raw(cached_data)
        else:  # Not in cache, retrieve it from the portal
            logger.debug(
                "Cache Miss. Retrieving integration details from the portal..",
                extra={
                    **extra_dict,
                    "cache_key": cache_key
                },
            )
            integration = await portal_v2.get_integration_details(integration_id=integration_id)
    except redis_exceptions.ConnectionError as e:
        logger.error(
            f"ConnectionError while reading integration details from Cache: {e}", extra={**extra_dict}
        )
    except Exception as e:
        logger.error(
            f"Internal Error while getting integration details: {e}", extra={**extra_dict}
        )
    else:
        write_to_cache_safe(
            key=cache_key,
            ttl=_cache_ttl,
            instance=integration,
            extra_dict=extra_dict
        )
    finally:
        return integration

# Map to get the right transformer for the observation type and destination
transformers_map = {
    schemas.v2.StreamPrefixEnum.event.value: {
        schemas.DestinationTypes.EarthRanger.value: EREventTransformer
    },
    schemas.v2.StreamPrefixEnum.attachment.value: {
        schemas.DestinationTypes.EarthRanger.value: ERAttachmentTransformer
    },
    schemas.v2.StreamPrefixEnum.observation.value: {
        schemas.DestinationTypes.Movebank.value: MBObservationTransformer
    },
}


def transform_observation_v2(observation, destination, provider, route_configuration):
    # Look for a proper transformer for this stream type and destination type
    stream_type = observation.observation_type
    destination_type = destination.type.value

    Transformer = transformers_map.get(stream_type, {}).get(destination_type)

    if not Transformer:
        logger.error(
            "No transformer found for stream type & destination type",
            extra={
                ExtraKeys.StreamType: stream_type,
                ExtraKeys.DestinationType: destination_type,
            }
        )
        raise TransformerNotFound(
            f"No transformer found for {stream_type} dest: {destination_type}"
        )

    # Check for extra configurations to apply
    rules = []
    if field_mappings := route_configuration.data.get("field_mappings"):
        configuration = field_mappings.get(
            # First look for configurations for this data provider
            str(observation.data_provider_id), {}
        ).get(  # Then look for configurations for this stream type
            str(stream_type), {}
        ).get(
            # Then look for configurations for this destination
            str(destination.id), {}
        )
        field_mapping_rule = FieldMappingRule(
            map=configuration.get("map", {}),
            source=configuration.get("provider_field", ""),
            target=configuration.get("destination_field", ""),
            default=configuration.get("default")
        )
        rules.append(field_mapping_rule)

    # Apply the transformer
    return Transformer.transform(message=observation, rules=rules, provider=provider, gundi_version=GUNDI_V2)


def transform_observation_to_destination_schema(
    *, observation, destination, provider=None, gundi_version="v1", route_configuration=None
) -> dict:
    if gundi_version == "v2":
        return transform_observation_v2(
            observation=observation,
            destination=destination,
            provider=provider,
            route_configuration=route_configuration
        )
    else:  # Default to v1
        return transform_observation(
            observation=observation,
            stream_type=observation.observation_type,
            config=destination,
        )


def build_transformed_message_attributes(observation, destination, gundi_version, provider_key=None):
    if gundi_version == "v2":
        return {
            "gundi_version": gundi_version,
            "provider_key": provider_key,
            "gundi_id": str(observation.gundi_id),
            "related_to": str(observation.related_to) if observation.related_to else None,
            "stream_type": str(observation.observation_type),
            "source_id": str(observation.source_id),
            "external_source_id": str(observation.external_source_id),
            "destination_id": str(destination.id),
            "data_provider_id": str(get_data_provider_id(observation, gundi_version)),
            "annotations": json.dumps(observation.annotations)
        }
    else:  # default to v1
        return {
            "observation_type": str(observation.observation_type),
            "device_id": str(get_source_id(observation, gundi_version)),
            "outbound_config_id": str(destination.id),
            "integration_id": str(get_data_provider_id(observation, gundi_version)),
        }
