import logging
import aiohttp
from typing import List, Union
from uuid import UUID
from gundi_core import schemas
from cdip_connector.core import cdip_settings
from gundi_core.schemas import ERPatrol, ERPatrolSegment
from pydantic import BaseModel, parse_obj_as
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
)
from gundi_client import PortalApi
from gundi_client_v2 import GundiClient

logger = logging.getLogger(__name__)

DEFAULT_LOCATION = schemas.Location(x=0.0, y=0.0)

_portal = PortalApi()
_cache_ttl = settings.PORTAL_CONFIG_OBJECT_CACHE_TTL
_cache_db = get_redis_db()
portal_v2 = GundiClient()


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
            observation.subject_name = coalesce(observation.name, device.name)

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
        ExtraKeys.InboundIntId: observation.integration_id,
        ExtraKeys.OutboundIntId: config.id,
        ExtraKeys.StreamType: stream_type,
    }

    # todo: need a better way than this to build the correct components.
    if (
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
        return transformer.transform(observation)
    else:
        logger.error(
            "No transformer found for stream type",
            extra={**extra_dict, ExtraKeys.Provider: config.type_slug},
        )
        raise TransformerNotFound(
            f"No transformer found for {stream_type} dest: {config.type_slug}"
        )


def get_ca_uuid_for_er_patrol(*, patrol: ERPatrol):
    segment: ERPatrolSegment
    ca_uuids = []
    for segment in patrol.patrol_segments:
        for event in segment.event_details:
            event, event_ca_uuid = get_ca_uuid_for_event(event=event)
            if event_ca_uuid not in ca_uuids:
                ca_uuids.append(event_ca_uuid)
    if len(ca_uuids) > 1:
        raise CAConflictException(
            f"Patrol events are mapped to more than one ca_uuid: {ca_uuids}"
        )
    if not ca_uuids:
        if not segment.leader:
            logger.warning(
                "Patrol has no reports or subject assigned to it",
                extra=dict(patrol=patrol),
            )
            raise IndeterminableCAException("Unable to determine CA uuid for patrol")
        leader_ca_uuid = segment.leader.additional.get("ca_uuid")
        ca_uuids.append(leader_ca_uuid)
    ca_uuid = ca_uuids[0]
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


def transform_observation_to_destination_schema(
    *, observation, destination, gundi_version="v1", route_configurations=None
) -> dict:
    if gundi_version == "v2":
        # ToDo: Apply extra configurations
        pass
    else:  # Default to v1
        return transform_observation(
            observation=observation,
            stream_type=observation.observation_type,
            config=destination,
        )
