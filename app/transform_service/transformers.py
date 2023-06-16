import json
import logging
from abc import ABC, abstractmethod
from urllib.parse import urlparse
from typing import Any

from gundi_core import schemas
from cdip_connector.core import cdip_settings

logger = logging.getLogger(__name__)

ADMIN_PORTAL_HOST = urlparse(cdip_settings.PORTAL_API_ENDPOINT).hostname


class Transformer(ABC):
    stream_type: schemas.StreamPrefixEnum
    destination_type: schemas.DestinationTypes

    @staticmethod
    @abstractmethod
    def transform(message: schemas.CDIPBaseModel) -> Any:
        ...


class ERPositionTransformer(Transformer):
    # stream_type: schemas.StreamPrefixEnum = schemas.StreamPrefixEnum.position
    # destination_type: str = schemas.DestinationTypes.EarthRanger

    @staticmethod
    def transform(position: schemas.Position) -> dict:
        if not position.location or not position.location.y or not position.location.x:
            logger.warning(f"bad position?? {position}")
        transformed_position = dict(
            manufacturer_id=position.device_id,
            source_type=position.type if position.type else "tracking-device",
            subject_name=position.name if position.name else position.device_id,
            recorded_at=position.recorded_at,
            location={"lon": position.location.x, "lat": position.location.y},
            additional=position.additional,
        )

        # ER does not except null subject_subtype so conditionally add to transformed position if set
        if position.subject_type:
            transformed_position["subject_subtype"] = position.subject_type

        return transformed_position


class ERGeoEventTransformer(Transformer):
    @staticmethod
    def transform(geo_event: schemas.GeoEvent) -> dict:
        return dict(
            title=geo_event.title,
            event_type=geo_event.event_type,
            event_details=geo_event.event_details,
            time=geo_event.recorded_at,
            location=dict(
                longitude=geo_event.location.x, latitude=geo_event.location.y
            ),
        )


class ERCameraTrapTransformer(Transformer):
    @staticmethod
    def transform(payload: schemas.CameraTrap) -> dict:
        return dict(
            file=payload.image_uri,
            camera_name=payload.camera_name,
            camera_description=payload.camera_description,
            time=payload.recorded_at,
            location=json.dumps(
                dict(longitude=payload.location.x, latitude=payload.location.y)
            ),
        )


class WPSWatchCameraTrapTransformer(Transformer):
    @staticmethod
    def transform(payload: schemas.CameraTrap) -> dict:
        # From and To are currently needed for current WPS Watch API
        # camera_name or device_id preceeding @ in 'To' is all that is necessary, other parts just useful for logging
        from_domain_name = f"{payload.integration_id}@{ADMIN_PORTAL_HOST}"
        return dict(
            Attachment1=payload.image_uri,
            Attachments="1",
            From=from_domain_name,
            To=f"{payload.camera_name}@upload.wpswatch.org",
        )
