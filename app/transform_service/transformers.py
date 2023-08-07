import json
import logging
import pytz
from abc import ABC, abstractmethod
from urllib.parse import urlparse
from typing import Any

from gundi_core import schemas
from cdip_connector.core import cdip_settings
from app.transform_service import helpers

logger = logging.getLogger(__name__)

ADMIN_PORTAL_HOST = urlparse(cdip_settings.PORTAL_API_ENDPOINT).hostname


class Transformer(ABC):
    stream_type: schemas.StreamPrefixEnum
    destination_type: schemas.DestinationTypes

    @staticmethod
    @abstractmethod
    def transform(message: schemas.CDIPBaseModel, rules: list = None, **kwargs) -> Any:
        ...


class ERPositionTransformer(Transformer):
    # stream_type: schemas.StreamPrefixEnum = schemas.StreamPrefixEnum.position
    # destination_type: str = schemas.DestinationTypes.EarthRanger

    @staticmethod
    def transform(position: schemas.Position, rules: list = None, **kwargs) -> dict:
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
    def transform(geo_event: schemas.GeoEvent, rules: list = None, **kwargs) -> dict:
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
    def transform(payload: schemas.CameraTrap, rules: list = None, **kwargs) -> dict:
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
    def transform(payload: schemas.CameraTrap, rules: list = None, **kwargs) -> dict:
        # From and To are currently needed for current WPS Watch API
        # camera_name or device_id preceeding @ in 'To' is all that is necessary, other parts just useful for logging
        from_domain_name = f"{payload.integration_id}@{ADMIN_PORTAL_HOST}"
        return dict(
            Attachment1=payload.image_uri,
            Attachments="1",
            From=from_domain_name,
            To=f"{payload.camera_name}@upload.wpswatch.org",
        )


class MBPositionTransformer(Transformer):
    @staticmethod
    def transform(position: schemas.Position, rules: list = None, **kwargs) -> dict:
        """
             kwargs:
               - integration_type: manufacturer identifier (coming from inbound) needed for tag_id generation.
               - gundi_version: Gundi version (v1 or v2) used to URN generation.
        """
        def build_tag_id():
            return f"{kwargs.get('integration_type')}.{position.device_id}.{str(position.integration_id)}"

        if not position.location or not position.location.y or not position.location.x:
            logger.warning(f"bad position?? {position}")
        tag_id = build_tag_id()
        gundi_urn = helpers.build_gundi_urn(
            gundi_version=kwargs.get("gundi_version"),
            integration_id=position.integration_id,
            device_id=position.device_id
        )
        transformed_position = dict(
            recorded_at=position.recorded_at.astimezone(tz=pytz.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
            tag_id=tag_id,
            lon=position.location.x,
            lat=position.location.y,
            sensor_type="GPS",
            tag_manufacturer_id=position.name,
            gundi_urn=gundi_urn,
        )

        return transformed_position


########################################################################################################################
# GUNDI V2
########################################################################################################################
class TransformationRule(ABC):

    @staticmethod
    @abstractmethod
    def apply(self, message: dict, **kwargs):
        ...


class FieldMappingRule(TransformationRule):

    def __init__(self, map: dict, source: str, target: str, default: str):
        self.map = map
        self.source = source
        self.target = target
        self.default = default

    def _extract_value(self, message, source):
        fields = source.lower().strip().split("__")
        value = message
        while fields:
            field = fields.pop(0)
            value = value.get(field)
        return value

    def apply(self, message: dict, **kwargs):
        message[self.target] = self.map.get(
            self._extract_value(
                message=message, source=self.source
            ),
            self.default
        )


class EREventTransformer(Transformer):
    @staticmethod
    def transform(message: schemas.v2.Event, rules: list = None) -> dict:
        transformed_message = dict(
            title=message.title,
            event_type=message.event_type,
            event_details=message.event_details,
            time=message.recorded_at,
            location=dict(
                longitude=message.location.lon, latitude=message.location.lat
            ),
        )
        # Apply extra transformation rules as needed
        if rules:
            for rule in rules:
                rule.apply(message=transformed_message)
        return transformed_message


class ERAttachmentTransformer(Transformer):
    @staticmethod
    def transform(message: schemas.v2.Attachment, rules: list = None) -> dict:
        # ToDo. Implement transformation logic for attachments
        return dict(
            file_path=message.file_path
        )
