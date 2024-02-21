import json
import logging
import pytz
from abc import ABC, abstractmethod
from urllib.parse import urlparse
from typing import Any
from gundi_core import schemas
from app.core import gundi, utils
from app import settings

logger = logging.getLogger(__name__)

ADMIN_PORTAL_HOST = urlparse(settings.PORTAL_API_ENDPOINT).hostname


class Transformer(ABC):
    stream_type: schemas.StreamPrefixEnum
    destination_type: schemas.DestinationTypes

    def __init__(self, *, config=None, **kwargs):
        self.config = config

    @abstractmethod
    async def transform(self, message, rules: list = None, **kwargs) -> Any:
        ...


class ERPositionTransformer(Transformer):
    # stream_type: schemas.StreamPrefixEnum = schemas.StreamPrefixEnum.position
    # destination_type: str = schemas.DestinationTypes.EarthRanger

    async def transform(
        self, position: schemas.Position, rules: list = None, **kwargs
    ) -> dict:
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
    async def transform(
        self, geo_event: schemas.GeoEvent, rules: list = None, **kwargs
    ) -> dict:
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
    async def transform(
        self, payload: schemas.CameraTrap, rules: list = None, **kwargs
    ) -> dict:
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
    async def transform(
        self, payload: schemas.CameraTrap, rules: list = None, **kwargs
    ) -> dict:
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
    async def transform(
        self, position: schemas.Position, rules: list = None, **kwargs
    ) -> dict:
        """
        kwargs:
          - integration_type: manufacturer identifier (coming from inbound) needed for tag_id generation.
          - gundi_version: Gundi version (v1 or v2) used to URN generation.
        """

        def build_tag_id():
            return f"{kwargs.get('integration_type')}.{position.device_id}.{str(position.integration_id)}"

        if not utils.is_valid_position(kwargs.get("gundi_version"), position.location):
            logger.warning(f"bad position?? {position}")
        tag_id = build_tag_id()
        gundi_urn = gundi.build_gundi_urn(
            gundi_version=kwargs.get("gundi_version"),
            integration_id=position.integration_id,
            device_id=position.device_id,
        )
        transformed_position = dict(
            recorded_at=position.recorded_at.astimezone(tz=pytz.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            tag_id=tag_id,
            lon=position.location.x,
            lat=position.location.y,
            sensor_type="GPS",
            tag_manufacturer_name=kwargs.get("integration_type"),
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
    def __init__(self, target: str, default: str, map: dict = None, source: str = None):
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
        if not self.source:
            message[self.target] = self.default
            return

        source_value = self._extract_value(message=message, source=self.source)
        if not self.map:
            message[self.target] = source_value
            return

        message[self.target] = self.map.get(source_value, self.default)


class EREventTransformer(Transformer):
    async def transform(
        self, message: schemas.v2.Event, rules: list = None, **kwargs
    ) -> dict:
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
    async def transform(
        self, message: schemas.v2.Attachment, rules: list = None, **kwargs
    ) -> dict:
        # ToDo. Implement transformation logic for attachments
        return dict(file_path=message.file_path)


class ERObservationTransformer(Transformer):
    async def transform(
        self, message: schemas.v2.Observation, rules: list = None, **kwargs
    ) -> dict:
        if not message.location or not message.location.lat or not message.location.lon:
            logger.warning(f"bad position?? {message}")
        transformed_message = dict(
            manufacturer_id=message.external_source_id,
            source_type=message.type or "tracking-device",
            subject_name=message.source_name or message.external_source_id,
            recorded_at=message.recorded_at,
            location={"lon": message.location.lon, "lat": message.location.lat},
            additional=message.additional,
        )

        # ER does not accept null subject_subtype so conditionally add to transformed position if set
        if subject_type := message.subject_type:
            transformed_message["subject_subtype"] = subject_type

        # Apply extra transformation rules as needed
        if rules:
            for rule in rules:
                rule.apply(message=transformed_message)

        return transformed_message


class MBObservationTransformer(Transformer):
    async def transform(
        self, message: schemas.v2.Observation, rules: list = None, **kwargs
    ) -> dict:
        """
        kwargs:
          - provider: manufacturer object (coming from provider config) needed for tag_id generation.
        """
        provider = kwargs.get("provider")

        def build_tag_id():
            return f"{provider.type.value}.{message.external_source_id}.{str(message.data_provider_id)}"

        if not utils.is_valid_position(kwargs.get("gundi_version"), message.location):
            logger.warning(f"bad position?? {message}")
        tag_id = build_tag_id()
        gundi_urn = gundi.build_gundi_urn(
            gundi_version=kwargs.get("gundi_version"),
            integration_id=message.data_provider_id,
            device_id=message.external_source_id,
        )
        transformed_position = dict(
            recorded_at=message.recorded_at.astimezone(tz=pytz.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            tag_id=tag_id,
            lon=message.location.lon,
            lat=message.location.lat,
            sensor_type="GPS",
            tag_manufacturer_name=provider.type.name,
            gundi_urn=gundi_urn,
        )

        return transformed_position