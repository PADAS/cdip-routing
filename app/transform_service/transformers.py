import logging
from abc import ABC, abstractmethod
from typing import Any

from cdip_connector.core import schemas

logger = logging.getLogger(__name__)


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
            logger.warning(f'bad position?? {position}')
        return dict(manufacturer_id=position.device_id,
                    source_type=position.type if position.type else 'tracking-device',
                    subject_name=position.name if position.name else position.device_id,
                    recorded_at=position.recorded_at,
                    location={
                        'lon': position.location.x,
                        'lat': position.location.y
                    },
                    additional=position.additional
                    )


class ERGeoEventTransformer(Transformer):
    @staticmethod
    def transform(geo_event: schemas.GeoEvent) -> dict:
        return dict(title=geo_event.title,
                    event_type=geo_event.event_type,
                    event_details=geo_event.event_details,
                    time=geo_event.recorded_at,
                    location=dict(longitude=geo_event.location.x,
                                  latitude=geo_event.location.y)
                    )