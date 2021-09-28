from datetime import datetime, timedelta, timezone
from xml.sax import parseString
import pytz
import logging
import uuid
import json
from app.subscribers import cache
from pydantic import BaseModel, Field
from typing import List,Dict, Any, Optional, Tuple
from enum import Enum
# from models import TransformationRules
# from transform_service.transformers import Transformer, ERGeoEventTransformer, ERPositionTransformer

from cdip_connector.core import schemas
import urllib.parse as uparse

import smartconnect
from smartconnect.utils import guess_ca_timezone
from smartconnect.models import SmartAttributes, SmartObservation, SMARTCONNECT_DATFORMAT, \
 SmartObservationGroup, IncidentProperties, IndependentIncident

logger = logging.getLogger(__name__)

# Smart Connect Outbound configuration models.
class CategoryPair(BaseModel):
    event_type: str
    category_path: str

# class CategoriesMap(BaseModel):
#     pairs: List[CategoryPair]


class OptionMap(BaseModel):
    from_key: str
    to_key: str

class AttributeMapper(BaseModel):
    from_key: str
    to_key: str
    type: Optional[str] = "string"
    options_map: Optional[List[OptionMap]]
    default_option: Optional[str]
    event_types: Optional[List[str]]

class TransformationRules(BaseModel):
    category_map: List[CategoryPair]
    attribute_map: List[AttributeMapper]

class SmartConnectConfigurationAdditional(BaseModel):
    ca_uuid: uuid.UUID
    transformation_rules: Optional[TransformationRules]


def transform_ca_datamodel(*, er_event: schemas.EREvent = None, ca_datamodel: smartconnect.DataModel = None):
    ca_datamodel.get_category(er_event.event_type)

class SmartEREventTransformer:
    '''
    Transform a single EarthRanger Event into an Independent Incident.
    
    TODO: apply transformation rules from SIntegrate configuration.
    
    '''

    def __init__(self, *, config: schemas.OutboundConfiguration = None, ca_datamodel: smartconnect.DataModel = None):
        self._config = config

        self.ca_uuid = self._config.additional.get('ca_uuid', None)

        self.smartconnect_client = smartconnect.SmartClient(api=config.endpoint, username=config.login, password=config.password)

        self._ca_datamodel = self.get_data_model(ca_uuid=self.ca_uuid) 
        #self.smartconnect_client.download_datamodel(ca_uuid=self.ca_uuid)

        for ca in self.smartconnect_client.get_conservation_areas():
            if ca.uuid == uuid.UUID(self.ca_uuid):
                self.ca = ca
                break
        else:
            logger.error('Can\'t find a Conservation Area with UUID: {self.ca_uuid}')
            self.ca = None

        self.ca_timezone = guess_ca_timezone(self.ca) if ca else None

        transformation_rules_dict = self._config.additional.get('transformation_rules', None)
        if transformation_rules_dict:
            self._transformation_rules = TransformationRules.parse_obj(transformation_rules_dict)

    def get_data_model(self, *, ca_uuid:str):


            cache_key = f'cache:smart-ca:{ca_uuid}:datamodel'
            try:
                cached_data = cache.cache.get(cache_key)
                if cached_data:
                    dm = smartconnect.DataModel()
                    dm.import_from_dict(json.loads(cached_data))
                    return dm
            except Exception:
                pass

            ca_datamodel = self.smartconnect_client.download_datamodel(ca_uuid=self.ca_uuid)
            cache.cache.setex(name=cache_key, time=60*5, value=json.dumps(ca_datamodel.export_as_dict()))
            return ca_datamodel


    def resolve_category_path_for_event(self, *, er_event:schemas.GeoEvent=None) -> str:
        '''
        Favor finding a match in the CA Datamodel.
        '''

        matched_category = self._ca_datamodel.get_category(path=er_event.event_type)
        if matched_category:
            return matched_category['path']
        else:
            for t in self._transformation_rules.category_map:
                if t.event_type == er_event.event_type:
                    return t.category_path

    def _resolve_attribute(self, key, value) -> Tuple[str]:

        attr = self._ca_datamodel.get_attribute(key=key)

        # Favor a match in the CA DataModel attributes dictionary.
        if attr:
            return key, value # TODO: also lookup value in DataModel.

        return_key = return_value = None
        # Find in transformation rules.
        for amap in self._transformation_rules.attribute_map:
            if amap.from_key == key:
                return_key = amap.to_key
                break
        else:
            logger.warning('No attribute map found for key: %s', key)
            return None, None
        
        if amap.options_map:
            for options_val in amap.options_map:
                if options_val.from_key == value: 
                    return_value = options_val.to_key
                    return return_key, return_value
            if amap.default_option:
                return return_key, amap.default_option
        
        return return_key, value


    def _resolve_attributes_for_event(self, *, geoevent:schemas.GeoEvent= None) -> dict:
        
        attributes = {}
        for k, v in geoevent.event_details.items():

            k,v = self._resolve_attribute(k, v)

            if k:
                attributes[k] = v
        return attributes


    def transform(self, item) -> dict:
        incident = self.geoevent_to_incident(geoevent=item)

        return json.loads(incident.json()) if incident else None

    def geoevent_to_incident(self, *, geoevent: schemas.GeoEvent = None) -> IndependentIncident:

        # Sanitize coordinates
        coordinates = [geoevent.location.x, geoevent.location.y] if geoevent.location else [0, 0]

        # Apply Transformation Rules

        category_path = self.resolve_category_path_for_event(er_event=geoevent)

        if not category_path:
            logger.info('No category found for event_type: %s', geoevent.event_type)
            return

        attributes = self._resolve_attributes_for_event(geoevent=geoevent)

        present_time = datetime.now(tz=pytz.utc)
        comment = f'Report: {geoevent.title if geoevent.title else geoevent.event_type}' \
                + f'\nImported: {present_time.isoformat()}' 


        incident_data = {
            'type': 'Feature',
            'geometry': {
                'coordinates': coordinates,
                'type': 'Point',
            },

            'properties': {
                'dateTime': geoevent.recorded_at.strftime(SMARTCONNECT_DATFORMAT),
                'smartDataType': 'incident',
                'smartFeatureType': 'observation',
                'smartAttributes': {
                    'comment': comment,
                    'observationGroups': [
                        {
                            'observations': [
                                {
                                    'category': category_path,
                                    'attributes': attributes,

                                }
                            ]
                        }
                    ]

                }
            }
        }

        incident = IndependentIncident.parse_obj(incident_data)

        return incident






