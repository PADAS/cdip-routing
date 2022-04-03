import json
import logging
import uuid
from datetime import datetime
from typing import List, Optional, Tuple, Union

import pytz
import smartconnect
import timezonefinder
from cdip_connector.core import schemas
from pydantic import BaseModel
from smartconnect.models import SMARTCONNECT_DATFORMAT, \
    IndependentIncident, ConservationArea
from smartconnect.utils import guess_ca_timezone

from app.subscribers import cache

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
    category_map: Optional[List[CategoryPair]] = []
    attribute_map: Optional[List[AttributeMapper]] = []

class SmartConnectConfigurationAdditional(BaseModel):
    ca_uuid: uuid.UUID
    transformation_rules: Optional[TransformationRules]
    version: Optional[str]


def transform_ca_datamodel(*, er_event: schemas.EREvent = None, ca_datamodel: smartconnect.DataModel = None):
    ca_datamodel.get_category(er_event.event_type)


BLANK_DATAMODEL_CONTENT = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<DataModel xmlns="http://www.smartconservationsoftware.org/xml/1.0/datamodel">
    <languages>
        <language code="en"/>
    </languages>
    <attributes>
        <attribute key="bright_ti4" isrequired="false" type="NUMERIC">
            <aggregations aggregation="avg"/>
            <aggregations aggregation="max"/>
            <aggregations aggregation="min"/>
            <aggregations aggregation="stddev_samp"/>
            <aggregations aggregation="sum"/>
            <aggregations aggregation="var_samp"/>
            <names language_code="en" value="Brightness ti4"/>
        </attribute>
        <attribute key="bright_ti5" isrequired="false" type="NUMERIC">
            <aggregations aggregation="avg"/>
            <aggregations aggregation="max"/>
            <aggregations aggregation="min"/>
            <aggregations aggregation="stddev_samp"/>
            <aggregations aggregation="sum"/>
            <aggregations aggregation="var_samp"/>
            <names language_code="en" value="Brightness ti5"/>
        </attribute>
        <attribute key="fireradiativepower" isrequired="false" type="TEXT">
            <qa_regex></qa_regex>
            <names language_code="en" value="Fire Radiative Power"/>
        </attribute>
        <attribute key="frp" isrequired="false" type="NUMERIC">
            <aggregations aggregation="avg"/>
            <aggregations aggregation="max"/>
            <aggregations aggregation="min"/>
            <aggregations aggregation="stddev_samp"/>
            <aggregations aggregation="sum"/>
            <aggregations aggregation="var_samp"/>
            <names language_code="en" value="Fire Radiative Power"/>
        </attribute>
        <attribute key="confidence" isrequired="false" type="NUMERIC">
            <aggregations aggregation="avg"/>
            <aggregations aggregation="max"/>
            <aggregations aggregation="min"/>
            <aggregations aggregation="stddev_samp"/>
            <aggregations aggregation="sum"/>
            <aggregations aggregation="var_samp"/>
            <names language_code="en" value="Confidence"/>
        </attribute>
        <attribute key="clustered_alerts" isrequired="false" type="NUMERIC">
            <aggregations aggregation="avg"/>
            <aggregations aggregation="max"/>
            <aggregations aggregation="min"/>
            <aggregations aggregation="stddev_samp"/>
            <aggregations aggregation="sum"/>
            <aggregations aggregation="var_samp"/>
            <names language_code="en" value="Clustered Alerts"/>
        </attribute>
    </attributes>
    <categories>
        <category key="gfwfirealert" ismultiple="true" isactive="true" iconkey="fire">
            <names language_code="en" value="GFW Fire Alert"/>
            <attribute isactive="true" attributekey="bright_ti4"/>
            <attribute isactive="true" attributekey="bright_ti5"/>
            <attribute isactive="true" attributekey="frp"/>
            <attribute isactive="true" attributekey="clustered_alerts"/>
        </category>
        <category key="gfwgladalert" ismultiple="true" isactive="true" iconkey="stump">
            <names language_code="en" value="GFW Glad Alert"/>
            <attribute isactive="true" attributekey="confidence"/>
        </category>
    </categories>
</DataModel>
'''

class SmartEREventTransformer:
    '''
    Transform a single EarthRanger Event into an Independent Incident.
    
    TODO: apply transformation rules from SIntegrate configuration.
    
    '''

    def __init__(self, *, config: schemas.OutboundConfiguration = None, **kwargs):
        self._config = config

        self.logger = logging.getLogger(self.__class__.__name__)

        self.ca_uuid = self._config.additional.get('ca_uuid', None)

        self.smartconnect_client = smartconnect.SmartClient(api=config.endpoint, username=config.login,
                                                            password=config.password)

        self._ca_datamodel = self.get_data_model(ca_uuid=self.ca_uuid)


        try:
            self.ca = self.get_conservation_area(ca_uuid=self.ca_uuid)
        except Exception as ex:
            self.logger.warning(f'Failed to get CA Metadata for endpoint: {config.endpoint}, username: {config.login}, CA-UUID: {self.ca_uuid}. Exception: {ex}.')
            self.ca = None

        # Let the timezone fall-back to configuration in the OutboundIntegration.
        try:
            val = self._config.additional.get('timezone', None)
            self._default_timezone = pytz.timezone(val)
        except pytz.exceptions.UnknownTimeZoneError as utze:
            self.logger.warning(f'Configured timezone is {val}, but it is not a known timezone. Defaulting to UTC unless timezone can be inferred from the Conservation Area\'s meta-data.')
            self._default_timezone = pytz.utc
        self.ca_timezone = guess_ca_timezone(self.ca) if self.ca and self.ca.caBoundaryJson else self._default_timezone

        transformation_rules_dict = self._config.additional.get('transformation_rules', {})
        if transformation_rules_dict:
            self._transformation_rules = TransformationRules.parse_obj(transformation_rules_dict)

        self._version = self._config.additional.get('version', "7.0")
        logger.info(f"Using SMART Integration version {self._version}")

    def get_conservation_area(self, *, ca_uuid:str = None):

        cache_key = f'cache:smart-ca:{ca_uuid}:metadata'
        self.logger.info('Looking up CA cached at {cache_key}.')
        try:
            cached_data = cache.cache.get(cache_key)
            if cached_data:
                self.logger.info('Found CA cached at {cache_key}.')
                self.ca = ConservationArea.parse_raw(cached_data)
                return self.ca

            self.logger.info(f'Cache miss for {cache_key}')
        except:
            self.logger.info(f'Cache miss/error for {cache_key}')
            pass

        try:
            self.logger.info('Querying Smart Connect for CAs at endpoint: %s, username: %s', self._config.endpoint, self._config.login)

            for ca in self.smartconnect_client.get_conservation_areas():
                if ca.uuid == uuid.UUID(ca_uuid):
                    self.ca = ca
                    break
            else:
                logger.error('Can\'t find a Conservation Area with UUID: {self.ca_uuid}')
                self.ca = None

            if self.ca:
                self.logger.info(f'Caching CA metadata at {cache_key}')
                cache.cache.setex(name=cache_key, time=60 * 5, value=json.dumps(dict(self.ca), default=str))

            return self.ca

        except Exception as ex:
            self.logger.exception(f'Failed to get Conservation Areas')

    def get_data_model(self, *, ca_uuid:str = None):

            # CA Data Model is not available for versions below 7. Use a blank.
            if self._version.startswith('6'):
                blank_datamodel = smartconnect.DataModel()
                blank_datamodel.load(BLANK_DATAMODEL_CONTENT)
                return blank_datamodel

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

    def resolve_category_path_for_event(self, *, event: [schemas.GeoEvent, schemas.EREvent] = None) -> str:
        '''
        Favor finding a match in the CA Datamodel.
        '''

        matched_category = self._ca_datamodel.get_category(path=event.event_type)

        if not matched_category:
            # convert ER event type to CA path syntax
            matched_category = self._ca_datamodel.get_category(path=str.replace(event.event_type, '_', '.'))

        if matched_category:
            return matched_category['path']
        else:
            for t in self._transformation_rules.category_map:
                if t.event_type == event.event_type:
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

    def _resolve_attributes_for_event(self, *, event: [schemas.GeoEvent, schemas.EREvent] = None) -> dict:
        
        attributes = {}
        for k, v in event.event_details.items():

            # er event detail values that are drop downs are received as lists
            v = v[0] if isinstance(v,list) else v

            k,v = self._resolve_attribute(k, v)

            if k:
                attributes[k] = v
        return attributes

    def transform(self, item) -> dict:
        if self._version == "7.4":
            incident = self.event_to_incident(event=item)
        else:
            incident = self.geoevent_to_incident(geoevent=item)

        return json.loads(incident.json()) if incident else None

    def guess_location_timezone(self, *, longitude:Union[float,int]=None, latitude: Union[float,int]=None):
        '''
        Guess the timezone at the given location. Gracefully fall back on the timezone that's configured for this
        OutboundConfiguration (which will in turn fall back to Utc).
        '''
        try:
            predicted_timezone = timezonefinder.TimezoneFinder().timezone_at(lng=longitude, lat=latitude)
            return pytz.timezone(predicted_timezone)
        except:
            return self._default_timezone

    # TODO: Depreciated use event_to_incident, remove when all integrations on smart connect version > 7.4.x
    def geoevent_to_incident(self, *, geoevent: schemas.GeoEvent = None) -> IndependentIncident:

        # Sanitize coordinates
        coordinates = [geoevent.location.x, geoevent.location.y] if geoevent.location else [0, 0]

        # Apply Transformation Rules

        category_path = self.resolve_category_path_for_event(event=geoevent)

        if not category_path:
            logger.info('No category found for event_type: %s', geoevent.event_type)
            return

        attributes = self._resolve_attributes_for_event(event=geoevent)

        location_timezone = self.guess_location_timezone(longitude=geoevent.location.x, latitude=geoevent.location.y)

        present_localtime = datetime.now(tz=pytz.utc).astimezone(location_timezone)
        geoevent_localtime = geoevent.recorded_at.astimezone(location_timezone)
        
        comment = f'Report: {geoevent.title if geoevent.title else geoevent.event_type}' \
                + f'\nImported: {present_localtime.isoformat()}' 

        incident_data = {
            'type': 'Feature',
            'geometry': {
                'coordinates': coordinates,
                'type': 'Point',
            },

            'properties': {
                'dateTime': geoevent_localtime.strftime(SMARTCONNECT_DATFORMAT),
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

    def event_to_incident(self, *, event: Union[schemas.EREvent, schemas.GeoEvent] = None) -> IndependentIncident:
        """
        Handle both geo events and er events for version > 7.0 of smart connect
        """

        is_er_event = isinstance(event, schemas.EREvent)

        # Sanitize coordinates
        coordinates = [0, 0]
        location_timezone = self._default_timezone
        if event.location:
            if is_er_event:
                coordinates = [event.location.longitude, event.location.latitude]
                location_timezone = self.guess_location_timezone(longitude=event.location.longitude,
                                                                 latitude=event.location.latitude)
            else:
                coordinates = [event.location.x, event.location.y]
                location_timezone = self.guess_location_timezone(longitude=event.location.x,
                                                                 latitude=event.location.y)


        # Apply Transformation Rules

        category_path = self.resolve_category_path_for_event(event=event)

        if not category_path:
            logger.info('No category found for event_type: %s', event.event_type)
            return

        attributes = self._resolve_attributes_for_event(event=event)

        present_localtime = datetime.now(tz=pytz.utc).astimezone(location_timezone)
        event_localtime = event.time.astimezone(location_timezone)

        comment = f'Report: {event.title if event.title else event.event_type}' \
                  + f'\nImported: {present_localtime.isoformat()}'

        # TODO: Handle Update logic

        incident_data = {
            'type': 'Feature',
            'geometry': {
                'coordinates': coordinates,
                'type': 'Point',
            },

            'properties': {
                'dateTime': event_localtime.strftime(SMARTCONNECT_DATFORMAT),
                'smartDataType': 'incident',
                'smartFeatureType': 'waypoint/new',
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
