import json
import logging
import uuid
from abc import ABC
from datetime import datetime
from typing import List, Optional, Tuple, Union

import pytz
import smartconnect
import timezonefinder
from cdip_connector.core import schemas
from cdip_connector.core.schemas import ERPatrol, ERPatrolSegment
from pydantic import BaseModel
from smartconnect.models import SMARTCONNECT_DATFORMAT, \
    SMARTRequest, ConservationArea, SMARTCompositeRequest, SMARTResponse, Patrol
from smartconnect.utils import guess_ca_timezone

from app.subscribers import cache
from app.transform_service.transformers import Transformer

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


class SMARTTransformer():

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
            self.logger.warning(
                f'Failed to get CA Metadata for endpoint: {config.endpoint}, username: {config.login}, CA-UUID: {self.ca_uuid}. Exception: {ex}.')
            self.ca = None

        # Let the timezone fall-back to configuration in the OutboundIntegration.
        try:
            val = self._config.additional.get('timezone', None)
            self._default_timezone = pytz.timezone(val)
        except pytz.exceptions.UnknownTimeZoneError as utze:
            self.logger.warning(
                f'Configured timezone is {val}, but it is not a known timezone. Defaulting to UTC unless timezone can be inferred from the Conservation Area\'s meta-data.')
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
            return key, value  # TODO: also lookup value in DataModel.

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
            v = v[0] if isinstance(v, list) else v

            k, v = self._resolve_attribute(k, v)

            if k:
                attributes[k] = v
        return attributes

    def event_to_incident(self, *, event: Union[schemas.EREvent, schemas.GeoEvent] = None) -> SMARTRequest:
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
                    'incidentUuid': str(event.id),
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

        incident = SMARTRequest.parse_obj(incident_data)

        return incident


class SmartEREventTransformer(SMARTTransformer, Transformer):
    '''
    Transform a single EarthRanger Event into an Independent Incident.
    
    '''

    def __init__(self, *, config: schemas.OutboundConfiguration = None, **kwargs):
        super().__init__(config=config)

    def transform(self, item) -> dict:
        if self._version and float(self._version) == "7.5":
            incident = self.event_to_incident(event=item)
            existing_incident = self.smartconnect_client.get_incident(incident_uuid=item.id)
            if existing_incident:
                # TODO Update Incident
                return None
        else:
            incident = self.geoevent_to_incident(geoevent=item)
        smart_request = SMARTCompositeRequest(waypoint_requests=[incident])

        return json.loads(smart_request.json()) if smart_request else None

    # TODO: Depreciated use event_to_incident, remove when all integrations on smart connect version > 7.5.x
    def geoevent_to_incident(self, *, geoevent: schemas.GeoEvent = None) -> SMARTRequest:

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

        incident_request = SMARTRequest.parse_obj(incident_data)

        return incident_request


class SmartERPatrolTransformer(SMARTTransformer, Transformer):
    def __init__(self, *, config: schemas.OutboundConfiguration = None, **kwargs):
        super().__init__(config=config)

    def transform(self, item) -> dict:
        smart_request = self.er_patrol_to_smart_patrol(patrol=item)

        return json.loads(smart_request.json()) if smart_request else None

    def get_incident_requests_from_er_patrol_leg(self, *, patrol_id, patrol_leg: ERPatrolSegment):
        incident_requests = []
        incident_request: SMARTRequest
        for event in patrol_leg.event_details:
            incident_request = self.event_to_patrol_waypoint(patrol_id=patrol_id, patrol_leg_id=patrol_leg.id, event=event)
            incident_request.properties.smartFeatureType = 'waypoint/new'
            incident_requests.append(incident_request)

    def event_to_patrol_waypoint(self, *, patrol_id, patrol_leg_id, event):
        incident_request = self.event_to_incident(event=event)
        # Associate the incident to this patrol leg
        incident_request.properties.smartDataType = 'patrol'
        incident_request.properties.smartAttributes.patrolUuid = patrol_id
        incident_request.properties.smartAttributes.patrolLegUuid = patrol_leg_id

        return incident_request

    def er_patrol_to_smart_patrol(self, patrol: ERPatrol):
        existing_smart_patrol = self.smartconnect_client.get_patrol(patrol_id=patrol.id)

        if existing_smart_patrol:
            # TODO: Update patrol/patrol_leg properties if changed
            # Get waypoints for patrol
            patrol_waypoints = self.smartconnect_client.get_patrol_waypoints(patrol_id=patrol.id)
            patrol_leg = patrol.patrol_segments[0]

            existing_waypoint_uuids = [waypoint.client_uuid for waypoint in patrol_waypoints] if patrol_waypoints else []

            incident_requests = []
            for event in patrol_leg.event_details:
                # SMART guids are stripped of dashes
                if str(event.er_uuid).replace('-', '') not in existing_waypoint_uuids:
                    incident_request = self.event_to_patrol_waypoint(patrol_id=patrol.id, patrol_leg_id=patrol_leg.id, event=event)
                    incident_request.properties.smartFeatureType = 'waypoint/new'
                    incident_requests.append(incident_request)
                else:
                    # TODO: Update logic for patrol waypoints
                    pass

            smart_request = SMARTCompositeRequest(waypoint_requests=incident_requests,
                                                  patrol_requests=[])

            return smart_request

        else:  # Create Patrol

            location_timezone = self._default_timezone

            members = []

            patrol_leg: ERPatrolSegment
            # create patrol with first leg, currently ER only supports single leg patrols
            patrol_leg = patrol.patrol_segments[0]

            # TODO: Revisit if this is the right spot to abandon the flow
            if not patrol_leg.start_location:
                # Need start location to pass in coordinates and determine location timezone
                logger.warning("patrol leg contains no start location")
                return None

            if not patrol_leg.leader:
                logger.warning("patrol leg contains no leader")
                return None

            coordinates = [patrol_leg.start_location.longitude, patrol_leg.start_location.latitude]
            location_timezone = self.guess_location_timezone(longitude=patrol_leg.start_location.longitude,
                                                             latitude=patrol_leg.start_location.latitude)
            present_localtime = datetime.now(tz=pytz.utc).astimezone(location_timezone)
            # datetime.strptime(patrol_leg.time_range.get('start_time'), "%Y-%m-%dT%H:%M:%S.%f%z")
            patrol_leg_start_localtime = datetime.fromisoformat(patrol_leg.time_range.get('start_time')).astimezone(location_timezone)

            comment = f'Patrol: {patrol.title}' \
                      + f'\nImported: {present_localtime.isoformat()}'
            for note in patrol.notes:
                comment += note.get('text') + '\n\n'

            # add leg leader to members
            if patrol_leg.leader:
                smart_member_id = patrol_leg.leader.get('additional').get('smart_member_id')
                if smart_member_id not in members:
                    members.append(smart_member_id)

            patrol_data = {
                "type": "Feature",
                "geometry": {
                    "coordinates": coordinates,
                    "type": "Point"
                },
                "properties": {
                    "dateTime": patrol_leg_start_localtime.strftime(SMARTCONNECT_DATFORMAT),

                    "smartDataType": "patrol",
                    "smartFeatureType": "patrol/new",
                    "smartAttributes": {
                        "patrolUuid": patrol.id,
                        "patrolLegUuid": patrol_leg.id,
                        "team": "communityteam1",
                        "objective": patrol.objective,
                        "comment": comment,
                        "isArmed": "false", # Dont think we have a way to determine this from ER Patrol
                        "transportType": "foot", # Potential to base off ER Patrol type
                        "mandate": "followup", # Dont think we have a way to determine this from ER Patrol
                        "number": -999, # ???
                        "members": members, # are these members specific to the leg or the patrol ?
                        "leader": patrol_leg.leader.get('additional').get('smart_member_id')
                    }
                }
            }

            patrol_request = SMARTRequest.parse_obj(patrol_data)

            incident_requests = self.get_incident_requests_from_er_patrol_leg(patrol_id=patrol.id, patrol_leg=patrol_leg)

            smart_request = SMARTCompositeRequest(patrol_requests=[patrol_request],
                                                  waypoint_requests=incident_requests)

            return smart_request



