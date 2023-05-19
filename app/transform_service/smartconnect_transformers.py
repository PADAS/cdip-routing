import base64
import json
import logging
import pathlib
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple, Union

import pytz
import smartconnect
import timezonefinder
from cdip_connector.core import schemas
from cdip_connector.core.cloudstorage import get_cloud_storage
from cdip_connector.core.schemas import ERPatrol, ERPatrolSegment
from pydantic import BaseModel
from smartconnect.models import (
    SMARTCONNECT_DATFORMAT,
    SMARTRequest,
    SMARTCompositeRequest,
    SMARTResponse,
    SmartObservation,
    Geometry,
    Properties,
    SmartAttributes,
    SmartObservationGroup,
    File,
)
from smartconnect.utils import guess_ca_timezone

from app.core.local_logging import ExtraKeys
from app.core.utils import is_uuid, ReferenceDataError
from app.transform_service.transformers import Transformer
from packaging import version

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


class CAConflictException(Exception):
    pass


class IndeterminableCAException(Exception):
    pass


class ObservationUUIDValueException(Exception):
    pass


class SMARTTransformer:
    """
    Transform a single EarthRanger Event into an Independent Incident.

    TODO: apply transformation rules from SIntegrate configuration.

    """

    def __init__(
        self, *args, config: schemas.OutboundConfiguration = None, ca_uuid: str = None
    ):

        assert not args, "SMARTTransformer does not accept positional arguments"

        self._config = config

        self.logger = logging.getLogger(self.__class__.__name__)

        # Handle 0:N SMART CA Mapping
        self.ca_uuid = ca_uuid  # priority to passed in ca_uuid
        if not self.ca_uuid:
            # no passed in value assumes only 1 CA is mapped
            ca_uuids = self._config.additional.get("ca_uuids", None)
            # if not exactly one CA mapped raise Exception
            self.ca_uuid = ca_uuids[0] if len(ca_uuids) == 1 else None
        if not self.ca_uuid:
            raise IndeterminableCAException(
                "Unable to determine CA uuid for observation"
            )

        self._version = self._config.additional.get("version", "7.5")
        logger.info(f"Using SMART Integration version {self._version}")

        self.smartconnect_client = smartconnect.SmartClient(
            api=config.endpoint,
            username=config.login,
            password=config.password,
            version=self._version,
        )

        try:
            self._ca_datamodel = self.smartconnect_client.get_data_model(
                ca_uuid=self.ca_uuid
            )
        except Exception as e:
            logger.exception(
                f"Error getting data model for SMART CA: {self.ca_uuid}",
                extra={ExtraKeys.Error: e},
            )
            raise ReferenceDataError(
                f"Error getting data model for SMART CA: {self.ca_uuid}"
            )

        try:
            self.cm_uuids = config.additional.get('configurable_models_enabled', [])

            self._configurable_models = list([
                self.smartconnect_client.get_configurable_data_model(cm_uuid=cm_uuid)
                for cm_uuid in self.cm_uuids
            ]) or []

        except Exception as e:
            self._ca_config_datamodel = []
            logger.exception(
                f"Error getting config data model for SMART CA: {self.ca_uuid}",
                extra={ExtraKeys.Error: e},
            )

        try:
            self.ca = self.smartconnect_client.get_conservation_area(
                ca_uuid=self.ca_uuid
            )
        except Exception as ex:
            self.logger.warning(
                f"Failed to get CA Metadata for endpoint: {config.endpoint}, username: {config.login}, CA-UUID: {self.ca_uuid}. Exception: {ex}."
            )
            self.ca = None

        # Let the timezone fall-back to configuration in the OutboundIntegration.
        try:
            val = self._config.additional.get("timezone", None)
            self._default_timezone = pytz.timezone(val)
        except pytz.exceptions.UnknownTimeZoneError as utze:
            self.logger.warning(
                f"Configured timezone is {val}, but it is not a known timezone. Defaulting to UTC unless timezone can be inferred from the Conservation Area's meta-data."
            )
            self._default_timezone = pytz.utc
        self.ca_timezone = (
            guess_ca_timezone(self.ca)
            if self.ca and self.ca.caBoundaryJson
            else self._default_timezone
        )

        transformation_rules_dict = self._config.additional.get(
            "transformation_rules", {}
        )
        self._transformation_rules = TransformationRules.parse_obj(
            transformation_rules_dict
        )
        self.cloud_storage = get_cloud_storage()

    def guess_location_timezone(
        self, *, longitude: Union[float, int] = None, latitude: Union[float, int] = None
    ):
        """
        Guess the timezone at the given location. Gracefully fall back on the timezone that's configured for this
        OutboundConfiguration (which will in turn fall back to Utc).
        """
        try:
            predicted_timezone = timezonefinder.TimezoneFinder().timezone_at(
                lng=longitude, lat=latitude
            )
            return pytz.timezone(predicted_timezone)
        except:
            return self._default_timezone

    def resolve_category_path_for_event(
        self, *, event: [schemas.GeoEvent, schemas.EREvent] = None
    ) -> str:
        """
        Favor finding a match in the Config CA Datamodel, then CA Datamodel.
        """

        matched_category = None
        search_for = event.event_type.replace("_", ".")
        for cm in self._configurable_models:
            # favor config datamodel match if present
            # convert ER event type to CA path syntax
            if matched_category := cm.get_category(path=search_for):
                return matched_category["hkeyPath"]


        # direct data model match
        if matched_category := self._ca_datamodel.get_category(path=event.event_type):
            return matched_category['path']

        # convert ER event type to CA path syntax
        if matched_category := self._ca_datamodel.get_category(
                path=str.replace(event.event_type, "_", ".")
            ):
            return matched_category['path']

        # Last option is a match in translation rules.
        for t in self._transformation_rules.category_map:
            if t.event_type == event.event_type:
                return t.category_path

    def _resolve_attribute(self, key, value) -> Tuple[str]:
        attr = None

        # Favor a match in configurable model.
        for cm in self._configurable_models:
            attr = cm.get_attribute(key=key)
            break
        else:
            attr = self._ca_datamodel.get_attribute(key=key)

        # Favor a match in the CA DataModel attributes dictionary.
        if attr:
            return key, value

        return_key = return_value = None
        # Find in transformation rules.
        for amap in self._transformation_rules.attribute_map:
            if amap.from_key == key:
                return_key = amap.to_key
                break
        else:
            logger.warning("No attribute map found for key: %s", key)
            return None, None

        if amap.options_map:
            for options_val in amap.options_map:
                if options_val.from_key == value:
                    return_value = options_val.to_key
                    return return_key, return_value
            if amap.default_option:
                return return_key, amap.default_option

        return return_key, value

    def _resolve_attributes_for_event(
        self, *, event: [schemas.GeoEvent, schemas.EREvent] = None
    ) -> dict:
        attributes = {}
        for k, v in event.event_details.items():
            # some event details are lists like updates
            v = v[0] if isinstance(v, list) and len(v) > 0 else v

            k, v = self._resolve_attribute(k, v)

            if k:
                attributes[k] = v
        return attributes

    def event_to_smart_request(
        self,
        *,
        event: Union[schemas.EREvent, schemas.GeoEvent] = None,
        smart_feature_type=None,
    ) -> SMARTRequest:
        """
        Common code used to construct a SMART request

        """

        is_er_event = isinstance(event, schemas.EREvent)

        # Sanitize coordinates
        coordinates = [0, 0]
        location_timezone = self._default_timezone
        if event.location:
            if is_er_event:
                coordinates = [event.location.longitude, event.location.latitude]
                location_timezone = self.guess_location_timezone(
                    longitude=event.location.longitude, latitude=event.location.latitude
                )
            else:
                coordinates = [event.location.x, event.location.y]
                location_timezone = self.guess_location_timezone(
                    longitude=event.location.x, latitude=event.location.y
                )

        # Apply Transformation Rules

        category_path = self.resolve_category_path_for_event(event=event)

        if not category_path:
            logger.error(f"No category found for event_type: {event.event_type}")
            raise ReferenceDataError(
                f"No category found for event_type: {event.event_type}"
            )

        attributes = self._resolve_attributes_for_event(event=event)

        present_localtime = datetime.now(tz=pytz.utc).astimezone(location_timezone)
        event_localtime = (
            event.time.astimezone(location_timezone)
            if is_er_event
            else event.recorded_at.astimezone(location_timezone)
        )

        comment = (
            f"Report: {event.title if event.title else event.event_type}"
            + f"\nImported: {present_localtime.isoformat()}"
        )

        incident_id = f"ER-{event.serial_number}" if is_er_event else None
        incident_uuid = str(event.id) if is_uuid(id_str=str(event.id)) else None

        # process attachments
        attachments = []
        if is_er_event:
            for event_file in event.files:
                file_extension = pathlib.Path(event_file.get("filename")).suffix
                download_file_name = event_file.get("id") + file_extension
                downloaded_file = self.cloud_storage.download(download_file_name)
                downloaded_file_base64 = base64.b64encode(
                    downloaded_file.getvalue()
                ).decode()
                file = File(
                    filename=event_file.get("filename"), data=downloaded_file_base64
                )
                attachments.append(file)

        smart_data_type = (
            "integrateincident"
            if is_er_event and version.parse(self._version) >= version.parse("7.5.7")
            else "incident"
        )

        # storing custom uuid on reports so that the incident_uuid and observation_uuid are distinct but associated
        observation_uuid = (
            str(event.id)
            if version.parse(self._version) >= version.parse("7.5.3")
            else event.event_details.get("smart_observation_uuid")
        )
        if is_er_event and not observation_uuid:
            raise ObservationUUIDValueException

        # Clean up observation UUID in case it was set to a str(None).
        # TODO: If this resolves the issue, then we should follow-up with a cleaner in SmartObservation.
        if observation_uuid == 'None':
            observation_uuid = str(uuid.uuid4())  # Provide a UUID as str

        smart_observation = SmartObservation(
            observationUuid=observation_uuid,
            category=category_path,
            attributes=attributes,
        )

        smart_attributes = (
            smart_observation
            if smart_feature_type == "waypoint/observation"
            else SmartAttributes(
                incidentId=incident_id,
                incidentUuid=incident_uuid,
                comment=comment,
                observationGroups=[
                    SmartObservationGroup(observations=[smart_observation])
                ],
                attachments=attachments,
            )
        )

        smart_request = SMARTRequest(
            type="Feature",
            geometry=Geometry(coordinates=coordinates, type="Point"),
            properties=Properties(
                dateTime=event_localtime.strftime(SMARTCONNECT_DATFORMAT),
                smartDataType=smart_data_type,
                smartFeatureType=smart_feature_type,
                smartAttributes=smart_attributes,
            ),
        )
        return smart_request

    def event_to_observation(
        self, *, event: Union[schemas.EREvent, schemas.GeoEvent] = None
    ) -> SMARTRequest:
        """
        Handle both geo events and er events for version > 7.5 of smart connect

        Creates an observation update request. New observations are created through event_to_incident
        """

        observation_update_request = self.event_to_smart_request(
            event=event, smart_feature_type="waypoint/observation"
        )

        return observation_update_request

    def event_to_incident(
        self,
        *,
        event: Union[schemas.EREvent, schemas.GeoEvent] = None,
        smart_feature_type=None,
    ) -> SMARTRequest:
        """
        Handle both geo events and er events for version > 7.5 of smart connect
        """

        incident_request = self.event_to_smart_request(
            event=event, smart_feature_type=smart_feature_type
        )

        return incident_request


class SmartEventTransformer(SMARTTransformer, Transformer):
    """
    Transform a single EarthRanger Event into an Independent Incident.

    """

    def __init__(
        self, *, config: schemas.OutboundConfiguration = None, ca_uuid: str, **kwargs
    ):
        super().__init__(config=config, ca_uuid=ca_uuid)

    def transform(self, item) -> dict:
        waypoint_requests = []
        if self._version and version.parse(self._version) >= version.parse("7.5"):
            smart_response = self.smartconnect_client.get_incident(
                incident_uuid=item.id
            )
            if not smart_response:
                incident = self.event_to_incident(
                    event=item, smart_feature_type="waypoint/new"
                )
                waypoint_requests.append(incident)
            else:
                # Update Incident
                incident = self.event_to_incident(
                    event=item, smart_feature_type="waypoint"
                )
                waypoint_requests.append(incident)
                # Update Observation
                observation = self.event_to_observation(event=item)
                waypoint_requests.append(observation)
        else:
            incident = self.geoevent_to_incident(geoevent=item)
            waypoint_requests.append(incident)
        smart_request = SMARTCompositeRequest(
            waypoint_requests=waypoint_requests, ca_uuid=self.ca_uuid
        )

        return json.loads(smart_request.json()) if smart_request else None

    # TODO: Depreciated use event_to_incident, remove when all integrations on smart connect version > 7.5.x
    def geoevent_to_incident(
        self, *, geoevent: schemas.GeoEvent = None
    ) -> SMARTRequest:
        # Sanitize coordinates
        coordinates = (
            [geoevent.location.x, geoevent.location.y] if geoevent.location else [0, 0]
        )

        # Apply Transformation Rules

        category_path = self.resolve_category_path_for_event(event=geoevent)

        if not category_path:
            logger.error(f"No category found for event_type: {geoevent.event_type}")
            raise ReferenceDataError(
                f"No category found for event_type: {geoevent.event_type}"
            )

        attributes = self._resolve_attributes_for_event(event=geoevent)

        location_timezone = self.guess_location_timezone(
            longitude=geoevent.location.x, latitude=geoevent.location.y
        )

        present_localtime = datetime.now(tz=pytz.utc).astimezone(location_timezone)
        geoevent_localtime = geoevent.recorded_at.astimezone(location_timezone)

        comment = (
            f"Report: {geoevent.title if geoevent.title else geoevent.event_type}"
            + f"\nImported: {present_localtime.isoformat()}"
        )

        incident_data = {
            "type": "Feature",
            "geometry": {
                "coordinates": coordinates,
                "type": "Point",
            },
            "properties": {
                "dateTime": geoevent_localtime.strftime(SMARTCONNECT_DATFORMAT),
                "smartDataType": "incident",
                "smartFeatureType": "observation",
                "smartAttributes": {
                    "comment": comment,
                    "observationGroups": [
                        {
                            "observations": [
                                {
                                    "category": category_path,
                                    "attributes": attributes,
                                }
                            ]
                        }
                    ],
                },
            },
        }

        incident_request = SMARTRequest.parse_obj(incident_data)

        return incident_request


class SmartERPatrolTransformer(SMARTTransformer, Transformer):
    def __init__(
        self, *, config: schemas.OutboundConfiguration = None, ca_uuid: str, **kwargs
    ):
        super().__init__(config=config, ca_uuid=ca_uuid)

    def transform(self, item) -> dict:
        smart_request = self.er_patrol_to_smart_patrol(patrol=item)

        return json.loads(smart_request.json()) if smart_request else None

    def get_incident_requests_from_er_patrol_leg(
        self, *, patrol_id, patrol_leg: ERPatrolSegment
    ):
        incident_requests = []
        incident_request: SMARTRequest
        for event in patrol_leg.event_details:
            incident_request = self.event_to_patrol_waypoint(
                patrol_id=patrol_id,
                patrol_leg_id=patrol_leg.id,
                event=event,
                smart_feature_type="waypoint/new",
            )
            incident_requests.append(incident_request)

        return incident_requests

    @staticmethod
    def get_track_point_requests_from_er_patrol_leg(*, patrol_leg: ERPatrolSegment):
        """SMART Connect API throws out duplicate track points so always can post new points
        Updates are not supported by their api"""
        track_point_requests = []
        for track_point in patrol_leg.track_points:
            track_point_data = {
                "geometry": {
                    "coordinates": [
                        track_point.location.longitude,
                        track_point.location.latitude,
                    ],
                    "type": "Point",
                },
                "properties": {
                    "dateTime": track_point.recorded_at.strftime(
                        SMARTCONNECT_DATFORMAT
                    ),
                    "smartDataType": "patrol",
                    "smartFeatureType": "trackpoint/new",
                    "smartAttributes": {"patrolLegUuid": patrol_leg.id},
                },
            }

            track_point_request = SMARTRequest.parse_obj(track_point_data)
            track_point_requests.append(track_point_request)
        return track_point_requests

    def event_to_patrol_waypoint(
        self, *, patrol_id, patrol_leg_id, event, smart_feature_type
    ):
        incident_request = self.event_to_incident(
            event=event, smart_feature_type=smart_feature_type
        )
        # Associate the incident to this patrol leg
        incident_request.properties.smartDataType = "patrol"
        incident_request.properties.smartAttributes.patrolUuid = patrol_id
        incident_request.properties.smartAttributes.patrolLegUuid = patrol_leg_id

        return incident_request

    def er_patrol_to_smart_patrol_request(
        self, patrol: ERPatrol, patrol_leg: ERPatrolSegment, smart_feature_type: str
    ):
        # TODO: what should members be here?
        members = []

        # These should already have been filtered out during sync process pull from ER, but checking again
        if not patrol_leg.start_location or not patrol_leg.leader:
            # Need start location to pass in coordinates and determine location timezone
            logger.warning("patrol leg contains no start location or no leader")
            return None

        coordinates = [
            patrol_leg.start_location.longitude,
            patrol_leg.start_location.latitude,
        ]
        location_timezone = self.guess_location_timezone(
            longitude=patrol_leg.start_location.longitude,
            latitude=patrol_leg.start_location.latitude,
        )
        present_localtime = datetime.now(tz=pytz.utc).astimezone(location_timezone)
        # datetime.strptime(patrol_leg.time_range.get('start_time'), "%Y-%m-%dT%H:%M:%S.%f%z")
        patrol_leg_start_localtime = datetime.fromisoformat(
            patrol_leg.time_range.get("start_time")
        ).astimezone(location_timezone)

        comment = f"\nImported: {present_localtime.isoformat()}"
        for note in patrol.notes:
            comment += note.get("text") + "\n\n"

        # add leg leader to members
        if patrol_leg.leader:
            smart_member_id = patrol_leg.leader.additional.get("smart_member_id")
            if smart_member_id not in members:
                members.append(smart_member_id)

        patrol_request = SMARTRequest(
            type="Feature",
            geometry=Geometry(coordinates=coordinates, type="Point"),
            properties=Properties(
                dateTime=patrol_leg_start_localtime.strftime(SMARTCONNECT_DATFORMAT),
                smartDataType="patrol",
                smartFeatureType=smart_feature_type,
                smartAttributes=SmartAttributes(
                    patrolId=f"ER-{patrol.serial_number}",
                    patrolUuid=patrol.id,
                    patrolLegUuid=patrol_leg.id,
                    team="communityteam1",  # Is there a sensible equivalent on the ER side ?
                    objective=patrol.objective,
                    comment=comment,
                    isArmed="false",
                    # Dont think we have a way to determine this from ER Patrol
                    transportType="foot",  # Potential to base off ER Patrol type
                    mandate="followup",
                    # Dont think we have a way to determine this from ER Patrol
                    members=members,  # are these members specific to the leg or the patrol ?
                    leader=patrol_leg.leader.additional.get("smart_member_id"),
                ),
            ),
        )
        return patrol_request

    def er_patrol_to_smart_patrol(self, patrol: ERPatrol):
        existing_smart_patrol = self.smartconnect_client.get_patrol(patrol_id=patrol.id)

        if existing_smart_patrol:
            patrol_leg = patrol.patrol_segments[0]

            # Update patrol/patrol_leg properties if changed
            patrol_requests = []
            patrol_request = self.er_patrol_to_smart_patrol_request(
                patrol=patrol, patrol_leg=patrol_leg, smart_feature_type="patrol"
            )
            patrol_requests.append(patrol_request)

            # Get waypoints for patrol
            patrol_waypoints = self.smartconnect_client.get_patrol_waypoints(
                patrol_id=patrol.id
            )

            existing_waypoint_uuids = (
                [waypoint.client_uuid for waypoint in patrol_waypoints]
                if patrol_waypoints
                else []
            )

            incident_requests = []
            for event in patrol_leg.event_details:
                # SMART guids are stripped of dashes
                if str(event.er_uuid).replace("-", "") not in existing_waypoint_uuids:
                    if version.parse(self._version) < version.parse("7.5.3"):
                        # check that the event wasn't already created as independent incident and then linked to patrol
                        smart_response = self.smartconnect_client.get_incident(
                            incident_uuid=event.id
                        )
                        if smart_response:
                            logger.info(
                                "skipping event because it already exists in destination outside of patrol"
                            )
                            # version ^7.5.3 will allow us to create waypoint on patrol with same uuid as existing ind inc
                            continue
                    incident_request = self.event_to_patrol_waypoint(
                        patrol_id=patrol.id,
                        patrol_leg_id=patrol_leg.id,
                        event=event,
                        smart_feature_type="waypoint/new",
                    )
                    incident_requests.append(incident_request)
                else:
                    incident_request = self.event_to_patrol_waypoint(
                        patrol_id=patrol.id,
                        patrol_leg_id=patrol_leg.id,
                        event=event,
                        smart_feature_type="waypoint",
                    )
                    incident_requests.append(incident_request)

                    # Update Observation
                    observation = self.event_to_observation(event=event)
                    incident_requests.append(observation)

            track_point_requests = self.get_track_point_requests_from_er_patrol_leg(
                patrol_leg=patrol_leg
            )

            smart_request = SMARTCompositeRequest(
                waypoint_requests=incident_requests,
                patrol_requests=patrol_requests,
                track_point_requests=track_point_requests,
                ca_uuid=self.ca_uuid,
            )

            return smart_request

        else:  # Create Patrol
            patrol_leg: ERPatrolSegment
            # create patrol with first leg, currently ER only supports single leg patrols
            patrol_leg = patrol.patrol_segments[0]

            patrol_request = self.er_patrol_to_smart_patrol_request(
                patrol=patrol, patrol_leg=patrol_leg, smart_feature_type="patrol/new"
            )

            incident_requests = self.get_incident_requests_from_er_patrol_leg(
                patrol_id=patrol.id, patrol_leg=patrol_leg
            )

            track_point_requests = self.get_track_point_requests_from_er_patrol_leg(
                patrol_leg=patrol_leg
            )

            smart_request = SMARTCompositeRequest(
                patrol_requests=[patrol_request],
                waypoint_requests=incident_requests,
                track_point_requests=track_point_requests,
                ca_uuid=self.ca_uuid,
            )

            return smart_request



