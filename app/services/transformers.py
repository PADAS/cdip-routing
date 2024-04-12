import json
import base64
import logging
import pytz
import pathlib
import uuid
from abc import ABC, abstractmethod
from urllib.parse import urlparse
from typing import Any, List, Union, Optional, Tuple
from datetime import datetime
from pydantic.types import UUID
from app.core import gundi, utils
from app import settings
import timezonefinder
from packaging import version
from gundi_core import schemas
from gundi_core.schemas import ERPatrol, ERPatrolSegment
from gundi_core.schemas.v2 import (
    SMARTTransformationRules,
    SMARTPushEventActionConfig,
    SMARTAuthActionConfig,
)
from pydantic import BaseModel
from smartconnect import AsyncSmartClient
from smartconnect.models import (
    SMARTCONNECT_DATFORMAT,
    SMARTRequest,
    SMARTCompositeRequest,
    SmartObservation,
    Geometry,
    Properties,
    SmartAttributes,
    SmartObservationGroup,
    File,
)
from smartconnect.utils import guess_ca_timezone
from app.core.gundi import GUNDI_V1, GUNDI_V2
from app.core.local_logging import ExtraKeys
from app.core.utils import is_uuid
from app.core.errors import (
    ReferenceDataError,
    ObservationUUIDValueException,
    TransformerNotFound,
    CAConflictException,
    IndeterminableCAException,
)

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
# SMART
########################################################################################################################
# Legacy config for Gundi v1 additional field
class SmartConnectConfigurationAdditional(BaseModel):
    ca_uuid: UUID
    transformation_rules: Optional[SMARTTransformationRules]
    version: Optional[str]


class SMARTTransformer:
    """
    Transform a single EarthRanger Event into an Independent Incident.

    TODO: apply transformation rules from Sintegrate configuration.

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

        self.smartconnect_client = AsyncSmartClient(
            api=config.endpoint,
            username=config.login,
            password=config.password,
            version=self._version,
        )
        self._ca_datamodel = None
        self._configurable_models = None
        self.ca = None
        self.cm_uuids = config.additional.get("configurable_models_enabled", [])

        # Let the timezone fall-back to configuration in the OutboundIntegration.
        try:
            val = self._config.additional.get("timezone", None)
            self._default_timezone = pytz.timezone(val)
        except pytz.exceptions.UnknownTimeZoneError as utze:
            self.logger.warning(
                f"Configured timezone is {val}, but it is not a known timezone. Defaulting to UTC unless timezone can be inferred from the Conservation Area's meta-data."
            )
            self._default_timezone = pytz.utc
        self.ca_timezone = guess_ca_timezone(self.ca) or self._default_timezone

        transformation_rules_dict = self._config.additional.get(
            "transformation_rules", {}
        )
        self._transformation_rules = SMARTTransformationRules.parse_obj(
            transformation_rules_dict
        )

    async def get_ca(self, config):
        if not self.ca:
            try:
                self.ca = await self.smartconnect_client.get_conservation_area(
                    ca_uuid=self.ca_uuid
                )
            except Exception as ex:
                self.logger.warning(
                    f"Failed to get CA Metadata for endpoint: {config.base_url}, username: {config.login}, CA-UUID: {self.ca_uuid}. Exception: {ex}."
                )
                self.ca = None
        return self.ca

    async def get_configurable_models(self):
        if not self._configurable_models:
            try:
                self._configurable_models = list(
                    [
                        await self.smartconnect_client.get_configurable_data_model(
                            cm_uuid=cm_uuid
                        )
                        for cm_uuid in self.cm_uuids
                    ]
                )
            except Exception as e:
                self._ca_config_datamodel = []
                logger.exception(
                    f"Error getting config data model for SMART CA: {self.ca_uuid}",
                    extra={ExtraKeys.Error: e},
                )
        return self._configurable_models

    async def get_ca_datamodel(self):
        if not self._ca_datamodel:
            try:
                self._ca_datamodel = await self.smartconnect_client.get_data_model(
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
        return self._ca_datamodel

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

    async def resolve_category_path_for_event(
        self, *, event: [schemas.GeoEvent, schemas.EREvent] = None
    ) -> str:
        """
        Favor finding a match in the Config CA Datamodel, then CA Datamodel.
        """

        # Remove the uuid prefix from the event type if present
        event_type = event.event_type
        event_type_parts = event.event_type.split("_")
        if event_type_parts and is_uuid(id_str=event_type_parts[0]):
            event_type = "_".join(event_type_parts[1:])

        # Convert ER event type to CA path syntax
        event_type = event_type.replace("_", ".")
        ca_datamodel = await self.get_ca_datamodel()
        # Look in the CA DataModel
        if matched_category := ca_datamodel.get_category(path=event_type):
            return matched_category["path"]

        # Look in configurable models
        configurable_models = await self.get_configurable_models()
        for cm in configurable_models:
            # favor config datamodel match if present
            # convert ER event type to CA path syntax
            if matched_category := cm.get_category(path=event_type):
                return matched_category["hkeyPath"]

        # Try a direct data model match
        if matched_category := ca_datamodel.get_category(path=event.event_type):
            return matched_category["path"]

        # Last option is a match in translation rules.
        for t in self._transformation_rules.category_map:
            if t.event_type == event.event_type:
                return t.category_path

    async def _resolve_attribute(
        self, key, value
    ) -> Tuple[Union[str, None], Union[str, None]]:
        attr = None

        # Favor a match in configurable model.
        configurable_models = await self.get_configurable_models()
        for cm in configurable_models:
            attr = cm.get_attribute(key=key)
            if attr:
                break
        else:
            ca_datamodel = await self.get_ca_datamodel()
            attr = ca_datamodel.get_attribute(key=key)

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

    async def _resolve_attributes_for_event(
        self, *, event: [schemas.GeoEvent, schemas.EREvent] = None
    ) -> dict:
        attributes = {}
        for k, v in event.event_details.items():
            # some event details are lists like updates
            v = v[0] if isinstance(v, list) and len(v) > 0 else v

            k, v = await self._resolve_attribute(k, v)

            if k:
                attributes[k] = v
        return attributes

    async def event_to_smart_request(
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

        category_path = await self.resolve_category_path_for_event(event=event)

        if not category_path:
            logger.error(f"No category found for event_type: {event.event_type}")
            raise ReferenceDataError(
                f"No category found for event_type: {event.event_type}"
            )

        attributes = await self._resolve_attributes_for_event(event=event)

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
                # ToDo: make this async or remove it?
                # downloaded_file = self.cloud_storage.download(download_file_name)
                # downloaded_file_base64 = base64.b64encode(
                #     downloaded_file.getvalue()
                # ).decode()
                file = File(
                    filename=event_file.get("filename"),
                    data=f"gundi:storage:{download_file_name}",  # downloaded_file_base64
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
        if observation_uuid == "None":
            observation_uuid = str(uuid.uuid4())  # Provide a UUID as str

        logger.info(
            f"Building SmartObservation with UUID: {observation_uuid}, category: {category_path}, attributes: {attributes}"
        )
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

    async def event_to_observation(
        self, *, event: Union[schemas.EREvent, schemas.GeoEvent] = None
    ) -> SMARTRequest:
        """
        Handle both geo events and er events for version > 7.5 of smart connect

        Creates an observation update request. New observations are created through event_to_incident
        """

        observation_update_request = await self.event_to_smart_request(
            event=event, smart_feature_type="waypoint/observation"
        )

        return observation_update_request

    async def event_to_incident(
        self,
        *,
        event: Union[schemas.EREvent, schemas.GeoEvent] = None,
        smart_feature_type=None,
    ) -> SMARTRequest:
        """
        Handle both geo events and er events for version > 7.5 of smart connect
        """

        incident_request = await self.event_to_smart_request(
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

    async def transform(self, item) -> dict:
        waypoint_requests = []
        if self._version and version.parse(self._version) >= version.parse("7.5"):
            smart_response = await self.smartconnect_client.get_incident(
                incident_uuid=item.id
            )
            if not smart_response:
                incident = await self.event_to_incident(
                    event=item, smart_feature_type="waypoint/new"
                )
                waypoint_requests.append(incident)
            else:
                # Update Incident
                incident = await self.event_to_incident(
                    event=item, smart_feature_type="waypoint"
                )
                waypoint_requests.append(incident)
                # Update Observation
                observation = await self.event_to_observation(event=item)
                waypoint_requests.append(observation)
        else:
            incident = await self.geoevent_to_incident(geoevent=item)
            waypoint_requests.append(incident)
        smart_request = SMARTCompositeRequest(
            waypoint_requests=waypoint_requests, ca_uuid=self.ca_uuid
        )

        return json.loads(smart_request.json()) if smart_request else None

    # TODO: Depreciated use event_to_incident, remove when all integrations on smart connect version > 7.5.x
    async def geoevent_to_incident(
        self, *, geoevent: schemas.GeoEvent = None
    ) -> SMARTRequest:
        # Sanitize coordinates
        coordinates = (
            [geoevent.location.x, geoevent.location.y] if geoevent.location else [0, 0]
        )

        # Apply Transformation Rules

        category_path = await self.resolve_category_path_for_event(event=geoevent)

        if not category_path:
            logger.error(f"No category found for event_type: {geoevent.event_type}")
            raise ReferenceDataError(
                f"No category found for event_type: {geoevent.event_type}"
            )

        attributes = await self._resolve_attributes_for_event(event=geoevent)

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

    async def transform(self, item) -> dict:
        smart_request = await self.er_patrol_to_smart_patrol(patrol=item)

        return json.loads(smart_request.json()) if smart_request else None

    async def get_incident_requests_from_er_patrol_leg(
        self, *, patrol_id, patrol_leg: ERPatrolSegment
    ):
        incident_requests = []
        incident_request: SMARTRequest
        for event in patrol_leg.event_details:
            incident_request = await self.event_to_patrol_waypoint(
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

    async def event_to_patrol_waypoint(
        self, *, patrol_id, patrol_leg_id, event, smart_feature_type
    ):
        incident_request = await self.event_to_incident(
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

    async def er_patrol_to_smart_patrol(self, patrol: ERPatrol):
        existing_smart_patrol = await self.smartconnect_client.get_patrol(
            patrol_id=patrol.id
        )

        if existing_smart_patrol:
            patrol_leg = patrol.patrol_segments[0]

            # Update patrol/patrol_leg properties if changed
            patrol_requests = []
            patrol_request = self.er_patrol_to_smart_patrol_request(
                patrol=patrol, patrol_leg=patrol_leg, smart_feature_type="patrol"
            )
            patrol_requests.append(patrol_request)

            # Get waypoints for patrol
            patrol_waypoints = await self.smartconnect_client.get_patrol_waypoints(
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
                        smart_response = await self.smartconnect_client.get_incident(
                            incident_uuid=event.id
                        )
                        if smart_response:
                            logger.info(
                                "skipping event because it already exists in destination outside of patrol"
                            )
                            # version ^7.5.3 will allow us to create waypoint on patrol with same uuid as existing ind inc
                            continue
                    incident_request = await self.event_to_patrol_waypoint(
                        patrol_id=patrol.id,
                        patrol_leg_id=patrol_leg.id,
                        event=event,
                        smart_feature_type="waypoint/new",
                    )
                    incident_requests.append(incident_request)
                else:
                    incident_request = await self.event_to_patrol_waypoint(
                        patrol_id=patrol.id,
                        patrol_leg_id=patrol_leg.id,
                        event=event,
                        smart_feature_type="waypoint",
                    )
                    incident_requests.append(incident_request)

                    # Update Observation
                    observation = await self.event_to_observation(event=event)
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

            incident_requests = await self.get_incident_requests_from_er_patrol_leg(
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


########################################################################################################################
# GUNDI V2
########################################################################################################################
def find_config_for_action(configurations, action_value):
    return next(
        (config for config in configurations if config.action.value == action_value),
        None,
    )


class SMARTTransformerV2(Transformer, ABC):
    def __init__(self, *, config=None, **kwargs):
        super().__init__(config=config, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        # Looks for the configurations needed by the transformer
        # Look for the configuration of the authentication action
        configurations = config.configurations
        auth_config = find_config_for_action(
            configurations=configurations, action_value="auth"
        )
        if not auth_config:
            raise ValueError(
                f"Authentication settings for integration {str(config.id)} are missing. Please fix the integration setup in the portal."
            )
        self.auth_config = SMARTAuthActionConfig.parse_obj(auth_config.data)
        push_events_config = find_config_for_action(
            configurations=configurations, action_value="push_events"
        )
        if not push_events_config:
            raise ValueError(
                f"Push Events settings for integration {str(config.id)} are missing. Please fix the integration setup in the portal."
            )
        self.push_config = SMARTPushEventActionConfig.parse_obj(push_events_config.data)
        self._ca_datamodel = None
        self._ca_config_datamodel = None
        self._configurable_models = None
        self.cm_uuids = self.push_config.configurable_models_enabled or []
        self.ca = None
        # Handle 0:N SMART CA Mapping. Look for CA in kwargs, then look in config
        self.ca_uuid = kwargs.get(
            "ca_uuid",
            str(self.push_config.ca_uuid) if self.push_config.ca_uuid else None,
        )  # priority to passed in ca_uuid
        # Look for CA in ca_uuids if only 1 CA is mapped
        if (
            not self.ca_uuid
            and self.push_config.ca_uuids
            and len(self.push_config.ca_uuids) == 1
        ):
            self.ca_uuid = str(self.push_config.ca_uuids[0])
        if not self.ca_uuid:
            raise IndeterminableCAException(
                "Unable to determine CA uuid for observation. Please set 'ca_uuids' in the portal."
            )

        self._version = self.auth_config.version or "7.5"
        logger.info(f"Using SMART Integration version {self._version}")

        self.smartconnect_client = AsyncSmartClient(
            api=f"{self.config.base_url}/server",
            username=self.auth_config.login,
            password=self.auth_config.password,
            version=self._version,
        )

        # Let the timezone fall-back to configuration.
        try:
            self._default_timezone = pytz.timezone(self.push_config.timezone)
        except pytz.exceptions.UnknownTimeZoneError as e:
            self.logger.warning(
                f"Configured timezone is {self.push_config.timezone}, but it is not a known timezone. Defaulting to UTC unless timezone can be inferred from the Conservation Area's meta-data."
            )
            self._default_timezone = pytz.utc

        self.ca_timezone = self._default_timezone
        self._transformation_rules = self.push_config.transformation_rules

    async def get_ca(self, config):
        if not self.ca:
            try:
                self.ca = await self.smartconnect_client.get_conservation_area(
                    ca_uuid=self.ca_uuid
                )
            except Exception as ex:
                self.logger.warning(
                    f"Failed to get CA Metadata for endpoint: {self.config.base_url}, username: {config.login}, CA-UUID: {self.ca_uuid}. Exception: {ex}."
                )
                self.ca = None
        return self.ca

    async def get_configurable_models(self):
        if not self._configurable_models:
            try:
                self._configurable_models = list(
                    [
                        await self.smartconnect_client.get_configurable_data_model(
                            cm_uuid=cm_uuid
                        )
                        for cm_uuid in self.cm_uuids
                    ]
                )
            except Exception as e:
                self._ca_config_datamodel = []
                logger.exception(
                    f"Error getting config data model for SMART CA: {self.ca_uuid}",
                    extra={ExtraKeys.Error: e},
                )
        return self._configurable_models

    async def get_ca_datamodel(self):
        if not self._ca_datamodel:
            try:
                self._ca_datamodel = await self.smartconnect_client.get_data_model(
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
        return self._ca_datamodel

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

    async def resolve_category_path_for_event(
        self, *, event: schemas.v2.Event = None
    ) -> str:
        """
        Favor finding a match in the Config CA Datamodel, then CA Datamodel.
        """

        search_for = event.event_type.replace("_", ".")
        configurable_models = await self.get_configurable_models()
        for cm in configurable_models:
            # favor config datamodel match if present
            # convert ER event type to CA path syntax
            if matched_category := cm.get_category(path=search_for):
                return matched_category["hkeyPath"]

        # direct data model match
        ca_datamodel = await self.get_ca_datamodel()
        if matched_category := ca_datamodel.get_category(path=event.event_type):
            return matched_category["path"]

        # convert event type to CA path syntax
        if matched_category := ca_datamodel.get_category(
            path=str.replace(event.event_type, "_", ".")
        ):
            return matched_category["path"]

        # Last option is a match in translation rules.
        for t in self._transformation_rules.category_map:
            if t.event_type == event.event_type:
                return t.category_path

    async def _resolve_attribute(
        self, key, value
    ) -> Tuple[Union[str, None], Union[str, None]]:
        attr = None

        # Favor a match in configurable model.
        configurable_models = await self.get_configurable_models()
        for cm in configurable_models:
            attr = cm.get_attribute(key=key)
            if attr:
                break
        else:
            ca_datamodel = await self.get_ca_datamodel()
            attr = ca_datamodel.get_attribute(key=key)

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

    async def _resolve_attributes_for_event(
        self, *, event: [schemas.GeoEvent, schemas.EREvent] = None
    ) -> dict:
        attributes = {}
        for k, v in event.event_details.items():
            # some event details are lists like updates
            v = v[0] if isinstance(v, list) and len(v) > 0 else v

            k, v = await self._resolve_attribute(k, v)

            if k:
                attributes[k] = v
        return attributes

    async def event_to_smart_request(
        self,
        *,
        event: schemas.v2.Event = None,
        smart_feature_type=None,
    ) -> SMARTRequest:
        """
        Common code used to construct a SMART request
        """

        # Sanitize coordinates
        coordinates = [0, 0]
        location_timezone = self._default_timezone
        if event.location:
            coordinates = [event.location.lon, event.location.lat]
            location_timezone = self.guess_location_timezone(
                longitude=event.location.lon, latitude=event.location.lat
            )

        # Apply SMART Transformation Rules
        category_path = await self.resolve_category_path_for_event(event=event)
        if not category_path:
            logger.error(f"No category found for event_type: {event.event_type}")
            raise ReferenceDataError(
                f"No category found for event_type: {event.event_type}"
            )

        attributes = await self._resolve_attributes_for_event(event=event)

        present_localtime = datetime.now(tz=pytz.utc).astimezone(location_timezone)
        event_localtime = event.recorded_at.astimezone(location_timezone)

        comment = (
            f"Report: {event.title if event.title else event.event_type}"
            + f"\nImported: {present_localtime.isoformat()}"
        )
        # ToDo: Once we support a provider-defined id, we should use that here
        event_id = str(event.gundi_id)
        incident_id = f"gundi_ev_{event_id}"
        incident_uuid = event_id
        smart_data_type = "incident"
        observation_uuid = event_id
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

    async def event_to_observation(
        self, *, event: schemas.v2.Event = None
    ) -> SMARTRequest:
        """
        Handle events v2 for version > 7.5 of smart connect

        Creates an observation update request. New observations are created through event_to_incident
        """

        observation_update_request = await self.event_to_smart_request(
            event=event, smart_feature_type="waypoint/observation"
        )

        return observation_update_request

    async def event_to_incident(
        self,
        *,
        event: schemas.v2.Event = None,
        smart_feature_type=None,
    ) -> SMARTRequest:
        """
        Handle events v2 for version > 7.5 of smart connect
        """

        incident_request = await self.event_to_smart_request(
            event=event, smart_feature_type=smart_feature_type
        )

        return incident_request


class SmartEventTransformerV2(SMARTTransformerV2):
    """
    Transform a single Event into an Independent Incident.
    """

    def __init__(self, *, config: SMARTPushEventActionConfig, **kwargs):
        super().__init__(config=config, **kwargs)

    async def transform(
        self, message: schemas.v2.Event, rules: list = None, **kwargs
    ) -> dict:
        message, message_ca_uuid = get_ca_uuid_for_event(event=message)
        if message_ca_uuid:
            self.ca_uuid = str(message_ca_uuid)
        waypoint_requests = []
        if self._version and version.parse(self._version) >= version.parse("7.5"):
            # ToDo: Revisit the create/update logic once we support updates in Gundi v2
            smart_response = await self.smartconnect_client.get_incident(
                incident_uuid=message.gundi_id
            )
            if not smart_response:  # Create Incident
                incident = await self.event_to_incident(
                    event=message, smart_feature_type="waypoint/new"
                )
                waypoint_requests.append(incident)
            else:
                # Update Incident
                incident = await self.event_to_incident(
                    event=message, smart_feature_type="waypoint"
                )
                waypoint_requests.append(incident)
                # Update Observation
                observation = await self.event_to_observation(event=message)
                waypoint_requests.append(observation)
        else:
            raise ValueError("Smart version < 7.5 is not supported")
        # ToDo. We could handle these as separate requests in Gundi v2, as we do with events and attachments.
        smart_request = SMARTCompositeRequest(
            waypoint_requests=waypoint_requests, ca_uuid=self.ca_uuid
        )
        transformed_message = json.loads(smart_request.json())
        # Apply extra transformation rules as needed (a.k.a Field mappings)
        if rules:
            for rule in rules:
                rule.apply(message=transformed_message)
        return transformed_message


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


async def transform_observation(
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
        transformer = MBPositionTransformer()
        additional_info["integration_type"] = config.inbound_type_slug
        additional_info["gundi_version"] = GUNDI_V1
    elif (
        stream_type == schemas.StreamPrefixEnum.position
        and config.type_slug == schemas.DestinationTypes.EarthRanger.value
    ):
        transformer = ERPositionTransformer()
    elif (
        stream_type == schemas.StreamPrefixEnum.geoevent
        and config.type_slug == schemas.DestinationTypes.EarthRanger.value
    ):
        transformer = ERGeoEventTransformer()
    elif (
        stream_type == schemas.StreamPrefixEnum.camera_trap
        and config.type_slug == schemas.DestinationTypes.EarthRanger.value
    ):
        transformer = ERCameraTrapTransformer()
    elif (
        stream_type == schemas.StreamPrefixEnum.camera_trap
        and config.type_slug == schemas.DestinationTypes.WPSWatch.value
    ):
        transformer = WPSWatchCameraTrapTransformer()
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
        return await transformer.transform(observation, **additional_info)
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


def get_ca_uuid_for_event(
    *args, event: Union[schemas.EREvent, schemas.GeoEvent, schemas.v2.Event]
):

    assert not args, "get_ca_uuid_for_event takes only keyword args"

    uuid_pattern = re.compile(
        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I
    )

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


def get_source_id(observation, gundi_version="v1"):
    return observation.source_id if gundi_version == "v2" else observation.device_id


def get_data_provider_id(observation, gundi_version="v1"):
    return (
        observation.data_provider_id
        if gundi_version == "v2"
        else observation.integration_id
    )


async def transform_observation_to_destination_schema(
    *,
    observation,
    destination,
    provider=None,
    gundi_version="v1",
    route_configuration=None,
) -> dict:
    if gundi_version == "v2":
        return await transform_observation_v2(
            observation=observation,
            destination=destination,
            provider=provider,
            route_configuration=route_configuration,
        )
    else:  # Default to v1
        return await transform_observation(
            observation=observation,
            stream_type=observation.observation_type,
            config=destination,
        )


def build_transformed_message_attributes(
    observation, destination, gundi_version, provider_key=None
):
    if gundi_version == "v2":
        return {
            "gundi_version": gundi_version,
            "provider_key": provider_key,
            "gundi_id": str(observation.gundi_id),
            "related_to": str(observation.related_to)
            if observation.related_to
            else None,
            "stream_type": str(observation.observation_type),
            "source_id": str(observation.source_id),
            "external_source_id": str(observation.external_source_id),
            "destination_id": str(destination.id),
            "data_provider_id": str(get_data_provider_id(observation, gundi_version)),
            "annotations": json.dumps(observation.annotations),
        }
    else:  # default to v1
        return {
            "observation_type": str(observation.observation_type),
            "device_id": str(get_source_id(observation, gundi_version)),
            "outbound_config_id": str(destination.id),
            "integration_id": str(get_data_provider_id(observation, gundi_version)),
        }


def convert_observation_to_cdip_schema(observation, gundi_version="v1"):
    if gundi_version == "v2":
        schema = schemas.v2.models_by_stream_type[observation.get("observation_type")]
    else:  # Default to v1
        schema = schemas.models_by_stream_type[observation.get("observation_type")]
    return schema.parse_obj(observation)


def create_message(attributes, observation):
    message = {"attributes": attributes, "data": observation}
    return message


def build_gcp_pubsub_message(*, payload):
    binary_data = json.dumps(payload, default=str).encode("utf-8")
    return binary_data


def extract_fields_from_message(message):
    if message:
        data = base64.b64decode(message.get("data", "").encode("utf-8"))
        observation = json.loads(data)
        attributes = message.get("attributes")
        if not observation:
            logger.warning(f"No observation was obtained from {message}")
        if not attributes:
            logger.debug(f"No attributes were obtained from {message}")
    else:
        logger.warning(f"message contained no payload", extra={"message": message})
        return None, None
    return observation, attributes


def get_key_for_transformed_observation(current_key: bytes, destination_id: UUID):
    # caller must provide key and destination_id must be present in order to create for transformed observation
    if current_key is None or destination_id is None:
        return current_key
    else:
        new_key = f"{current_key.decode('utf-8')}.{str(destination_id)}"
        return new_key.encode("utf-8")


async def transform_observation_v2(
    observation, destination, provider, route_configuration=None
):
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
            },
        )
        raise TransformerNotFound(
            f"No transformer found for {stream_type} dest: {destination_type}"
        )

    # Check for extra configurations to apply
    rules = []
    if route_configuration and (
        field_mappings := route_configuration.data.get("field_mappings")
    ):
        configuration = (
            field_mappings.get(
                # First look for configurations for this data provider
                str(observation.data_provider_id),
                {},
            )
            .get(  # Then look for configurations for this stream type
                str(stream_type), {}
            )
            .get(
                # Then look for configurations for this destination
                str(destination.id),
                {},
            )
        )
        if configuration:
            destination_field = configuration.get("destination_field")
            if not destination_field:
                raise ReferenceDataError(
                    f"No destination_field found in route configuration {route_configuration}"
                )
            field_mapping_rule = FieldMappingRule(
                target=destination_field,
                default=configuration.get("default"),
                source=configuration.get("provider_field"),
                map=configuration.get("map"),
            )
            rules.append(field_mapping_rule)

    # Apply the transformer
    transformer = Transformer(config=destination)
    return await transformer.transform(
        message=observation, rules=rules, provider=provider, gundi_version=GUNDI_V2
    )


# Map to get the right transformer for the observation type and destination
transformers_map = {
    schemas.v2.StreamPrefixEnum.event.value: {
        schemas.DestinationTypes.EarthRanger.value: EREventTransformer,
        schemas.DestinationTypes.SmartConnect.value: SmartEventTransformerV2,
    },
    schemas.v2.StreamPrefixEnum.attachment.value: {
        schemas.DestinationTypes.EarthRanger.value: ERAttachmentTransformer
    },
    schemas.v2.StreamPrefixEnum.observation.value: {
        schemas.DestinationTypes.Movebank.value: MBObservationTransformer,
        schemas.DestinationTypes.EarthRanger.value: ERObservationTransformer,
    },
    # ToDo. Support patrols in SMART v2?
    # schemas.StreamPrefixEnum.earthranger_patrol : {
    #     schemas.DestinationTypes.SmartConnect.value: SmartERPatrolTransformerV2
    # }
}
