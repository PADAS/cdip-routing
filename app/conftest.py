import base64
import datetime
import aiohttp
import httpx
import pytest
import uuid
import asyncio
import gundi_core.schemas.v2 as schemas_v2
import gundi_core.schemas.v1 as schemas_v1
from pydantic.types import UUID
from redis import exceptions as redis_exceptions
from smartconnect import SMARTClientException

from app.core.deduplication import EventProcessingStatus


def async_return(result):
    f = asyncio.Future()
    f.set_result(result)
    return f


@pytest.fixture
def mock_cache(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.set.return_value = async_return(None)
    mock_cache.get.return_value = async_return(None)
    mock_cache.setex.return_value = async_return(None)
    mock_cache.incr.return_value = mock_cache
    mock_cache.decr.return_value = async_return(None)
    mock_cache.expire.return_value = mock_cache
    mock_cache.execute.return_value = async_return((1, True))
    mock_cache.__aenter__.return_value = mock_cache
    mock_cache.__aexit__.return_value = None
    mock_cache.pipeline.return_value = mock_cache
    return mock_cache


@pytest.fixture
def mock_deduplication_cache_empty(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.get.return_value = async_return(None)
    mock_cache.setex.return_value = async_return(None)
    mock_cache.__aenter__.return_value = mock_cache
    mock_cache.__aexit__.return_value = None
    return mock_cache


@pytest.fixture
def mock_deduplication_cache_one_miss(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.get.side_effect = (
        async_return(None),
        async_return("1"),
    )
    mock_cache.setex.return_value = async_return(None)
    mock_cache.__aenter__.return_value = mock_cache
    mock_cache.__aexit__.return_value = None
    return mock_cache


@pytest.fixture
def mock_cache_with_cached_connection(mocker, connection_v2):
    mock_cache = mocker.MagicMock()
    cached_data = connection_v2.json()
    mock_cache.get.return_value = async_return(cached_data)
    return mock_cache


@pytest.fixture
def mock_cache_with_connection_error(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.get.side_effect = redis_exceptions.ConnectionError(
        "Error while reading from 172.22.161.3:6379 : (104, 'Connection reset by peer')"
    )
    return mock_cache


@pytest.fixture
def mock_gundi_client(
    mocker,
    inbound_integration_config,
    outbound_integration_config,
    outbound_integration_config_list,
    device,
):
    mock_client = mocker.MagicMock()
    mock_client.get_inbound_integration.return_value = async_return(
        inbound_integration_config
    )
    mock_client.get_outbound_integration.return_value = async_return(
        outbound_integration_config
    )
    mock_client.get_outbound_integration_list.return_value = async_return(
        outbound_integration_config_list
    )
    mock_client.ensure_device.return_value = async_return(device)
    return mock_client


@pytest.fixture
def smart_ca_data_model():
    dm = DataModel(use_language_code="en")
    with open("app/tests/test_datamodel.xml", "r") as f:
        text = f.read()
        dm.load(text)
    return dm


@pytest.fixture
def smart_cm_data_models():
    return []


@pytest.fixture
def mock_smart_async_client(mocker, smart_ca_data_model, smart_cm_data_models):
    mock_client = mocker.MagicMock()
    mock_client.get_incident.return_value = async_return(None)
    mock_client.get_patrol.return_value = async_return(None)
    mock_client.get_configurable_models.return_value = async_return(
        smart_cm_data_models
    )
    mock_client.get_data_model.return_value = async_return(smart_ca_data_model)
    mock_client.post_smart_request.return_value = async_return({"status": "success"})
    return mock_client


@pytest.fixture
def mock_smart_async_client_with_server_error(
    mocker, smart_ca_data_model, smart_cm_data_models
):
    mock_client = mocker.MagicMock()
    mock_client.get_incident.side_effect = SMARTClientException
    mock_client.get_patrol.return_value = async_return(None)
    mock_client.get_configurable_models.return_value = async_return(
        smart_cm_data_models
    )
    mock_client.get_data_model.return_value = async_return(smart_ca_data_model)
    mock_client.post_smart_request.return_value = async_return({"status": "success"})
    return mock_client


@pytest.fixture
def mock_smart_async_client_class(mocker, mock_smart_async_client):
    mock_smart_async_client_class = mocker.MagicMock()
    mock_smart_async_client_class.return_value = mock_smart_async_client
    return mock_smart_async_client_class


@pytest.fixture
def mock_smart_async_client_class_with_server_error(
    mocker, mock_smart_async_client_with_server_error
):
    mock_smart_async_client_class = mocker.MagicMock()
    mock_smart_async_client_class.return_value = (
        mock_smart_async_client_with_server_error
    )
    return mock_smart_async_client_class


def _set_side_effect_error_on_gundi_client_once(mock_client, error):
    # Side effects to raise an exception only the first time each method is called
    mock_client.get_inbound_integration.side_effect = [
        error,
        async_return(inbound_integration_config),
    ]
    mock_client.get_outbound_integration.side_effect = [
        error,
        async_return(outbound_integration_config),
    ]
    mock_client.get_outbound_integration_list.side_effect = [
        error,
        async_return(outbound_integration_config_list),
    ]
    mock_client.ensure_device.side_effect = [error, async_return(device)]


@pytest.fixture
def mock_gundi_client_with_client_connector_error_once(
    mocker,
    inbound_integration_config,
    outbound_integration_config,
    outbound_integration_config_list,
    device,
):
    mock_client = mocker.MagicMock()
    # Simulate a connection error
    client_connector_error = httpx.ConnectError(
        message="Connection error",
        request=httpx.Request("GET", "https://cdip-portal.pamdas.org"),
    )
    _set_side_effect_error_on_gundi_client_once(
        mock_client=mock_client, error=client_connector_error
    )
    return mock_client


@pytest.fixture
def mock_gundi_client_with_server_disconnected_error_once(
    mocker,
    inbound_integration_config,
    outbound_integration_config,
    outbound_integration_config_list,
    device,
):
    mock_client = mocker.MagicMock()
    # Simulate a server disconnected error
    server_disconnected_error = httpx.NetworkError(
        message="Server disconnected",
        request=httpx.Request("GET", "https://cdip-portal.pamdas.org"),
    )
    _set_side_effect_error_on_gundi_client_once(
        mock_client=mock_client, error=server_disconnected_error
    )
    return mock_client


@pytest.fixture
def mock_gundi_client_with_server_timeout_error_once(
    mocker,
    inbound_integration_config,
    outbound_integration_config,
    outbound_integration_config_list,
    device,
):
    mock_client = mocker.MagicMock()
    # Simulate a timeout error
    server_timeout_error = httpx.TimeoutException(
        message="Server timeout",
        request=httpx.Request("GET", "https://cdip-portal.pamdas.org"),
    )
    _set_side_effect_error_on_gundi_client_once(
        mock_client=mock_client, error=server_timeout_error
    )
    return mock_client


@pytest.fixture
def mock_pubsub(mocker, gcp_pubsub_publish_response):
    mock_client = mocker.MagicMock()
    mock_publisher = mocker.MagicMock()
    mock_publisher.publish.return_value = async_return(gcp_pubsub_publish_response)
    mock_client.PublisherClient.return_value = mock_publisher
    return mock_client


@pytest.fixture
def mock_pubsub_client_with_timeout_once(mocker, gcp_pubsub_publish_response):
    mock_client = mocker.MagicMock()
    mock_publisher = mocker.MagicMock()
    # Side effects to raise an exception only the first time it's called
    mock_publisher.publish.side_effect = [
        asyncio.TimeoutError(),
        async_return(gcp_pubsub_publish_response),
    ]
    mock_client.PublisherClient.return_value = mock_publisher
    return mock_client


@pytest.fixture
def mock_pubsub_client_with_client_error_once(mocker, gcp_pubsub_publish_response):
    mock_client = mocker.MagicMock()
    mock_publisher = mocker.MagicMock()
    # Side effects to raise an exception only the first time it's called
    mock_publisher.publish.side_effect = [
        aiohttp.ClientError(),
        async_return(gcp_pubsub_publish_response),
    ]
    mock_client.PublisherClient.return_value = mock_publisher
    return mock_client


@pytest.fixture
def mock_send_observation_to_dead_letter_topic(mocker):
    mock = mocker.MagicMock()
    mock.return_value = async_return(None)
    return mock


@pytest.fixture
def mock_send_observation_to_pubsub_topic(mocker):
    mock = mocker.MagicMock()
    mock.return_value = async_return(None)
    return mock


@pytest.fixture
def gcp_pubsub_publish_response():
    return {"messageIds": ["7061707768812258"]}


@pytest.fixture
def inbound_integration_config():
    return {
        "state": {},
        "id": "12345b4f-88cd-49c4-a723-0ddff1f580c4",
        "type": "1234e5bd-a473-4c02-9227-27b6134615a4",
        "owner": "1234191a-bcf3-471b-9e7d-6ba8bc71be9e",
        "endpoint": "https://logins.testbidtrack.co.za/restintegration/",
        "login": "test",
        "password": "test",
        "token": "",
        "type_slug": "bidtrack",
        "provider": "bidtrack",
        "default_devicegroup": "1234cfdc-1aae-44b0-8e0a-22c72355ea85",
        "enabled": True,
        "name": "BidTrack - Manyoni",
    }


@pytest.fixture
def outbound_integration_config():
    return {
        "id": "2222dc7e-73e2-4af3-93f5-a1cb322e5add",
        "type": "f61b0c60-c863-44d7-adc6-d9b49b389e69",
        "owner": "1111191a-bcf3-471b-9e7d-6ba8bc71be9e",
        "name": "[Internal] AI2 Test -  Bidtrack to  ER",
        "endpoint": "https://cdip-er.pamdas.org/api/v1.0",
        "state": {},
        "login": "",
        "password": "",
        "token": "1111d87681cd1d01ad07c2d0f57d15d6079ae7d7",
        "type_slug": "earth_ranger",
        "inbound_type_slug": "bidtrack",
        "additional": {
            "broker": "gcp_pubsub",
            "topic": "er-dispatcher-topic",
        },
    }


@pytest.fixture
def outbound_integration_config_list():
    return [
        {
            "id": "2222dc7e-73e2-4af3-93f5-a1cb322e5add",
            "type": "f61b0c60-c863-44d7-adc6-d9b49b389e69",
            "owner": "1111191a-bcf3-471b-9e7d-6ba8bc71be9e",
            "name": "[Internal] AI2 Test -  Bidtrack to  ER",
            "endpoint": "https://cdip-er.pamdas.org/api/v1.0",
            "state": {},
            "login": "",
            "password": "",
            "token": "1111d87681cd1d01ad07c2d0f57d15d6079ae7d7",
            "type_slug": "earth_ranger",
            "inbound_type_slug": "bidtrack",
            "additional": {
                "broker": "gcp_pubsub",
                "topic": "er-dispatcher-topic",
            },
        }
    ]


@pytest.fixture
def device():
    return {
        "id": "564daff9-8cac-4004-b1a4-e169cf3b4bdb",
        "external_id": "018910999",
        "name": "",
        "subject_type": None,
        "inbound_configuration": {
            "state": {},
            "id": "36485b4f-88cd-49c4-a723-0ddff1f580c4",
            "type": "b069e5bd-a473-4c02-9227-27b6134615a4",
            "owner": "088a191a-bcf3-471b-9e7d-6ba8bc71be9e",
            "endpoint": "https://logins.bidtrack.co.za/restintegration/",
            "login": "fakeusername",
            "password": "something fancy",
            "token": "",
            "type_slug": "bidtrack",
            "provider": "bidtrack",
            "default_devicegroup": "0da5cfdc-1aae-44b0-8e0a-22c72355ea85",
            "enabled": True,
            "name": "BidTrack - Manyoni",
        },
        "additional": {},
    }


@pytest.fixture
def raw_observation_position():
    return {
        "id": None,
        "owner": "na",
        "integration_id": "36485b4f-88cd-49c4-a723-0ddff1f580c4",
        "device_id": "018910980",
        "name": "Logistics Truck test",
        "type": "tracking-device",
        "subject_type": None,
        "recorded_at": "2023-03-03 09:34:00+02:00",
        "location": {
            "x": 35.43935,
            "y": -1.59083,
            "z": 0.0,
            "hdop": None,
            "vdop": None,
        },
        "additional": {"voltage": "7.4", "fuel_level": 71, "speed": "41 kph"},
        "voltage": None,
        "temperature": None,
        "radio_status": None,
        "observation_type": "ps",
    }


@pytest.fixture
def raw_observation_position_attributes():
    return {"observation_type": "ps"}


@pytest.fixture
def raw_observation_geoevent():
    return {
        "id": None,
        "owner": "na",
        "integration_id": "36485b4f-88cd-49c4-a723-0ddff1f580c4",
        "device_id": "003",
        "recorded_at": "2023-07-05 09:16:02-03:00",
        "location": {
            "x": -55.784992,
            "y": 20.806785,
            "z": 0.0,
            "hdop": None,
            "vdop": None,
        },
        "additional": None,
        "title": "Rainfall",
        "event_type": "rainfall_rep",
        "event_details": {"amount_mm": 6, "height_m": 3},
        "geometry": None,
        "observation_type": "ge",
    }


@pytest.fixture
def raw_observation_geoevent_for_smart():
    return {
        "id": None,
        "owner": "na",
        "integration_id": "36485b4f-88cd-49c4-a723-0ddff1f580c4",
        "device_id": "survey_one",
        "recorded_at": "2024-08-16 15:01:00-07:00",
        "location": {
            "x": -71.04190,
            "y": 34.490285,
            "z": 0.0,
            "hdop": None,
            "vdop": None,
        },
        "additional": None,
        "title": "Anthropogenic Disturbance",
        "event_type": "humanactivity_humansign",
        "event_details": {"typeofhumansign": "footprints", "ageofsign": "10 days"},
        "geometry": None,
        "observation_type": "ge",
    }


@pytest.fixture
def raw_observation_geoevent_with_valid_uuid():
    return {
        "id": "088a191a-bcf3-471b-9e7d-6ba8bc71be9e",
        "owner": "na",
        "integration_id": "36485b4f-88cd-49c4-a723-0ddff1f580c4",
        "device_id": "003",
        "recorded_at": "2023-07-05 09:16:02-03:00",
        "location": {
            "x": -55.784992,
            "y": 20.806785,
            "z": 0.0,
            "hdop": None,
            "vdop": None,
        },
        "additional": None,
        "title": "Rainfall",
        "event_type": "f61b0c60-c863-44d7-adc6-d9b49b389e69_rainfall_rep",
        "event_details": {"amount_mm": 6, "height_m": 3},
        "geometry": None,
        "observation_type": "ge",
    }


@pytest.fixture
def raw_observation_geoevent_attributes():
    return {"observation_type": "ge"}


@pytest.fixture
def raw_observation_cameratrap():
    return {
        "id": None,
        "owner": "integration:17e7a1e0-168b-4f68-9392-35ec29222f13",
        "integration_id": "17e7a1e0-168b-4f68-9392-35ec29222f13",
        "device_id": "test_cam",
        "name": None,
        "type": "camerea-trap",
        "recorded_at": "2023-03-21 09:29:00-03:00",
        "location": {"x": -122.5, "y": 48.65, "z": 0.0, "hdop": None, "vdop": None},
        "additional": None,
        "image_uri": "2023-07-04-1851_leopard.jpg",
        "camera_name": "test_cam",
        "camera_description": "Test camera",
        "camera_version": None,
        "observation_type": "ct",
    }


@pytest.fixture
def raw_observation_cameratrap_attributes():
    return {"observation_type": "ct"}


@pytest.fixture
def raw_observation_er_event():
    return {
        "id": "d3109853-747e-4c39-b821-6c897a992744",
        "owner": "na",
        "integration_id": "1055c18c-ae2f-4609-aa2f-dde86419c701",
        "er_uuid": "d3109853-747e-4c39-b821-6c897a992744",
        "location": {"latitude": -41.145108, "longitude": -71.262104},
        "time": "2024-02-08 06:08:00-06:00",
        "created_at": "2024-02-08 06:08:51.424788-06:00",
        "updated_at": "2024-02-08 06:08:51.424124-06:00",
        "serial_number": 49534,
        "event_type": "169361d0-62b8-411d-a8e6-019823805016_animals_sign",
        "priority": 0,
        "priority_label": "Gray",
        "title": None,
        "state": "active",
        "url": "https://gundi-er.pamdas.org/api/v1.0/activity/event/d3109853-747e-4c39-b821-6c897a992744",
        "event_details": {"comments": "Mm test", "updates": []},
        "patrols": [],
        "files": [],
        "uri": "",
        "device_id": "d3109853-747e-4c39-b821-6c897a992744",
        "observation_type": "er_event",
    }


@pytest.fixture
def raw_observation_er_event_attributes():
    return {}


@pytest.fixture
def raw_observation_er_patrol():
    return {
        "id": "2ce0001b-f2a3-4a4d-a108-a051019fc27d",
        "owner": "na",
        "integration_id": "1055c18c-ae2f-4609-aa2f-dde86419c701",
        "files": [],
        "serial_number": 14,
        "title": "Routine Patrol",
        "device_id": "2ce0001b-f2a3-4a4d-a108-a051019fc27d",
        "notes": [],
        "objective": "Routine Patrol",
        "patrol_segments": [
            {
                "end_location": None,
                "events": [
                    {
                        "id": "c9ca269e-c873-423c-ab12-b72791b303b0",
                        "event_type": "169361d0-62b8-411d-a8e6-019823805016_animals_sign",
                        "updated_at": "2024-02-07 14:42:36.638888-06:00",
                        "geojson": None,
                    },
                    {
                        "id": "b8e589fb-baf2-476a-96a6-0c87cf2ed847",
                        "event_type": "169361d0-62b8-411d-a8e6-019823805016_animals_sign",
                        "updated_at": "2024-02-08 10:04:25.052375-06:00",
                        "geojson": {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [-122.3589888, 47.6839936],
                            },
                        },
                    },
                ],
                "event_details": [
                    {
                        "id": "c9ca269e-c873-423c-ab12-b72791b303b0",
                        "owner": "na",
                        "integration_id": None,
                        "er_uuid": "c9ca269e-c873-423c-ab12-b72791b303b0",
                        "location": None,
                        "time": "2024-02-07 14:42:09.709000-06:00",
                        "created_at": "2024-02-07 14:42:36.638857-06:00",
                        "updated_at": "2024-02-07 14:42:36.637713-06:00",
                        "serial_number": 49532,
                        "event_type": "169361d0-62b8-411d-a8e6-019823805016_animals_sign",
                        "priority": 0,
                        "priority_label": "Gray",
                        "title": None,
                        "state": "active",
                        "url": "https://gundi-er.pamdas.org/api/v1.0/activity/event/c9ca269e-c873-423c-ab12-b72791b303b0",
                        "event_details": {
                            "animalpoached": "carcass",
                            "targetspecies": "birds.greyheron",
                            "actiontakenanimal": "conficated",
                            "numberofanimalpoached": 4,
                            "updates": [],
                        },
                        "patrols": ["2ce0001b-f2a3-4a4d-a108-a051019fc27d"],
                        "files": [],
                        "uri": "",
                        "device_id": "none",
                        "observation_type": "er_event",
                    },
                    {
                        "id": "b8e589fb-baf2-476a-96a6-0c87cf2ed847",
                        "owner": "na",
                        "integration_id": None,
                        "er_uuid": "b8e589fb-baf2-476a-96a6-0c87cf2ed847",
                        "location": {"latitude": 47.6839936, "longitude": -122.3589888},
                        "time": "2024-02-08 10:04:11.203000-06:00",
                        "created_at": "2024-02-08 10:04:25.052343-06:00",
                        "updated_at": "2024-02-08 10:04:25.051476-06:00",
                        "serial_number": 49539,
                        "event_type": "169361d0-62b8-411d-a8e6-019823805016_animals_sign",
                        "priority": 0,
                        "priority_label": "Gray",
                        "title": None,
                        "state": "active",
                        "url": "https://gundi-er.pamdas.org/api/v1.0/activity/event/b8e589fb-baf2-476a-96a6-0c87cf2ed847",
                        "event_details": {
                            "affiliation_whn": "ranger_whn",
                            "nameprotectedarea_whn": "phousithonesca_whn",
                            "patrolleaderorfocalpoint_whn": "ronniemartinez_whn",
                            "updates": [],
                        },
                        "patrols": ["2ce0001b-f2a3-4a4d-a108-a051019fc27d"],
                        "files": [],
                        "uri": "",
                        "device_id": "none",
                        "observation_type": "er_event",
                    },
                ],
                "id": "b575aeb0-4736-4df3-86ac-6a9ae47bcb4c",
                "leader": {
                    "id": "10ce2200-6565-401a-837d-fc153fb9db41",
                    "name": "Chris Doehring (Gundi)",
                    "subject_subtype": "ranger",
                    "additional": {
                        "ca_uuid": "test1230-62b8-411d-a8e6-019823805016",
                        "smart_member_id": "a99bbb3959d04ba7b02250b21b8a3d2b",
                    },
                    "is_active": True,
                },
                "patrol_type": "routine_patrol",
                "scheduled_start": None,
                "scheduled_end": "2024-02-09 02:00:00-06:00",
                "start_location": {
                    "latitude": 47.686076965642606,
                    "longitude": -122.35928648394253,
                },
                "time_range": {
                    "start_time": "2024-02-07T14:40:24-06:00",
                    "end_time": None,
                },
                "updates": [
                    {
                        "message": "Updated fields: ",
                        "time": "2024-02-08 16:04:27.683350+00:00",
                        "user": {
                            "username": "chrisd",
                            "first_name": "Chris",
                            "last_name": "Doehring",
                            "id": "8ffee729-7ff5-490e-9206-6f33a4038926",
                            "content_type": "accounts.user",
                        },
                        "type": "update_segment",
                    },
                    {
                        "message": "Report Added",
                        "time": "2024-02-08 16:04:25.087190+00:00",
                        "user": {
                            "username": "chrisd",
                            "first_name": "Chris",
                            "last_name": "Doehring",
                            "id": "8ffee729-7ff5-490e-9206-6f33a4038926",
                            "content_type": "accounts.user",
                        },
                        "type": "add_event",
                    },
                    {
                        "message": "Updated fields: ",
                        "time": "2024-02-08 16:03:33.073148+00:00",
                        "user": {
                            "username": "chrisd",
                            "first_name": "Chris",
                            "last_name": "Doehring",
                            "id": "8ffee729-7ff5-490e-9206-6f33a4038926",
                            "content_type": "accounts.user",
                        },
                        "type": "update_segment",
                    },
                    {
                        "message": "Report Added",
                        "time": "2024-02-07 20:42:36.667571+00:00",
                        "user": {
                            "username": "chrisd",
                            "first_name": "Chris",
                            "last_name": "Doehring",
                            "id": "8ffee729-7ff5-490e-9206-6f33a4038926",
                            "content_type": "accounts.user",
                        },
                        "type": "add_event",
                    },
                    {
                        "message": "Updated fields: Scheduled End",
                        "time": "2024-02-07 20:41:55.477668+00:00",
                        "user": {
                            "username": "chrisd",
                            "first_name": "Chris",
                            "last_name": "Doehring",
                            "id": "8ffee729-7ff5-490e-9206-6f33a4038926",
                            "content_type": "accounts.user",
                        },
                        "type": "update_segment",
                    },
                ],
                "track_points": [
                    {
                        "id": "c37a9148-f061-439d-ac60-5e4e4a8f03c8",
                        "location": {
                            "latitude": 47.68607630950026,
                            "longitude": -122.3592951927474,
                        },
                        "created_at": "2024-02-09 04:33:53+00:00",
                        "recorded_at": "2024-02-09 04:33:52+00:00",
                        "source": "cf72a238-b4d1-46c8-8b80-01f46c474e22",
                        "observation_details": {"accuracy": 4.7},
                    },
                    {
                        "id": "091a74bb-014a-44cf-8713-56ed866205ef",
                        "location": {
                            "latitude": 47.68612342657477,
                            "longitude": -122.3592770754273,
                        },
                        "created_at": "2024-02-09 04:43:53+00:00",
                        "recorded_at": "2024-02-09 04:43:52+00:00",
                        "source": "cf72a238-b4d1-46c8-8b80-01f46c474e22",
                        "observation_details": {"accuracy": 4.7},
                    },
                    {
                        "id": "cf6fd52f-37d4-4411-a761-a1721b792deb",
                        "location": {
                            "latitude": 47.68612342657477,
                            "longitude": -122.3592770754273,
                        },
                        "created_at": "2024-02-09 04:53:53+00:00",
                        "recorded_at": "2024-02-09 04:53:52+00:00",
                        "source": "cf72a238-b4d1-46c8-8b80-01f46c474e22",
                        "observation_details": {"accuracy": 4.7},
                    },
                ],
            }
        ],
        "observation_type": "er_patrol",
        "state": "open",
        "updates": [
            {
                "message": "Patrol Added",
                "time": "2024-02-07 20:40:24.507001+00:00",
                "user": {
                    "username": "chrisd",
                    "first_name": "Chris",
                    "last_name": "Doehring",
                    "id": "8ffee729-7ff5-490e-9206-6f33a4038926",
                    "content_type": "accounts.user",
                },
                "type": "add_patrol",
            },
            {
                "message": "Updated fields: ",
                "time": "2024-02-08 16:04:27.683350+00:00",
                "user": {
                    "username": "chrisd",
                    "first_name": "Chris",
                    "last_name": "Doehring",
                    "id": "8ffee729-7ff5-490e-9206-6f33a4038926",
                    "content_type": "accounts.user",
                },
                "type": "update_segment",
            },
            {
                "message": "Report Added",
                "time": "2024-02-08 16:04:25.087190+00:00",
                "user": {
                    "username": "chrisd",
                    "first_name": "Chris",
                    "last_name": "Doehring",
                    "id": "8ffee729-7ff5-490e-9206-6f33a4038926",
                    "content_type": "accounts.user",
                },
                "type": "add_event",
            },
            {
                "message": "Updated fields: ",
                "time": "2024-02-08 16:03:33.073148+00:00",
                "user": {
                    "username": "chrisd",
                    "first_name": "Chris",
                    "last_name": "Doehring",
                    "id": "8ffee729-7ff5-490e-9206-6f33a4038926",
                    "content_type": "accounts.user",
                },
                "type": "update_segment",
            },
            {
                "message": "Report Added",
                "time": "2024-02-07 20:42:36.667571+00:00",
                "user": {
                    "username": "chrisd",
                    "first_name": "Chris",
                    "last_name": "Doehring",
                    "id": "8ffee729-7ff5-490e-9206-6f33a4038926",
                    "content_type": "accounts.user",
                },
                "type": "add_event",
            },
            {
                "message": "Updated fields: Scheduled End",
                "time": "2024-02-07 20:41:55.477668+00:00",
                "user": {
                    "username": "chrisd",
                    "first_name": "Chris",
                    "last_name": "Doehring",
                    "id": "8ffee729-7ff5-490e-9206-6f33a4038926",
                    "content_type": "accounts.user",
                },
                "type": "update_segment",
            },
        ],
    }


@pytest.fixture
def raw_observation_er_patrol_attributes():
    return {}


@pytest.fixture
def transformed_observation_position():
    return {
        "manufacturer_id": "018910980",
        "source_type": "tracking-device",
        "subject_name": "Logistics Truck test",
        "recorded_at": datetime.datetime(
            2023,
            3,
            2,
            18,
            47,
            tzinfo=datetime.timezone(datetime.timedelta(seconds=7200)),
        ),
        "location": {"lon": 35.43929, "lat": -1.59083},
        "additional": {"voltage": "7.4", "fuel_level": 71, "speed": "41 kph"},
    }


@pytest.fixture
def transformed_observation_attributes():
    return {
        "observation_type": "ps",
        "device_id": "018910980",
        "outbound_config_id": "1c19dc7e-73e2-4af3-93f5-a1cb322e5add",
        "integration_id": "36485b4f-88cd-49c4-a723-0ddff1f580c4",
    }


@pytest.fixture
def transformed_observation_gcp_message():
    return b'{"manufacturer_id": "018910980", "source_type": "tracking-device", "subject_name": "Logistics Truck test", "recorded_at": "2023-03-02 18:47:00+02:00", "location": {"lon": 35.43929, "lat": -1.59083}, "additional": {"voltage": "7.4", "fuel_level": 71, "speed": "41 kph"}}'


@pytest.fixture
def outbound_configuration_gcp_pubsub():
    return schemas_v1.OutboundConfiguration.parse_obj(
        {
            "id": "1c19dc7e-73e2-4af3-93f5-a1cb322e5add",
            "type": "f61b0c60-c863-44d7-adc6-d9b49b389e69",
            "owner": "088a191a-bcf3-471b-9e7d-6ba8bc71be9e",
            "name": "[Internal] AI2 Test -  Bidtrack to  ER load test",
            "endpoint": "https://gundi-load-testing.pamdas.org/api/v1.0",
            "state": {},
            "login": "",
            "password": "",
            "token": "0890d87681cd1d01ad07c2d0f57d15d6079ae7d7",
            "type_slug": "earth_ranger",
            "inbound_type_slug": "bidtrack",
            "additional": {"broker": "gcp_pubsub"},
        }
    )


@pytest.fixture
def smart_outbound_configuration_gcp_pubsub():
    return schemas_v1.OutboundConfiguration.parse_obj(
        {
            "id": "38ebbae6-2535-43f9-be88-96f9daec83f3",
            "type": "f61b0c60-c863-44d7-adc6-d9b49b389e69",
            "owner": "1111191a-bcf3-471b-9e7d-6ba8bc71be9e",
            "name": "[Internal] AI2 Test -  GFW to  SMART Connect",
            "endpoint": "https://fakesmartconnectsite.smartconservationtools.org/server",
            "state": {},
            "login": "test",
            "password": "test",  # pragma: allowlist secret
            "token": "",  # pragma: allowlist secret
            "type_slug": "smart_connect",
            "inbound_type_slug": "gfw",
            "additional": {
                "broker": "gcp_pubsub",
                "topic": "smart-dispatcher-xyz-topic",
                "version": "7.5.7",
                "ca_uuids": ["b48f15a7-dd87-4fb0-af42-a6a7d893705f"],
            },
        }
    )


@pytest.fixture
def outbound_configuration_default():
    return schemas_v1.OutboundConfiguration.parse_obj(
        {
            "id": "1c19dc7e-73e2-4af3-93f5-a1cb322e5add",
            "type": "f61b0c60-c863-44d7-adc6-d9b49b389e69",
            "owner": "088a191a-bcf3-471b-9e7d-6ba8bc71be9e",
            "name": "[Internal] AI2 Test -  Bidtrack to  ER load test",
            "endpoint": "https://gundi-load-testing.pamdas.org/api/v1.0",
            "state": {},
            "login": "",
            "password": "",
            "token": "0890d87681cd1d01ad07c2d0f57d15d6079ae7d7",
            "type_slug": "earth_ranger",
            "inbound_type_slug": "bidtrack",
            "additional": {},
        }
    )


# ToDo: Find a common place for mocks and testing utilities that can be reused across services
@pytest.fixture
def mock_gundi_client_v2(
    mocker,
    connection_v2,
    destination_integration_v2,
    route_v2,
):
    mock_client = mocker.MagicMock()
    mock_client.get_connection_details.return_value = async_return(connection_v2)
    mock_client.get_integration_details.return_value = async_return(
        destination_integration_v2
    )
    mock_client.get_route_details.return_value = async_return(route_v2)
    mock_client.__aenter__.return_value = mock_client
    return mock_client


@pytest.fixture
def mock_gundi_client_v2_class(mocker, mock_gundi_client_v2):
    mock_gundi_client_v2_class = mocker.MagicMock()
    mock_gundi_client_v2_class.return_value = mock_gundi_client_v2
    return mock_gundi_client_v2_class


@pytest.fixture
def destination_integration_v2():
    # ToDo: Move mocks used by routing, dispatchers, etc. to a common place
    return schemas_v2.Integration.parse_obj(
        {
            "id": "338225f3-91f9-4fe1-b013-353a229ce504",
            "name": "ER Load Testing",
            "base_url": "https://gundi-load-testing.pamdas.org",
            "enabled": True,
            "type": {
                "id": "45c66a61-71e4-4664-a7f2-30d465f87aa6",
                "name": "EarthRanger",
                "value": "earth_ranger",
                "description": "Integration type for Earth Ranger Sites",
                "actions": [
                    {
                        "id": "43ec4163-2f40-43fc-af62-bca1db77c06b",
                        "type": "auth",
                        "name": "Authenticate",
                        "value": "auth",
                        "description": "Authenticate against Earth Ranger",
                        "schema": {
                            "type": "object",
                            "required": ["token"],
                            "properties": {"token": {"type": "string"}},
                        },
                    },
                    {
                        "id": "036c2098-f494-40ec-a595-710b314d5ea5",
                        "type": "pull",
                        "name": "Pull Positions",
                        "value": "pull_positions",
                        "description": "Pull position data from an Earth Ranger site",
                        "schema": {
                            "type": "object",
                            "required": ["endpoint"],
                            "properties": {"endpoint": {"type": "string"}},
                        },
                    },
                    {
                        "id": "9286bb71-9aca-425a-881f-7fe0b2dba4f4",
                        "type": "push",
                        "name": "Push Events",
                        "value": "push_events",
                        "description": "EarthRanger sites support sending Events (a.k.a Reports)",
                        "schema": {},
                    },
                    {
                        "id": "aae0cf50-fbc7-4810-84fd-53fb75020a43",
                        "type": "push",
                        "name": "Push Positions",
                        "value": "push_positions",
                        "description": "Push position data to an Earth Ranger site",
                        "schema": {
                            "type": "object",
                            "required": ["endpoint"],
                            "properties": {"endpoint": {"type": "string"}},
                        },
                    },
                    {
                        "id": "1286bb71-9aca-425a-881f-7fe0b2dba4b5",
                        "type": "push",
                        "name": "Push Messages",
                        "value": "push_messages",
                        "description": "Push text messages to EarthRanger",
                        "schema": {},
                    },
                ],
            },
            "owner": {
                "id": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
                "name": "Test Organization",
                "description": "",
            },
            "configurations": [
                {
                    "id": "013ea7ce-4944-4f7e-8a2f-e5338b3741ce",
                    "integration": "338225f3-91f9-4fe1-b013-353a229ce504",
                    "action": {
                        "id": "43ec4163-2f40-43fc-af62-bca1db77c06b",
                        "type": "auth",
                        "name": "Authenticate",
                        "value": "auth",
                    },
                    "data": {"token": "1390d87681cd1d01ad07c2d0f57d15d6079ae9ab"},
                },
                {
                    "id": "5de91c7b-f28a-4ce7-8137-273ac10674d2",
                    "integration": "338225f3-91f9-4fe1-b013-353a229ce504",
                    "action": {
                        "id": "aae0cf50-fbc7-4810-84fd-53fb75020a43",
                        "type": "push",
                        "name": "Push Positions",
                        "value": "push_positions",
                    },
                    "data": {"endpoint": "api/v1/positions"},
                },
                {
                    "id": "7947b19e-1d2d-4ca3-bd6c-74976ae1de68",
                    "integration": "338225f3-91f9-4fe1-b013-353a229ce504",
                    "action": {
                        "id": "036c2098-f494-40ec-a595-710b314d5ea5",
                        "type": "pull",
                        "name": "Pull Positions",
                        "value": "pull_positions",
                    },
                    "data": {"endpoint": "api/v1/positions"},
                },
            ],
            "additional": {
                "topic": "destination-v2-338225f3-91f9-4fe1-b013-353a229ce504-dev",
                "broker": "gcp_pubsub",
            },
            "default_route": {
                "id": "38dd8ec2-b3ee-4c31-940e-b6cc9c1f4326",
                "name": "Mukutan - Load Testing",
            },
            "status": "healthy",
            "status_details": "",
        }
    )


@pytest.fixture
def destination_integration_v2_inreach():
    return schemas_v2.Integration.parse_obj(
        {
            "id": "abc0946d-15b0-4308-b93d-e0470b6d33b6",
            "name": "InReach API",
            "base_url": "https://test.inreach.com",
            "enabled": True,
            "type": {
                "id": "f61b0c60-c863-44d7-adc6-d9b49b389e69",
                "name": "InReach",
                "value": "inreach",
                "description": "Integration type for InReach API",
                "actions": [
                    {
                        "id": "1c9e2383-8154-404a-90b1-f4c591627d51",
                        "type": "auth",
                        "name": "Authenticate",
                        "value": "auth",
                        "description": "",
                        "schema": {
                            "type": "object",
                            "required": ["api_key"],
                            "properties": {"api_key": {"type": "string"}},
                        },
                    },
                    {
                        "id": "1109d75f-6456-4060-b2da-385e9ddde554",
                        "type": "push",
                        "name": "Push Messages",
                        "value": "push_messages",
                        "description": "Push messages to InReach",
                        "schema": {},
                    },
                ],
            },
            "owner": {
                "id": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
                "name": "Test Organization",
            },
            "configurations": [
                {
                    "id": "92d9fb49-f3c6-473f-9220-59745e854da5",
                    "integration": "abc0946d-15b0-4308-b93d-e0470b6d33b6",
                    "action": {
                        "id": "1109d75f-6456-4060-b2da-385e9ddde554",
                        "type": "push",
                        "name": "Push Messages",
                        "value": "push_messages",
                    },
                    "data": {},
                },
                {
                    "id": "61a55770-0cf4-42b9-9054-ba0222ee5bc3",
                    "integration": "abc0946d-15b0-4308-b93d-e0470b6d33b6",
                    "action": {
                        "id": "1c9e2383-8154-404a-90b1-f4c591627d51",
                        "type": "auth",
                        "name": "Authenticate",
                        "value": "auth",
                    },
                    "data": {"api_key": "fakekey123"},  # pragma: allowlist secret
                },
            ],
            "webhook_configuration": None,
            "additional": {
                "topic": "inreach-messages-topic",
                "broker": "gcp_pubsub",
            },
            "default_route": None,
            "status": "healthy",
            "status_details": "",
        }
    )


@pytest.fixture
def destination_integration_v2_wpswatch():
    return schemas_v2.Integration.parse_obj(
        {
            "id": "79bef222-74aa-4065-88a8-ac9656246693",
            "name": "WPS Watch QA",
            "base_url": "https://wpswatch-api-qa.azurewebsites.net",
            "enabled": True,
            "type": {
                "id": "80bda69d-51ed-4c33-8420-0104e62d6639",
                "name": "WPS Watch QA",
                "value": "wps_watch",
                "description": "",
                "actions": [
                    {
                        "id": "1c9e2383-8154-404a-90b1-f4c591627d51",
                        "type": "auth",
                        "name": "Authenticate",
                        "value": "auth",
                        "description": "",
                        "schema": {
                            "type": "object",
                            "required": ["api_key"],
                            "properties": {"api_key": {"type": "string"}},
                        },
                    },
                    {
                        "id": "1109d75f-6456-4060-b2da-385e9ddde554",
                        "type": "push",
                        "name": "Push Events",
                        "value": "push_events",
                        "description": "",
                        "schema": {
                            "type": "object",
                            "required": ["upload_domain"],
                            "properties": {"upload_domain": {"type": "string"}},
                        },
                    },
                ],
                "webhook": None,
            },
            "owner": {
                "id": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
                "name": "Test Org",
                "description": "",
            },
            "configurations": [
                {
                    "id": "92d9fb49-f3c6-473f-9220-59745e854da5",
                    "integration": "79bef222-74aa-4065-88a8-ac9656246693",
                    "action": {
                        "id": "1109d75f-6456-4060-b2da-385e9ddde554",
                        "type": "push",
                        "name": "Push Events",
                        "value": "push_events",
                    },
                    "data": {"upload_domain": "upload-qa.wpswatch.org"},
                },
                {
                    "id": "61a55770-0cf4-42b9-9054-ba0222ee5bc3",
                    "integration": "79bef222-74aa-4065-88a8-ac9656246693",
                    "action": {
                        "id": "1c9e2383-8154-404a-90b1-f4c591627d51",
                        "type": "auth",
                        "name": "Authenticate",
                        "value": "auth",
                    },
                    "data": {"api_key": "fakekey123"},  # pragma: allowlist secret
                },
            ],
            "webhook_configuration": None,
            "additional": {
                "topic": "wpswatch-api-qa-wpswatch-sTHDqqN-topic",
                "broker": "gcp_pubsub",
            },
            "default_route": None,
            "status": "healthy",
            "status_details": "",
        }
    )


@pytest.fixture
def destination_integration_v2_traptagger():
    return schemas_v2.Integration.parse_obj(
        {
            "id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
            "name": "Trap Tagger",
            "base_url": "https://test.traptagger.com",
            "enabled": True,
            "type": {
                "id": "190e3710-3a29-4710-b932-f951222209a7",
                "name": "TrapTagger",
                "value": "trap_tagger",
                "description": "",
                "actions": [
                    {
                        "id": "1c9e2383-8154-404a-90b1-f4c591627d51",
                        "type": "auth",
                        "name": "Authenticate",
                        "value": "auth",
                        "description": "",
                        "schema": {
                            "type": "object",
                            "required": ["apikey"],
                            "properties": {"apikey": {"type": "string"}},
                        },
                    },
                    {
                        "id": "1109d75f-6456-4060-b2da-385e9ddde554",
                        "type": "push",
                        "name": "Push Events",
                        "value": "push_events",
                        "description": "",
                        "schema": {},
                    },
                ],
                "webhook": None,
            },
            "owner": {
                "id": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
                "name": "Test Org",
                "description": "",
            },
            "configurations": [
                {
                    "id": "92d9fb49-f3c6-473f-9220-59745e854da5",
                    "integration": "79bef222-74aa-4065-88a8-ac9656246693",
                    "action": {
                        "id": "1109d75f-6456-4060-b2da-385e9ddde554",
                        "type": "push",
                        "name": "Push Events",
                        "value": "push_events",
                    },
                    "data": {},
                },
                {
                    "id": "61a55770-0cf4-42b9-9054-ba0222ee5bc3",
                    "integration": "79bef222-74aa-4065-88a8-ac9656246693",
                    "action": {
                        "id": "1c9e2383-8154-404a-90b1-f4c591627d51",
                        "type": "auth",
                        "name": "Authenticate",
                        "value": "auth",
                    },
                    "data": {"apikey": "fakekey123"},  # pragma: allowlist secret
                },
            ],
            "webhook_configuration": None,
            "additional": {
                "topic": "wpswatch-api-qa-wpswatch-sTHDqqN-topic",
                "broker": "gcp_pubsub",
            },
            "default_route": None,
            "status": "healthy",
            "status_details": "",
        }
    )


@pytest.fixture
def destination_integration_v2_smart():
    return schemas_v2.Integration.parse_obj(
        {
            "id": "0eecad72-1d2e-43b1-a169-c419c81fc957",
            "name": "SMART Demo (Mariano)",
            "base_url": "https://smartdemoconnect.smartconservationtools.org:443/server",
            "enabled": True,
            "type": {
                "id": "be324645-de3f-4f88-beb7-c9a287f938f7",
                "name": "SMART Connect",
                "value": "smart_connect",
                "description": "",
                "actions": [
                    {
                        "id": "a306a813-047c-4868-9f17-4e7732000b44",
                        "type": "auth",
                        "name": "Authenticate",
                        "value": "auth",
                        "description": "Authenticate against smart connect",
                        "schema": {
                            "type": "object",
                            "title": "SMARTAuthActionConfig",
                            "required": ["login", "password"],
                            "properties": {
                                "login": {"type": "string", "title": "Login"},
                                "password": {"type": "string", "title": "Password"},
                            },
                        },
                    },
                    {
                        "id": "eb78e965-f955-461a-9325-f678c9c57fdb",
                        "type": "push",
                        "name": "Push Events",
                        "value": "push_events",
                        "description": "Send Events to SMART Connect (a.k.a Incidents or waypoints)",
                        "schema": {
                            "type": "object",
                            "title": "SMARTPushEventActionConfig",
                            "properties": {
                                "ca_uuids": {
                                    "type": "array",
                                    "items": {"type": "string", "format": "uuid"},
                                    "title": "Ca Uuids",
                                }
                            },
                        },
                    },
                ],
                "webhook": None,
            },
            "owner": {
                "id": "45018398-7a2a-4f48-8971-39a2710d5dbd",
                "name": "Gundi Engineering",
                "description": "Test organization",
            },
            "configurations": [
                {
                    "id": "fc2484d5-ad49-4895-9afe-f122c2fdda8f",
                    "integration": "0eecad72-1d2e-43b1-a169-c419c81fc957",
                    "action": {
                        "id": "eb78e965-f955-461a-9325-f678c9c57fdb",
                        "type": "push",
                        "name": "Push Events",
                        "value": "push_events",
                    },
                    "data": {
                        "version": "7.5.7",
                        "ca_uuids": ["169361d0-62b8-411d-a8e6-019823805016"],
                        "configurable_models_lists": {
                            "169361d0-62b8-411d-a8e6-019823805016": [
                                {
                                    "name": "គូលែន ព្រហ្មទេព 072022",
                                    "uuid": "303b2e0a-d4b7-41b8-b6dc-065c9a661c7b",
                                    "ca_id": "SMART",
                                    "ca_name": "Demo Conservation Area",
                                    "ca_uuid": "169361d0-62b8-411d-a8e6-019823805016",
                                    "translations": [
                                        {
                                            "value": "គូលែន ព្រហ្មទេព 072022",
                                            "language_code": "en",
                                        },
                                        {
                                            "value": "គូលែន ព្រហ្មទេព 072022",
                                            "language_code": "km",
                                        },
                                    ],
                                    "use_with_earth_ranger": False,
                                },
                                {
                                    "name": "ខនិក គូលែន ព្រហ្មទេព 092022",
                                    "uuid": "a645f302-7fb0-4a29-a0ce-9d0092652803",
                                    "ca_id": "SMART",
                                    "ca_name": "Demo Conservation Area",
                                    "ca_uuid": "169361d0-62b8-411d-a8e6-019823805016",
                                    "translations": [
                                        {
                                            "value": "ខនិក គូលែន ព្រហ្មទេព 092022",
                                            "language_code": "en",
                                        }
                                    ],
                                    "use_with_earth_ranger": True,
                                },
                            ]
                        },
                    },
                },
                {
                    "id": "2a0e8863-2ed9-4f9d-9496-1a08ac483df9",
                    "integration": "0eecad72-1d2e-43b1-a169-c419c81fc957",
                    "action": {
                        "id": "a306a813-047c-4868-9f17-4e7732000b44",
                        "type": "auth",
                        "name": "Authenticate",
                        "value": "auth",
                    },
                    "data": {
                        "login": "fakeuser",  # ggignore
                        "password": "fakepass",  # ggignore # pragma: allowlist secret
                    },
                },
            ],
            "webhook_configuration": None,
            "additional": {
                "topic": "smartdemoconnect-smartcon-c0PRUpN-topic",
                "broker": "gcp_pubsub",
            },
            "default_route": None,
            "status": "healthy",
            "status_details": "",
        }
    )


@pytest.fixture
def connection_v2():
    return schemas_v2.Connection.parse_obj(
        {
            "id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
            "provider": {
                "id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
                "name": "Trap Tagger",
                "owner": {
                    "id": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
                    "name": "Test Organization",
                },
                "type": {
                    "id": "190e3710-3a29-4710-b932-f951222209a7",
                    "name": "TrapTagger",
                    "value": "traptagger",
                },
                "base_url": "https://test.traptagger.com",
                "status": "healthy",
                "status_details": "",
            },
            "destinations": [
                {
                    "id": "338225f3-91f9-4fe1-b013-353a229ce504",
                    "name": "ER Load Testing",
                    "owner": {
                        "id": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
                        "name": "Test Organization",
                    },
                    "type": {
                        "id": "45c66a61-71e4-4664-a7f2-30d465f87aa6",
                        "name": "EarthRanger",
                        "value": "earth_ranger",
                    },
                    "base_url": "https://gundi-load-testing.pamdas.org",
                    "status": "healthy",
                    "status_details": "",
                }
            ],
            "routing_rules": [
                {
                    "id": "835897f9-1ef2-4d99-9c6c-ea2663380c1f",
                    "name": "TrapTagger Default Route",
                }
            ],
            "default_route": {
                "id": "835897f9-1ef2-4d99-9c6c-ea2663380c1f",
                "name": "TrapTagger Default Route",
            },
            "owner": {
                "id": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
                "name": "Test Organization",
                "description": "",
            },
            "status": "healthy",
        }
    )


@pytest.fixture
def connection_v2_traptagger_to_wpswatch():
    return schemas_v2.Connection.parse_obj(
        {
            "id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
            "provider": {
                "id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
                "name": "Trap Tagger",
                "owner": {
                    "id": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
                    "name": "Test Organization",
                },
                "type": {
                    "id": "190e3710-3a29-4710-b932-f951222209a7",
                    "name": "TrapTagger",
                    "value": "traptagger",
                },
                "base_url": "https://test.traptagger.com",
                "status": "healthy",
                "status_details": "",
            },
            "destinations": [
                {
                    "id": "79bef222-74aa-4065-88a8-ac9656246693",
                    "name": "WPS Watch QA",
                    "owner": {
                        "id": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
                        "name": "Test Organization",
                    },
                    "type": {
                        "id": "45c66a61-71e4-4664-a7f2-30d465f87aa6",
                        "name": "WPS Watch",
                        "value": "wps_watch",
                    },
                    "base_url": "https://wpswatch-api-qa.azurewebsites.net",
                    "status": "healthy",
                    "status_details": "",
                }
            ],
            "routing_rules": [
                {
                    "id": "835897f9-1ef2-4d99-9c6c-ea2663380c1f",
                    "name": "TrapTagger Default Route",
                }
            ],
            "default_route": {
                "id": "835897f9-1ef2-4d99-9c6c-ea2663380c1f",
                "name": "TrapTagger Default Route",
            },
            "owner": {
                "id": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
                "name": "Test Organization",
                "description": "",
            },
            "status": "healthy",
        }
    )


@pytest.fixture
def connection_v2_wpswatch_to_traptagger():
    return schemas_v2.Connection.parse_obj(
        {
            "id": "79bef222-74aa-4065-88a8-ac9656246693",
            "provider": {
                "id": "79bef222-74aa-4065-88a8-ac9656246693",
                "name": "WPS Watch Push",
                "owner": {
                    "id": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
                    "name": "Test Organization",
                },
                "type": {
                    "id": "45c66a61-71e4-4664-a7f2-30d465f87aa6",
                    "name": "WPS Watch",
                    "value": "wps_watch_r",
                },
                "base_url": "",
                "status": "healthy",
                "status_details": "",
            },
            "destinations": [
                {
                    "id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
                    "name": "Trap Tagger",
                    "owner": {
                        "id": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
                        "name": "Test Organization",
                    },
                    "type": {
                        "id": "190e3710-3a29-4710-b932-f951222209a7",
                        "name": "TrapTagger",
                        "value": "trap_tagger",
                    },
                    "base_url": "https://test.traptagger.com",
                    "status": "healthy",
                    "status_details": "",
                }
            ],
            "routing_rules": [
                {
                    "id": "835897f9-1ef2-4d99-9c6c-ea2663380c1f",
                    "name": "TrapTagger Default Route",
                }
            ],
            "default_route": {
                "id": "835897f9-1ef2-4d99-9c6c-ea2663380c1f",
                "name": "TrapTagger Default Route",
            },
            "owner": {
                "id": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
                "name": "Test Organization",
                "description": "",
            },
            "status": "healthy",
        }
    )


@pytest.fixture
def connection_v2_traptagger_to_smart():
    return schemas_v2.Connection.parse_obj(
        {
            "id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
            "provider": {
                "id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
                "name": "Trap Tagger",
                "owner": {
                    "id": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
                    "name": "Test Organization",
                },
                "type": {
                    "id": "190e3710-3a29-4710-b932-f951222209a7",
                    "name": "TrapTagger",
                    "value": "traptagger",
                },
                "base_url": "https://test.traptagger.com",
                "status": "healthy",
                "status_details": "",
            },
            "destinations": [
                {
                    "id": "0eecad72-1d2e-43b1-a169-c419c81fc957",
                    "name": "SMART Demo (Mariano)",
                    "owner": {
                        "id": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
                        "name": "Test Organization",
                    },
                    "type": {
                        "id": "be324645-de3f-4f88-beb7-c9a287f938f7",
                        "name": "SMART Connect",
                        "value": "smart_connect",
                    },
                    "base_url": "https://smartdemoconnect.smartconservationtools.org:443/server",
                    "status": "healthy",
                    "status_details": "",
                }
            ],
            "routing_rules": [
                {
                    "id": "835897f9-1ef2-4d99-9c6c-ea2663380c1f",
                    "name": "TrapTagger Default Route",
                }
            ],
            "default_route": {
                "id": "835897f9-1ef2-4d99-9c6c-ea2663380c1f",
                "name": "TrapTagger Default Route",
            },
            "owner": {
                "id": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
                "name": "Test Organization",
                "description": "",
            },
            "status": "healthy",
        }
    )


@pytest.fixture
def connection_v2_inreach_to_er():
    return schemas_v2.Connection.parse_obj(
        {
            "id": "abc0946d-15b0-4308-b93d-e0470b6d33b6",
            "provider": {
                "id": "abc0946d-15b0-4308-b93d-e0470b6d33b6",
                "name": "InReach WH",
                "owner": {
                    "id": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
                    "name": "Test Organization",
                },
                "type": {
                    "id": "200e3710-3a29-4710-b932-f951222209f4",
                    "name": "InReach",
                    "value": "inreach",
                },
                "base_url": "https://test.inreach.com",
                "status": "healthy",
                "status_details": "",
            },
            "destinations": [
                {
                    "id": f"338225f3-91f9-4fe1-b013-353a229ce504",
                    "name": "ER Load Testing",
                    "owner": {
                        "id": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
                        "name": "Test Organization",
                    },
                    "type": {
                        "id": "45c66a61-71e4-4664-a7f2-30d465f87aa6",
                        "name": "EarthRanger",
                        "value": "earth_ranger",
                    },
                    "base_url": "https://gundi-load-testing.pamdas.org",
                    "status": "healthy",
                    "status_details": "",
                }
            ],
            "routing_rules": [
                {
                    "id": "835897f9-1ef2-4d99-9c6c-ea2663380c1f",
                    "name": "InReach WH Default Route",
                }
            ],
            "default_route": {
                "id": "835897f9-1ef2-4d99-9c6c-ea2663380c1f",
                "name": "InReach WH Default Route",
            },
            "owner": {
                "id": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
                "name": "Test Organization",
                "description": "",
            },
            "status": "healthy",
        }
    )


@pytest.fixture
def connection_v2_er_to_inreach():
    return schemas_v2.Connection.parse_obj(
        {
            "id": "338225f3-91f9-4fe1-b013-353a229ce504",
            "provider": {
                "id": f"338225f3-91f9-4fe1-b013-353a229ce504",
                "name": "ER Load Testing",
                "owner": {
                    "id": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
                    "name": "Test Organization",
                },
                "type": {
                    "id": "45c66a61-71e4-4664-a7f2-30d465f87aa6",
                    "name": "EarthRanger",
                    "value": "earth_ranger",
                },
                "base_url": "https://gundi-load-testing.pamdas.org",
                "status": "healthy",
                "status_details": "",
            },
            "destinations": [
                {
                    "id": "abc0946d-15b0-4308-b93d-e0470b6d33b6",
                    "name": "InReach API",
                    "owner": {
                        "id": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
                        "name": "Test Organization",
                    },
                    "type": {
                        "id": "200e3710-3a29-4710-b932-f951222209f4",
                        "name": "InReach",
                        "value": "inreach",
                    },
                    "base_url": "https://test.inreach.com",
                    "status": "healthy",
                    "status_details": "",
                }
            ],
            "routing_rules": [
                {
                    "id": "835897f9-1ef2-4d99-9c6c-ea2663380c1f",
                    "name": "ER Default Route",
                }
            ],
            "default_route": {
                "id": "835897f9-1ef2-4d99-9c6c-ea2663380c1f",
                "name": "ER Default Route",
            },
            "owner": {
                "id": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
                "name": "Test Organization",
                "description": "",
            },
            "status": "healthy",
        }
    )


@pytest.fixture
def route_v2():
    return schemas_v2.Route.parse_obj(
        {
            "id": "835897f9-1ef2-4d99-9c6c-ea2663380c1f",
            "name": "TrapTagger Default Route",
            "owner": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
            "data_providers": [
                {
                    "id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
                    "name": "Trap Tagger",
                    "owner": {
                        "id": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
                        "name": "Test Organization",
                    },
                    "type": {
                        "id": "190e3710-3a29-4710-b932-f951222209a7",
                        "name": "TrapTagger",
                        "value": "traptagger",
                    },
                    "base_url": "https://test.traptagger.com",
                    "status": "healthy",
                    "status_details": "",
                }
            ],
            "destinations": [
                {
                    "id": "338225f3-91f9-4fe1-b013-353a229ce504",
                    "name": "ER Load Testing",
                    "owner": {
                        "id": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
                        "name": "Test Organization",
                    },
                    "type": {
                        "id": "45c66a61-71e4-4664-a7f2-30d465f87aa6",
                        "name": "EarthRanger",
                        "value": "earth_ranger",
                    },
                    "base_url": "https://gundi-load-testing.pamdas.org",
                    "status": "healthy",
                    "status_details": "",
                }
            ],
            "configuration": {
                "id": "1a3e3e73-94ad-42cb-a765-09a7193ae0b1",
                "name": "Trap Tagger to ER - Event Type Mapping",
                "data": {
                    "field_mappings": {
                        "ddd0946d-15b0-4308-b93d-e0470b6d33b6": {
                            "ev": {
                                "338225f3-91f9-4fe1-b013-353a229ce504": {
                                    "map": {
                                        "Leopard": "leopard_sighting",
                                        "Wilddog": "wild_dog_sighting",
                                    },
                                    "default": "wildlife_sighting_rep",
                                    "provider_field": "event_details__species",
                                    "destination_field": "event_type",
                                }
                            }
                        }
                    }
                },
            },
            "additional": {},
        }
    )


@pytest.fixture
def route_v2_with_provider_key_field_mapping(connection_v2):
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    return schemas_v2.Route.parse_obj(
        {
            "id": "835897f9-1ef2-4d99-9c6c-ea2663380c1f",
            "name": "Telonics Migrated Connection Default Route",
            "owner": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
            "data_providers": [
                {
                    "id": provider_id,
                    "name": "Telonics Migrated Connection",
                    "owner": {
                        "id": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
                        "name": "Test Organization",
                    },
                    "type": {
                        "id": "190e3710-3a29-4710-b932-f951222209a7",
                        "name": "Telonics",
                        "value": "telonics",
                    },
                    "base_url": "",
                    "status": "healthy",
                    "status_details": "",
                }
            ],
            "destinations": [
                {
                    "id": destination_id,
                    "name": "ER Load Testing",
                    "owner": {
                        "id": "e2d1b0fc-69fe-408b-afc5-7f54872730c0",
                        "name": "Test Organization",
                    },
                    "type": {
                        "id": "45c66a61-71e4-4664-a7f2-30d465f87aa6",
                        "name": "EarthRanger",
                        "value": "earth_ranger",
                    },
                    "base_url": "https://gundi-load-testing.pamdas.org",
                    "status": "healthy",
                    "status_details": "",
                }
            ],
            "configuration": {
                "id": "1a3e3e73-94ad-42cb-a765-09a7193ae0b1",
                "name": "Telonics Migrated Connection - Provider key Mapping",
                "data": {
                    "field_mappings": {
                        provider_id: {
                            "obv": {
                                destination_id: {
                                    "default": "telonics-collars",
                                    "destination_field": "provider_key",
                                }
                            }
                        }
                    }
                },
            },
            "additional": {},
        }
    )


@pytest.fixture
def raw_observation_v2():
    return {
        "event_id": "d8523635-546e-4cd4-ad6a-d9fdf494698e",
        "timestamp": "2024-07-22 11:51:12.684788+00:00",
        "schema_version": "v1",
        "payload": {
            "gundi_id": "9573c2b0-3fd7-4502-884b-43d5628ce7a8",
            "related_to": None,
            "owner": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
            "data_provider_id": "f870e228-4a65-40f0-888c-41bdc1124c3c",
            "annotations": {},
            "source_id": "eb47e6ad-a677-4218-856b-59ad4d8d0e73",
            "external_source_id": "test-device",
            "source_name": "Mariano",
            "type": "tracking-device",
            "subject_type": "mm-tracker",
            "recorded_at": "2024-07-22 11:51:05+00:00",
            "location": {
                "lat": -51.688246,
                "lon": -72.704459,
                "alt": 0,
                "hdop": None,
                "vdop": None,
            },
            "additional": {"speed_kmph": 30},
            "observation_type": "obv",
        },
        "event_type": "ObservationReceived",
    }


@pytest.fixture
def raw_observation_v2_attributes():
    return {
        "observation_type": "obv",
        "gundi_version": "v2",
        "gundi_id": "9573c2b0-3fd7-4502-884b-43d5628ce7a8",
    }


@pytest.fixture
def raw_event_v2():
    return {
        "event_type": "EventReceived",
        "event_id": "21bda0d3-b2de-4285-9540-0b6a84881b52",
        "timestamp": "2024-07-04 18:09:19.853663+00:00",
        "schema_version": "v1",
        "payload": {
            "gundi_id": "27e9329e-82ab-44f9-8997-b09f123eccbb",
            "related_to": None,
            "owner": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
            "data_provider_id": "f870e228-4a65-40f0-888c-41bdc1124c3c",
            "annotations": {},
            "source_id": "ac1b9cdc-a193-4515-b446-b177bcc5f342",
            "external_source_id": "camera123",
            "recorded_at": "2024-07-04 18:09:12+00:00",
            "location": {
                "lat": 13.688635,
                "lon": 13.783064,
                "alt": 0.0,
                "hdop": None,
                "vdop": None,
            },
            "title": "Animal Detected Test Event",
            "event_type": "wildlife_sighting_rep",
            "event_details": {"species": "lion"},
            "geometry": {},
            "observation_type": "ev",
        },
    }


@pytest.fixture
def raw_event_v2_attributes():
    return {
        "observation_type": "ev",
        "gundi_version": "v2",
        "gundi_id": "5b793d17-cd79-49c8-abaa-712cb40f2b54",
    }


@pytest.fixture
def raw_event_update():
    return {
        "event_id": "04532433-79d3-4265-a201-7b920582c7e7",
        "timestamp": "2024-07-22 12:23:20.449844+00:00",
        "schema_version": "v1",
        "payload": {
            "gundi_id": "b1fd63df-9337-40bf-8097-307d986d7e94",
            "related_to": None,
            "owner": "a91b400b-482a-4546-8fcb-ee42b01deeb6",
            "data_provider_id": "f870e228-4a65-40f0-888c-41bdc1124c3c",
            "annotations": None,
            "source_id": "ac1b9cdc-a193-4515-b446-b177bcc5f342",
            "external_source_id": "camera123",
            "changes": {"event_details": {"species": "wildcat"}},
            "observation_type": "evu",
        },
        "event_type": "EventUpdateReceived",
    }


@pytest.fixture
def raw_event_update_attributes():
    return {
        "observation_type": "evu",
        "gundi_version": "v2",
        "gundi_id": "b1fd63df-9337-40bf-8097-307d986d7e94",
    }


@pytest.fixture
def raw_attachment_v2():
    return {
        "event_type": "AttachmentReceived",
        "event_id": "6fe6c88a-3b48-4294-8dbb-2dec5d911f71",
        "timestamp": "2024-07-19T19:41:31.866487Z",
        "schema_version": "v1",
        "payload": {
            "gundi_id": "7687a8d5-a89d-4ceb-be3b-5e1e3b7dc1a9",
            "related_to": "5f0b33b0-cf0d-4280-990f-0096c357b6c5",
            "owner": "na",
            "data_provider_id": "f870e228-4a65-40f0-888c-41bdc1124c3c",
            "annotations": None,
            "source_id": "None",
            "external_source_id": "None",
            "file_path": "attachments/7687a8d5-a89d-4ceb-be3b-5e1e3b7dc1a9_elephant-female.png",
            "observation_type": "att",
        },
    }


@pytest.fixture
def raw_attachment_v2_attributes():
    return {
        "observation_type": "att",
        "gundi_version": "v2",
        "gundi_id": "8b62fdd5-2e70-40e1-b202-f80c6014d596",
    }


@pytest.fixture
def leopard_detected_event_v2():
    return schemas_v2.Event(
        gundi_id="b9b46dc1-e033-447d-a99b-0fe373ca04c9",
        related_to="None",
        owner="e2d1b0fc-69fe-408b-afc5-7f54872730c0",
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        annotations={},
        source_id="afa0d606-c143-4705-955d-68133645db6d",
        external_source_id="Xyz123",
        recorded_at=datetime.datetime(2023, 7, 4, 21, 38, tzinfo=datetime.timezone.utc),
        location=schemas_v2.Location(
            lat=-51.667875, lon=-72.71195, alt=1800.0, hdop=None, vdop=None
        ),
        title="Leopard Detected",
        event_type="leopard_sighting",
        event_details={
            "site_name": "Camera2G",
            "species": "Leopard",
            "tags": ["female adult", "male child"],
            "animal_count": 2,
        },
        geometry={},
        observation_type="ev",
    )


@pytest.fixture
def leopard_detected_from_species_event_v2():
    return schemas_v2.Event(
        gundi_id="b9b46dc1-e033-447d-a99b-0fe373ca04c9",
        related_to="None",
        owner="e2d1b0fc-69fe-408b-afc5-7f54872730c0",
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        annotations={},
        source_id="afa0d606-c143-4705-955d-68133645db6d",
        external_source_id="Xyz123",
        recorded_at=datetime.datetime(2023, 7, 4, 21, 38, tzinfo=datetime.timezone.utc),
        location=schemas_v2.Location(
            lat=-51.667875, lon=-72.71195, alt=1800.0, hdop=None, vdop=None
        ),
        title="Leopard Detected",
        event_type=None,
        event_details={
            "site_name": "Camera2G",
            "species": "Leopard",
            "tags": ["female adult", "male child"],
            "animal_count": 2,
        },
        geometry={},
        observation_type="ev",
    )


@pytest.fixture
def event_v2_with_empty_event_details():
    return schemas_v2.Event(
        gundi_id="b9b46dc1-e033-447d-a99b-0fe373ca04c9",
        related_to="None",
        owner="e2d1b0fc-69fe-408b-afc5-7f54872730c0",
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        annotations={},
        source_id="afa0d606-c143-4705-955d-68133645db6d",
        external_source_id="Xyz123",
        recorded_at=datetime.datetime(2023, 7, 4, 21, 38, tzinfo=datetime.timezone.utc),
        location=schemas_v2.Location(
            lat=-51.667875, lon=-72.71195, alt=1800.0, hdop=None, vdop=None
        ),
        title="Animal Detected",
        event_type="wildlife_sighting_rep",
        event_details={},
        geometry={},
        observation_type="ev",
    )


@pytest.fixture
def event_update_species_wildcat():
    return schemas_v2.EventUpdate(
        gundi_id="78867a74-67f0-4b56-8e44-125ae66408ff",
        related_to=None,
        owner="a91b400b-482a-4546-8fcb-ee42b01deeb6",
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        annotations=None,
        source_id="ac1b9cdc-a193-4515-b446-b177bcc5f342",
        external_source_id="camera123",
        changes={"event_details": {"species": "Wildcat"}},
        observation_type="evu",
    )


@pytest.fixture
def event_update_location_lon():
    return schemas_v2.EventUpdate(
        gundi_id="78867a74-67f0-4b56-8e44-125ae66408ff",
        related_to=None,
        owner="a91b400b-482a-4546-8fcb-ee42b01deeb6",
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        annotations=None,
        source_id="ac1b9cdc-a193-4515-b446-b177bcc5f342",
        external_source_id="camera123",
        changes={"location": {"lon": 13.783061}},
        observation_type="evu",
    )


@pytest.fixture
def event_update_location_full():
    return schemas_v2.EventUpdate(
        gundi_id="78867a74-67f0-4b56-8e44-125ae66408ff",
        related_to=None,
        owner="a91b400b-482a-4546-8fcb-ee42b01deeb6",
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        annotations=None,
        source_id="ac1b9cdc-a193-4515-b446-b177bcc5f342",
        external_source_id="camera123",
        changes={"location": {"lat": 13.688632, "lon": 13.783061}},
        observation_type="evu",
    )


@pytest.fixture
def event_update_status_resolved():
    return schemas_v2.EventUpdate(
        gundi_id="78867a74-67f0-4b56-8e44-125ae66408ff",
        related_to=None,
        owner="a91b400b-482a-4546-8fcb-ee42b01deeb6",
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        annotations=None,
        source_id="ac1b9cdc-a193-4515-b446-b177bcc5f342",
        external_source_id="camera123",
        changes={"status": "resolved"},
        observation_type="evu",
    )


from smartconnect.models import (
    SMARTRequest,
    SmartAttributes,
    Geometry,
    SMARTCONNECT_DATFORMAT,
    Properties,
    DataModel,
)


@pytest.fixture
def smartrequest_with_no_attachments():
    smart_attributes = SmartAttributes()
    return SMARTRequest(
        type="Feature",
        geometry=Geometry(coordinates=[0.1, 0.1], type="Point"),
        properties=Properties(
            dateTime=datetime.datetime.now().strftime(SMARTCONNECT_DATFORMAT),
            smartDataType="",
            smartFeatureType="",
            smartAttributes=smart_attributes,
        ),
    )


@pytest.fixture
def observation_object_v2():
    return schemas_v2.Observation.parse_obj(
        {
            "external_source_id": "bc14b256-dec0-4363-831d-39d0d2d85d50",
            "data_provider_id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6",
            "source_name": "Logistics Truck A",
            "type": "tracking-device",
            "recorded_at": "2021-03-27 11:15:00+0200",
            "location": {"lon": 35.43902, "lat": -1.59083},
            "additional": {
                "voltage": "7.4",
                "fuel_level": 71,
                "speed": "41 kph",
            },
        }
    )


@pytest.fixture
def unmapped_animal_detected_event_v2():
    return schemas_v2.Event(
        gundi_id="b9b46dc1-e033-447d-a99b-0fe373ca04c9",
        related_to="None",
        owner="e2d1b0fc-69fe-408b-afc5-7f54872730c0",
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        annotations={},
        source_id="afa0d606-c143-4705-955d-68133645db6d",
        external_source_id="Xyz123",
        recorded_at=datetime.datetime(2023, 7, 4, 21, 38, tzinfo=datetime.timezone.utc),
        location=schemas_v2.Location(
            lat=-51.667875, lon=-72.71195, alt=1800.0, hdop=None, vdop=None
        ),
        title="Animal Detected",
        event_type=None,
        event_details={
            "site_name": "Camera2G",
            "species": "Unknown",
            "tags": ["female adult", "male child"],
            "animal_count": 2,
        },
        geometry={},
        observation_type="ev",
    )


@pytest.fixture
def animals_sign_event_v2():
    return schemas_v2.Event(
        gundi_id="c1b46dc1-b144-556c-c87a-2ef373ca04b0",
        owner="e2d1b0fc-69fe-408b-afc5-7f54872730c0",
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        annotations={},
        source_id="afa0d606-c143-4705-955d-68133645db6d",
        external_source_id="Xyz123",
        recorded_at=datetime.datetime(
            2023, 12, 28, 19, 26, tzinfo=datetime.timezone.utc
        ),
        location=schemas_v2.Location(
            lat=-51.688645, lon=-72.704440, alt=1800.0, hdop=None, vdop=None
        ),
        title="Animal Sign",
        event_type="animals_sign",
        event_details={
            "species": "lion",
            "ageofsign": "days",
        },
        geometry={},
        observation_type="ev",
    )


@pytest.fixture
def animals_sign_event_with_status():
    return schemas_v2.Event(
        gundi_id="c1b46dc1-b144-556c-c87a-2ef373ca04b0",
        owner="e2d1b0fc-69fe-408b-afc5-7f54872730c0",
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        annotations={},
        source_id="afa0d606-c143-4705-955d-68133645db6d",
        external_source_id="Xyz123",
        recorded_at=datetime.datetime(
            2023, 12, 28, 19, 26, tzinfo=datetime.timezone.utc
        ),
        location=schemas_v2.Location(
            lat=-51.688645, lon=-72.704440, alt=1800.0, hdop=None, vdop=None
        ),
        title="Animal Sign",
        event_type="animals_sign",
        event_details={
            "species": "lion",
            "ageofsign": "days",
        },
        geometry={},
        status="active",
        observation_type="ev",
    )


@pytest.fixture
def photo_attachment_v2_jpg():
    return schemas_v2.Attachment(
        gundi_id="9bedc03e-8415-46db-aa70-782490cdff31",
        related_to="b9b46dc1-e033-447d-a99b-0fe373ca04c9",
        owner="a91b400b-482a-4546-8fcb-ee42b01deeb6",
        data_provider_id="d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
        source_id="ea2d5fca-752a-4a44-b170-668d780db85e",
        external_source_id="gunditest",
        file_path="attachments/9bedc03e-8415-46db-aa70-782490cdff31_elephant.jpg",
        observation_type="att",
    )


@pytest.fixture
def photo_attachment_v2_svg():
    return schemas_v2.Attachment(
        gundi_id="9bedc03e-8415-46db-aa70-782490cdff31",
        related_to="b9b46dc1-e033-447d-a99b-0fe373ca04c9",
        owner="a91b400b-482a-4546-8fcb-ee42b01deeb6",
        data_provider_id="d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
        source_id="ea2d5fca-752a-4a44-b170-668d780db85e",
        external_source_id="gunditest",
        file_path="attachments/1aedc03e-8415-46db-aa70-782490cdff31_elephant.svg",
        observation_type="att",
    )


@pytest.fixture
def photo_attachment_v2_png():
    return schemas_v2.Attachment(
        gundi_id="9bedc03e-8415-46db-aa70-782490cdff31",
        related_to="b9b46dc1-e033-447d-a99b-0fe373ca04c9",
        owner="a91b400b-482a-4546-8fcb-ee42b01deeb6",
        data_provider_id="d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
        source_id="ea2d5fca-752a-4a44-b170-668d780db85e",
        external_source_id="gunditest",
        file_path="attachments/1aedc03e-8415-46db-aa70-782490cdff31_elephant.png",
        observation_type="att",
    )


@pytest.fixture
def photo_attachment_v2_gif():
    return schemas_v2.Attachment(
        gundi_id="9bedc03e-8415-46db-aa70-782490cdff31",
        related_to="b9b46dc1-e033-447d-a99b-0fe373ca04c9",
        owner="a91b400b-482a-4546-8fcb-ee42b01deeb6",
        data_provider_id="d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
        source_id="ea2d5fca-752a-4a44-b170-668d780db85e",
        external_source_id="gunditest",
        file_path="attachments/1aedc03e-8415-46db-aa70-782490cdff31_elephant.gif",
        observation_type="att",
    )


@pytest.fixture
def photo_attachment_v2_bmp():
    return schemas_v2.Attachment(
        gundi_id="9bedc03e-8415-46db-aa70-782490cdff31",
        related_to="b9b46dc1-e033-447d-a99b-0fe373ca04c9",
        owner="a91b400b-482a-4546-8fcb-ee42b01deeb6",
        data_provider_id="d88ac520-2bf6-4e6b-ab09-38ed1ec6947a",
        source_id="ea2d5fca-752a-4a44-b170-668d780db85e",
        external_source_id="gunditest",
        file_path="attachments/1aedc03e-8415-46db-aa70-782490cdff31_elephant.bmp",
        observation_type="att",
    )


@pytest.fixture
def smart_event_v2_with_unmatched_category():
    return schemas_v2.Event(
        gundi_id="c1b46dc1-b144-556c-c87a-2ef373ca04b0",
        owner="e2d1b0fc-69fe-408b-afc5-7f54872730c0",
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        annotations={},
        source_id="afa0d606-c143-4705-955d-68133645db6d",
        external_source_id="Xyz123",
        recorded_at=datetime.datetime(
            2023, 12, 28, 19, 26, tzinfo=datetime.timezone.utc
        ),
        location=schemas_v2.Location(
            lat=-51.688645, lon=-72.704440, alt=1800.0, hdop=None, vdop=None
        ),
        title="Test Event",
        event_type="bad_event_type",
        event_details={
            "species": "lion",
            "ageofsign": "days",
        },
        geometry={},
        observation_type="ev",
    )


@pytest.fixture
def animals_sign_event_update_v2():
    return schemas_v2.EventUpdate(
        gundi_id="c1b46dc1-b144-556c-c87a-2ef373ca04b0",
        related_to=None,
        owner="e2d1b0fc-69fe-408b-afc5-7f54872730c0",
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        annotations={},
        source_id="afa0d606-c143-4705-955d-68133645db6d",
        external_source_id="Xyz123",
        changes={
            "title": "Puma Sign",
            "recorded_at": "2024-08-05 13:27:10+00:00",
            "location": {"lat": 13.123456, "lon": 13.123456},
            "event_type": "animals_sign",  # Event type and details must be changed together in SMART
            "event_details": {
                "species": "puma",
                "ageofsign": "weeks",
            },
            "status": "resolved",
        },
        observation_type="evu",
    )


@pytest.fixture
def animals_sign_event_update_title_v2():
    return schemas_v2.EventUpdate(
        gundi_id="c1b46dc1-b144-556c-c87a-2ef373ca04b0",
        related_to=None,
        owner="e2d1b0fc-69fe-408b-afc5-7f54872730c0",
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        annotations={},
        source_id="afa0d606-c143-4705-955d-68133645db6d",
        external_source_id="Xyz123",
        changes={"title": "Leopard Sign"},
        observation_type="evu",
    )


@pytest.fixture
def animals_sign_event_update_location_v2():
    return schemas_v2.EventUpdate(
        gundi_id="c1b46dc1-b144-556c-c87a-2ef373ca04b0",
        related_to=None,
        owner="e2d1b0fc-69fe-408b-afc5-7f54872730c0",
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        annotations={},
        source_id="afa0d606-c143-4705-955d-68133645db6d",
        external_source_id="Xyz123",
        changes={"location": {"lat": 13.123457, "lon": 13.123457}},
        observation_type="evu",
    )


@pytest.fixture
def animals_sign_event_update_details_v2():
    return schemas_v2.EventUpdate(
        gundi_id="c1b46dc1-b144-556c-c87a-2ef373ca04b0",
        related_to=None,
        owner="e2d1b0fc-69fe-408b-afc5-7f54872730c0",
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        annotations={},
        source_id="afa0d606-c143-4705-955d-68133645db6d",
        external_source_id="Xyz123",
        changes={
            "event_type": "animals_sign",  # Event type and details must be changed together in SMART
            "event_details": {"species": "leopard"},
        },
        observation_type="evu",
    )


@pytest.fixture
def animals_sign_event_update_details_without_event_type_v2():
    return schemas_v2.EventUpdate(
        gundi_id="c1b46dc1-b144-556c-c87a-2ef373ca04b0",
        related_to=None,
        owner="e2d1b0fc-69fe-408b-afc5-7f54872730c0",
        data_provider_id="ddd0946d-15b0-4308-b93d-e0470b6d33b6",
        annotations={},
        source_id="afa0d606-c143-4705-955d-68133645db6d",
        external_source_id="Xyz123",
        changes={
            # Event type intentionally left out
            "event_details": {"species": "leopard"},
        },
        observation_type="evu",
    )


@pytest.fixture
def text_message_from_inreach():
    return schemas_v2.TextMessage(
        gundi_id="d2b46dc1-b144-556c-c87a-2ef373ca04c3",
        related_to=None,
        owner="e2d1b0fc-69fe-408b-afc5-7f54872730c0",
        data_provider_id="abc0946d-15b0-4308-b93d-e0470b6d33b6",
        source_id="abc12345-6789-0123-4567-89abcdef0123",
        sender="2075752244",
        recipients=["2185852245"],
        text="Assistance needed, please respond.",
        created_at="2025-06-04T13:27:10+03:00",
        location=schemas_v2.Location(
            lon=-72.704459,
            lat=-51.688246,
        ),
        additional={
            "status": {
                "autonomous": 0,
                "lowBattery": 1,
                "intervalChange": 0,
                "resetDetected": 0,
            }
        },
        observation_type=schemas_v2.StreamPrefixEnum.text_message.value,
    )


@pytest.fixture
def text_message_from_earthranger():
    return schemas_v2.TextMessage(
        gundi_id="c1b46dc1-b144-556c-c87a-2ef373ca04b2",
        related_to=None,
        owner="e2d1b0fc-69fe-408b-afc5-7f54872730c0",
        data_provider_id="338225f3-91f9-4fe1-b013-353a229ce504",
        source_id="abc12345-6789-0123-4567-89abcdef0123",
        sender="admin@sitex.pamdas.org",
        recipients=["2075752244"],
        text="Assistance is on the way.",
        created_at="2025-06-04T13:35:10+03:00",
        observation_type=schemas_v2.StreamPrefixEnum.text_message.value,
    )


@pytest.fixture
def integration_type_er():
    return schemas_v2.ConnectionIntegrationType(
        id="45c66a61-71e4-4664-a7f2-30d465f87aa6",
        name="EarthRanger",
        value="earth_ranger",
    )


@pytest.fixture
def integration_type_movebank():
    return schemas_v2.ConnectionIntegrationType(
        id="45c66a61-71e4-4664-a7f2-30d465f87bb7", name="Movebank", value="movebank"
    )


@pytest.fixture
def destination_integration_v2_er(integration_type_er):
    return schemas_v2.ConnectionIntegration(
        id="338225f3-91f9-4fe1-b013-353a229ce504",
        name="ER Load Testing",
        type=integration_type_er,
        base_url="https://gundi-load-testing.pamdas.org",
        status="healthy",
        status_details="",
    )


@pytest.fixture
def destination_integration_v1_smartconnect():
    return schemas_v1.OutboundConfiguration(
        id="338225f3-91f9-4fe1-b013-353a229ce504",
        name="SmartConnect Server",
        type=uuid.uuid4(),
        endpoint="https://smartconnect.example.com",
        username="something",
        password="something fancy",
        type_slug="smart_connect",
        owner=uuid.uuid4(),
    )


@pytest.fixture
def destination_integration_v2_movebank(integration_type_movebank):
    return schemas_v2.ConnectionIntegration(
        id="338225f3-91f9-4fe1-b013-353a229ce504",
        name="MB Load Testing",
        type=integration_type_movebank,
        base_url="https://mb-load-testing.pamdas.org",
        status="healthy",
        status_details="",
    )


@pytest.fixture
def integration_type_smart():
    return schemas_v2.ConnectionIntegrationType(
        id="56b76a61-53e4-4664-a7f2-21d465f87bc6",
        name="SMART Connect",
        value="smart_connect",
    )


@pytest.fixture
def smart_ca_uuid():
    return "a13b9201-6228-45e0-a75b-abe5b6a9f98e"


@pytest.fixture
def destination_integration_v2_smart(integration_type_smart, smart_ca_uuid):
    return schemas_v2.Integration(
        id=UUID("b42c9205-5228-49e0-a75b-ebe5b6a9f78e"),
        name="Integration X SMART Connect",
        type=schemas_v2.IntegrationType(
            id=integration_type_smart.id,
            name=integration_type_smart.name,
            value=integration_type_smart.value,
            description="",
            actions=[
                schemas_v2.IntegrationAction(
                    id=UUID("b0a0e7ed-d668-41b5-96d2-397f026c4ecb"),
                    type="auth",
                    name="Authenticate",
                    value="auth",
                    description="Authenticate against smart",
                    action_schema={
                        "type": "object",
                        "title": "SMARTAuthActionConfig",
                        "required": ["login", "password"],
                        "properties": {
                            "login": {"type": "string", "title": "Login"},
                            "endpoint": {"type": "string", "title": "Endpoint"},
                            "password": {"type": "string", "title": "Password"},
                        },
                    },
                ),
                schemas_v2.IntegrationAction(
                    id=UUID("ebe72917-c112-4064-9f38-707bbd14a50f"),
                    type="push",
                    name="Push Events",
                    value="push_events",
                    description="Send Events to SMART Connect (a.k.a Incidents or waypoints)",
                    action_schema={
                        "type": "object",
                        "title": "SMARTPushEventActionConfig",
                        "properties": {
                            "ca_uuid": {
                                "type": "string",
                                "title": "Ca Uuid",
                                "format": "uuid",
                            },
                            "version": {"type": "string", "title": "Version"},
                            "ca_uuids": {
                                "type": "array",
                                "items": {"type": "string", "format": "uuid"},
                                "title": "Ca Uuids",
                            },
                            "transformation_rules": {
                                "$ref": "#/definitions/TransformationRules"
                            },
                            "configurable_models_lists": {
                                "type": "object",
                                "title": "Configurable Models Lists",
                            },
                            "configurable_models_enabled": {
                                "type": "array",
                                "items": {"type": "string", "format": "uuid"},
                                "title": "Configurable Models Enabled",
                            },
                        },
                        "definitions": {
                            "OptionMap": {
                                "type": "object",
                                "title": "OptionMap",
                                "required": ["from_key", "to_key"],
                                "properties": {
                                    "to_key": {"type": "string", "title": "To Key"},
                                    "from_key": {"type": "string", "title": "From Key"},
                                },
                            },
                            "CategoryPair": {
                                "type": "object",
                                "title": "CategoryPair",
                                "required": ["event_type", "category_path"],
                                "properties": {
                                    "event_type": {
                                        "type": "string",
                                        "title": "Event Type",
                                    },
                                    "category_path": {
                                        "type": "string",
                                        "title": "Category Path",
                                    },
                                },
                            },
                            "AttributeMapper": {
                                "type": "object",
                                "title": "AttributeMapper",
                                "required": ["from_key", "to_key"],
                                "properties": {
                                    "type": {
                                        "type": "string",
                                        "title": "Type",
                                        "default": "string",
                                    },
                                    "to_key": {"type": "string", "title": "To Key"},
                                    "from_key": {"type": "string", "title": "From Key"},
                                    "event_types": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "title": "Event Types",
                                    },
                                    "options_map": {
                                        "type": "array",
                                        "items": {"$ref": "#/definitions/OptionMap"},
                                        "title": "Options Map",
                                    },
                                    "default_option": {
                                        "type": "string",
                                        "title": "Default Option",
                                    },
                                },
                            },
                            "TransformationRules": {
                                "type": "object",
                                "title": "TransformationRules",
                                "properties": {
                                    "category_map": {
                                        "type": "array",
                                        "items": {"$ref": "#/definitions/CategoryPair"},
                                        "title": "Category Map",
                                        "default": [],
                                    },
                                    "attribute_map": {
                                        "type": "array",
                                        "items": {
                                            "$ref": "#/definitions/AttributeMapper"
                                        },
                                        "title": "Attribute Map",
                                        "default": [],
                                    },
                                },
                            },
                        },
                    },
                ),
            ],
        ),
        base_url="https://integrationx.smartconservationtools.org/server",
        enabled=True,
        owner=schemas_v2.Organization(
            id=UUID("a91b400b-482a-4546-8fcb-ee42b01deeb6"),
            name="Test Org",
            description="",
        ),
        configurations=[
            schemas_v2.IntegrationActionConfiguration(
                id=UUID("55760315-3d68-4925-bf2d-c4d39de433c9"),
                integration=UUID("b42c9205-5228-49e0-a75b-ebe5b6a9f78e"),
                action=schemas_v2.IntegrationActionSummary(
                    id=UUID("ebe72917-c112-4064-9f38-707bbd14a50f"),
                    type="push",
                    name="Push Events",
                    value="push_events",
                ),
                data={
                    "version": "7.5.3",
                    "ca_uuids": [smart_ca_uuid],
                    "transformation_rules": {"category_map": [], "attribute_map": []},
                },
            ),
            schemas_v2.IntegrationActionConfiguration(
                id=UUID("abce8c67-a74c-46fd-b5c3-62a7b76f2b17"),
                integration=UUID("b42c9205-5228-49e0-a75b-ebe5b6a9f78e"),
                action=schemas_v2.IntegrationActionSummary(
                    id=UUID("b0a0e7ed-d668-41b5-96d2-397f026c4ecb"),
                    type="auth",
                    name="Authenticate",
                    value="auth",
                ),
                data={
                    "login": "fakeusername",
                    "endpoint": "https://integrationx.smartconservationtools.org/server",
                    "password": "something fancy",
                },
            ),
        ],
        default_route=None,
        additional={},
        status="healthy",
        status_details="",
    )


@pytest.fixture
def destination_integration_v2_smart_without_transform_rules(
    integration_type_smart, smart_ca_uuid
):
    return schemas_v2.Integration(
        id=UUID("b42c9205-5228-49e0-a75b-ebe5b6a9f78e"),
        name="Integration X SMART Connect",
        type=schemas_v2.IntegrationType(
            id=integration_type_smart.id,
            name=integration_type_smart.name,
            value=integration_type_smart.value,
            description="",
            actions=[
                schemas_v2.IntegrationAction(
                    id=UUID("b0a0e7ed-d668-41b5-96d2-397f026c4ecb"),
                    type="auth",
                    name="Authenticate",
                    value="auth",
                    description="Authenticate against smart",
                    action_schema={
                        "type": "object",
                        "title": "SMARTAuthActionConfig",
                        "required": ["login", "password"],
                        "properties": {
                            "login": {"type": "string", "title": "Login"},
                            "endpoint": {"type": "string", "title": "Endpoint"},
                            "password": {"type": "string", "title": "Password"},
                        },
                    },
                ),
                schemas_v2.IntegrationAction(
                    id=UUID("ebe72917-c112-4064-9f38-707bbd14a50f"),
                    type="push",
                    name="Push Events",
                    value="push_events",
                    description="Send Events to SMART Connect (a.k.a Incidents or waypoints)",
                    action_schema={
                        "type": "object",
                        "title": "SMARTPushEventActionConfig",
                        "properties": {
                            "ca_uuid": {
                                "type": "string",
                                "title": "Ca Uuid",
                                "format": "uuid",
                            },
                            "version": {"type": "string", "title": "Version"},
                            "ca_uuids": {
                                "type": "array",
                                "items": {"type": "string", "format": "uuid"},
                                "title": "Ca Uuids",
                            },
                            "transformation_rules": {
                                "$ref": "#/definitions/TransformationRules"
                            },
                            "configurable_models_lists": {
                                "type": "object",
                                "title": "Configurable Models Lists",
                            },
                            "configurable_models_enabled": {
                                "type": "array",
                                "items": {"type": "string", "format": "uuid"},
                                "title": "Configurable Models Enabled",
                            },
                        },
                        "definitions": {
                            "OptionMap": {
                                "type": "object",
                                "title": "OptionMap",
                                "required": ["from_key", "to_key"],
                                "properties": {
                                    "to_key": {"type": "string", "title": "To Key"},
                                    "from_key": {"type": "string", "title": "From Key"},
                                },
                            },
                            "CategoryPair": {
                                "type": "object",
                                "title": "CategoryPair",
                                "required": ["event_type", "category_path"],
                                "properties": {
                                    "event_type": {
                                        "type": "string",
                                        "title": "Event Type",
                                    },
                                    "category_path": {
                                        "type": "string",
                                        "title": "Category Path",
                                    },
                                },
                            },
                            "AttributeMapper": {
                                "type": "object",
                                "title": "AttributeMapper",
                                "required": ["from_key", "to_key"],
                                "properties": {
                                    "type": {
                                        "type": "string",
                                        "title": "Type",
                                        "default": "string",
                                    },
                                    "to_key": {"type": "string", "title": "To Key"},
                                    "from_key": {"type": "string", "title": "From Key"},
                                    "event_types": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "title": "Event Types",
                                    },
                                    "options_map": {
                                        "type": "array",
                                        "items": {"$ref": "#/definitions/OptionMap"},
                                        "title": "Options Map",
                                    },
                                    "default_option": {
                                        "type": "string",
                                        "title": "Default Option",
                                    },
                                },
                            },
                            "TransformationRules": {
                                "type": "object",
                                "title": "TransformationRules",
                                "properties": {
                                    "category_map": {
                                        "type": "array",
                                        "items": {"$ref": "#/definitions/CategoryPair"},
                                        "title": "Category Map",
                                        "default": [],
                                    },
                                    "attribute_map": {
                                        "type": "array",
                                        "items": {
                                            "$ref": "#/definitions/AttributeMapper"
                                        },
                                        "title": "Attribute Map",
                                        "default": [],
                                    },
                                },
                            },
                        },
                    },
                ),
            ],
        ),
        base_url="https://integrationx.smartconservationtools.org/server",
        enabled=True,
        owner=schemas_v2.Organization(
            id=UUID("a91b400b-482a-4546-8fcb-ee42b01deeb6"),
            name="Test Org",
            description="",
        ),
        configurations=[
            schemas_v2.IntegrationActionConfiguration(
                id=UUID("55760315-3d68-4925-bf2d-c4d39de433c9"),
                integration=UUID("b42c9205-5228-49e0-a75b-ebe5b6a9f78e"),
                action=schemas_v2.IntegrationActionSummary(
                    id=UUID("ebe72917-c112-4064-9f38-707bbd14a50f"),
                    type="push",
                    name="Push Events",
                    value="push_events",
                ),
                data={
                    "version": "7.5.3",
                    "ca_uuids": [smart_ca_uuid],
                    # No transformation rules set in configuration
                    # "transformation_rules": {"category_map": [], "attribute_map": []},
                },
            ),
            schemas_v2.IntegrationActionConfiguration(
                id=UUID("abce8c67-a74c-46fd-b5c3-62a7b76f2b17"),
                integration=UUID("b42c9205-5228-49e0-a75b-ebe5b6a9f78e"),
                action=schemas_v2.IntegrationActionSummary(
                    id=UUID("b0a0e7ed-d668-41b5-96d2-397f026c4ecb"),
                    type="auth",
                    name="Authenticate",
                    value="auth",
                ),
                data={
                    "login": "fakeusername",
                    "endpoint": "https://integrationx.smartconservationtools.org/server",
                    "password": "something fancy",
                },
            ),
        ],
        default_route=None,
        additional={},
        status="healthy",
        status_details="",
    )


@pytest.fixture
def route_config_with_event_type_mappings():
    return schemas_v2.RouteConfiguration(
        id="1a3e3e73-94ad-42cb-a765-09a7193ae0b1",
        name="Trap Tagger to ER - Event Type Mapping",
        data={
            "field_mappings": {
                "ddd0946d-15b0-4308-b93d-e0470b6d33b6": {
                    "ev": {
                        "338225f3-91f9-4fe1-b013-353a229ce504": {
                            "map": {
                                "Leopard": "leopard_sighting",
                                "Wilddog": "wild_dog_sighting",
                                "Wildcat": "wild_cat_sighting",
                            },
                            "default": "wildlife_sighting_rep",
                            "provider_field": "event_details__species",
                            "destination_field": "event_type",
                        }
                    },
                    "evu": {
                        "338225f3-91f9-4fe1-b013-353a229ce504": {
                            "map": {
                                "Leopard": "leopard_sighting",
                                "Wilddog": "wild_dog_sighting",
                                "Wildcat": "wild_cat_sighting",
                            },
                            "default": "wildlife_sighting_rep",
                            "provider_field": "event_details__species",
                            "destination_field": "event_type",
                        }
                    },
                }
            }
        },
    )


@pytest.fixture
def route_config_with_provider_key_mappings():
    return schemas_v2.RouteConfiguration(
        id="1a3e3e73-94ad-42cb-a765-09a7193ae0b1",
        name="Trap Tagger to ER - Provider Key",
        data={
            "field_mappings": {
                "ddd0946d-15b0-4308-b93d-e0470b6d33b6": {
                    "ev": {
                        "338225f3-91f9-4fe1-b013-353a229ce504": {
                            "default": "mapipedia",
                            "destination_field": "provider_key",
                        }
                    },
                    "obv": {
                        "338225f3-91f9-4fe1-b013-353a229ce504": {
                            "default": "mapipedia",
                            "destination_field": "provider_key",
                        }
                    },
                }
            }
        },
    )


@pytest.fixture
def route_config_with_no_mappings():
    return schemas_v2.RouteConfiguration(
        id="1a3e3e73-94ad-42cb-a765-09a7193ae0b1",
        name="Trap Tagger to ER - No Mapping",
        data={"field_mappings": {}},
    )


@pytest.fixture
def geoevent_v1_request_payload():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return {
        "message": {
            "attributes": {
                "observation_type": "ge",
                "tracing_context": "{}",
            },
            "data": "eyJpZCI6IG51bGwsICJvd25lciI6ICJuYSIsICJpbnRlZ3JhdGlvbl9pZCI6ICJjMjVhNTNmNi1lMjA2LTQzZGQtOGY2Mi1hMzc1OTU2NWFlM2QiLCAiZGV2aWNlX2lkIjogIm5vbmUiLCAicmVjb3JkZWRfYXQiOiAiMjAyNC0wMi0yMCAyMDozNDowOC0wMzowMCIsICJsb2NhdGlvbiI6IHsieCI6IC01MS42ODg2NzUsICJ5IjogLTcyLjcwNDQ2NSwgInoiOiAwLjAsICJoZG9wIjogbnVsbCwgInZkb3AiOiBudWxsfSwgImFkZGl0aW9uYWwiOiBudWxsLCAidGl0bGUiOiAiUG9hY2hlcnMgQWN0aXZpdHkiLCAiZXZlbnRfdHlwZSI6ICJodW1hbmFjdGl2aXR5X3BvYWNoaW5nIiwgImV2ZW50X2RldGFpbHMiOiB7fSwgImdlb21ldHJ5IjogbnVsbCwgIm9ic2VydmF0aW9uX3R5cGUiOiAiZ2UifQ==",  # pragma: allowlist secret
            # pragma: allowlist secret
            "messageId": "8255786613739821",
            "message_id": "8255786613739821",
            "publishTime": timestamp,
            "publish_time": timestamp,
        },
        "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB",  # pragma: allowlist secret
    }


@pytest.fixture
def geoevent_v1_eventarc_request_payload():
    return {
        "message": {
            "attributes": {
                "observation_type": "ge",
                "tracing_context": "{}",
            },
            "data": "eyJpZCI6IG51bGwsICJvd25lciI6ICJuYSIsICJpbnRlZ3JhdGlvbl9pZCI6ICJjMjVhNTNmNi1lMjA2LTQzZGQtOGY2Mi1hMzc1OTU2NWFlM2QiLCAiZGV2aWNlX2lkIjogIm5vbmUiLCAicmVjb3JkZWRfYXQiOiAiMjAyNC0wMi0yMCAyMDozNDowOC0wMzowMCIsICJsb2NhdGlvbiI6IHsieCI6IC01MS42ODg2NzUsICJ5IjogLTcyLjcwNDQ2NSwgInoiOiAwLjAsICJoZG9wIjogbnVsbCwgInZkb3AiOiBudWxsfSwgImFkZGl0aW9uYWwiOiBudWxsLCAidGl0bGUiOiAiUG9hY2hlcnMgQWN0aXZpdHkiLCAiZXZlbnRfdHlwZSI6ICJodW1hbmFjdGl2aXR5X3BvYWNoaW5nIiwgImV2ZW50X2RldGFpbHMiOiB7fSwgImdlb21ldHJ5IjogbnVsbCwgIm9ic2VydmF0aW9uX3R5cGUiOiAiZ2UifQ==",  # pragma: allowlist secret
            "messageId": "8255786613739821",
            "message_id": "8255786613739821",
        },
        "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB",  # pragma: allowlist secret
    }


@pytest.fixture
def event_v2_request_payload(animals_sign_event_v2):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return {
        "message": {
            "attributes": {
                "gundi_id": "457c0bfd-e208-4d65-bb9e-a7280822cb42",
                "gundi_version": "v2",
                "observation_type": "ev",
                "tracing_context": "{}",
            },
            "data": "eyJldmVudF9pZCI6ICI0Y2M5NjVmZS04ZGQxLTQxZjEtODc1ZC0wYTdmNWIyMjI5ZDMiLCAidGltZXN0YW1wIjogIjIwMjQtMDgtMTIgMTI6Mjk6MjkuNDc2NDA1KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Imd1bmRpX2lkIjogIjQ1N2MwYmZkLWUyMDgtNGQ2NS1iYjllLWE3MjgwODIyY2I0MiIsICJvd25lciI6ICI0NTAxODM5OC03YTJhLTRmNDgtODk3MS0zOWEyNzEwZDVkYmQiLCAiZGF0YV9wcm92aWRlcl9pZCI6ICI3Y2M4MGU4NS01YTk3LTRmYjQtYWE5Yy0zMTVhNjg0YjU2NDYiLCAiYW5ub3RhdGlvbnMiOiB7fSwgInNvdXJjZV9pZCI6ICIzYjNjMjk5My04MjAxLTQ0MzQtOTMzNy0yMzBiODkxN2Y2NTIiLCAiZXh0ZXJuYWxfc291cmNlX2lkIjogImRlZmF1bHQtc291cmNlIiwgInJlY29yZGVkX2F0IjogIjIwMjQtMDgtMTIgMTI6Mjk6MTArMDA6MDAiLCAibG9jYXRpb24iOiB7ImxhdCI6IDEzLjY4ODYzMiwgImxvbiI6IDEzLjc4MzA2OCwgImFsdCI6IDAuMH0sICJ0aXRsZSI6ICJBbmltYWxzIERldGVjdGVkIiwgImV2ZW50X3R5cGUiOiAiYW5pbWFscyIsICJldmVudF9kZXRhaWxzIjogeyJ0YXJnZXRzcGVjaWVzIjogInJlcHRpbGVzLnB5dGhvbnNwcCIsICJ3aWxkbGlmZW9ic2VydmF0aW9udHlwZSI6ICJkaXJlY3RvYnNlcnZhdGlvbiIsICJhZ2VvZnNpZ25hbmltYWwiOiAiZnJlc2giLCAibnVtYmVyb2ZhbmltYWwiOiAyfSwgIm9ic2VydmF0aW9uX3R5cGUiOiAiZXYifSwgImV2ZW50X3R5cGUiOiAiRXZlbnRSZWNlaXZlZCJ9",  # pragma: allowlist secret
            "messageId": "11960027960451651",
            "message_id": "11960027960451651",
            "publishTime": timestamp,
            "publish_time": timestamp,
        },
        "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB",  # pragma: allowlist secret
    }


@pytest.fixture
def event_v2_eventarc_request_payload(animals_sign_event_v2):
    return {
        "message": {
            "attributes": {
                "gundi_id": "457c0bfd-e208-4d65-bb9e-a7280822cb42",
                "gundi_version": "v2",
                "observation_type": "ev",
                "tracing_context": "{}",
            },
            "data": "eyJldmVudF9pZCI6ICI0Y2M5NjVmZS04ZGQxLTQxZjEtODc1ZC0wYTdmNWIyMjI5ZDMiLCAidGltZXN0YW1wIjogIjIwMjQtMDgtMTIgMTI6Mjk6MjkuNDc2NDA1KzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Imd1bmRpX2lkIjogIjQ1N2MwYmZkLWUyMDgtNGQ2NS1iYjllLWE3MjgwODIyY2I0MiIsICJvd25lciI6ICI0NTAxODM5OC03YTJhLTRmNDgtODk3MS0zOWEyNzEwZDVkYmQiLCAiZGF0YV9wcm92aWRlcl9pZCI6ICI3Y2M4MGU4NS01YTk3LTRmYjQtYWE5Yy0zMTVhNjg0YjU2NDYiLCAiYW5ub3RhdGlvbnMiOiB7fSwgInNvdXJjZV9pZCI6ICIzYjNjMjk5My04MjAxLTQ0MzQtOTMzNy0yMzBiODkxN2Y2NTIiLCAiZXh0ZXJuYWxfc291cmNlX2lkIjogImRlZmF1bHQtc291cmNlIiwgInJlY29yZGVkX2F0IjogIjIwMjQtMDgtMTIgMTI6Mjk6MTArMDA6MDAiLCAibG9jYXRpb24iOiB7ImxhdCI6IDEzLjY4ODYzMiwgImxvbiI6IDEzLjc4MzA2OCwgImFsdCI6IDAuMH0sICJ0aXRsZSI6ICJBbmltYWxzIERldGVjdGVkIiwgImV2ZW50X3R5cGUiOiAiYW5pbWFscyIsICJldmVudF9kZXRhaWxzIjogeyJ0YXJnZXRzcGVjaWVzIjogInJlcHRpbGVzLnB5dGhvbnNwcCIsICJ3aWxkbGlmZW9ic2VydmF0aW9udHlwZSI6ICJkaXJlY3RvYnNlcnZhdGlvbiIsICJhZ2VvZnNpZ25hbmltYWwiOiAiZnJlc2giLCAibnVtYmVyb2ZhbmltYWwiOiAyfSwgIm9ic2VydmF0aW9uX3R5cGUiOiAiZXYifSwgImV2ZW50X3R5cGUiOiAiRXZlbnRSZWNlaXZlZCJ9",  # pragma: allowlist secret
            "messageId": "11960027960451651",
            "message_id": "11960027960451651",
        },
        "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB",  # pragma: allowlist secret
    }


@pytest.fixture
def event_update_v2_request_payload():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return {
        "message": {
            "attributes": {
                "gundi_id": "457c0bfd-e208-4d65-bb9e-a7280822cb42",
                "gundi_version": "v2",
                "observation_type": "evu",
                "tracing_context": "{}",
            },
            "data": "eyJldmVudF9pZCI6ICI1ZDU4NjQ5ZC00YWQ1LTQ2YmUtOWEwZS1mMGJmZmYxMWMwMjQiLCAidGltZXN0YW1wIjogIjIwMjQtMDgtMTIgMTI6NDg6MDcuMzM2NzIwKzAwOjAwIiwgInNjaGVtYV92ZXJzaW9uIjogInYxIiwgInBheWxvYWQiOiB7Imd1bmRpX2lkIjogIjQ1N2MwYmZkLWUyMDgtNGQ2NS1iYjllLWE3MjgwODIyY2I0MiIsICJyZWxhdGVkX3RvIjogIk5vbmUiLCAib3duZXIiOiAiNDUwMTgzOTgtN2EyYS00ZjQ4LTg5NzEtMzlhMjcxMGQ1ZGJkIiwgImRhdGFfcHJvdmlkZXJfaWQiOiAiN2NjODBlODUtNWE5Ny00ZmI0LWFhOWMtMzE1YTY4NGI1NjQ2IiwgInNvdXJjZV9pZCI6ICIzYjNjMjk5My04MjAxLTQ0MzQtOTMzNy0yMzBiODkxN2Y2NTIiLCAiZXh0ZXJuYWxfc291cmNlX2lkIjogImRlZmF1bHQtc291cmNlIiwgImNoYW5nZXMiOiB7InRpdGxlIjogIlNuZWFrcyBkZXRlY3RlZCIsICJyZWNvcmRlZF9hdCI6ICIyMDI0LTA4LTEyIDEyOjEyOjEwKzAwOjAwIiwgImxvY2F0aW9uIjogeyJsYXQiOiAxMy42ODg2MzUsICJsb24iOiAxMy43ODMwNjh9LCAiZXZlbnRfdHlwZSI6ICJhbmltYWxzIiwgImV2ZW50X2RldGFpbHMiOiB7InRhcmdldHNwZWNpZXMiOiAicmVwdGlsZXMucHl0aG9uc3BwIiwgIndpbGRsaWZlb2JzZXJ2YXRpb250eXBlIjogImRpcmVjdG9ic2VydmF0aW9uIiwgImFnZW9mc2lnbmFuaW1hbCI6ICJmcmVzaCIsICJudW1iZXJvZmFuaW1hbCI6IDJ9fSwgIm9ic2VydmF0aW9uX3R5cGUiOiAiZXZ1In0sICJldmVudF90eXBlIjogIkV2ZW50VXBkYXRlUmVjZWl2ZWQifQ==",  # pragma: allowlist secret
            "messageId": "11960897894856249",
            "message_id": "11960897894856249",
            "orderingKey": "457c0bfd-e208-4d65-bb9e-a7280822cb42",
            "publishTime": timestamp,
            "publish_time": timestamp,
        },
        "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB",  # pragma: allowlist secret
    }


@pytest.fixture
def pubsub_request_headers():
    return {
        "host": "routing-transformer-service-dev-jba4og2dyq-uc.a.run.app",
        "content-type": "application/json",
        "authorization": "Bearer test-token",
        "content-length": "1524",
        "accept": "application/json",
        "from": "noreply@google.com",
        "user-agent": "APIs-Google; (+https://developers.google.com/webmasters/APIs-Google.html)",
        "x-cloud-trace-context": "cb94e62c4850655436ac78b7e9466b18/12389870433059380709;o=1",
        "traceparent": "00-cb94e62c4850655436ac78b7e9466b18-abf1a983b79e9de5-01",
        "x-forwarded-for": "66.102.6.198",
        "x-forwarded-proto": "https",
        "forwarded": 'for="66.102.6.198";proto=https',
        "accept-encoding": "gzip, deflate, br",
    }


@pytest.fixture
def eventarc_request_headers():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return {
        "host": "routing-transformer-service-prod-nhgde2snxa-uc.a.run.app",
        "content-type": "application/json",
        "authorization": "Bearer F4keT0k3n",
        "content-length": "1733",
        "accept": "application/json",
        "from": "noreply@google.com",
        "user-agent": "APIs-Google; (+https://developers.google.com/webmasters/APIs-Google.html)",
        "x-cloud-trace-context": "fa75a3bf8bed02e44d18cb795acf931b/10784891745238219625",
        "traceparent": "00-fa75a3bf8bed02e44d18cb795acf931b-95aba1e6c7f77b69-00",
        "x-forwarded-for": "64.233.172.66",
        "x-forwarded-proto": "https",
        "forwarded": 'for="64.233.172.66";proto=https',
        "accept-encoding": "gzip, deflate, br",
        "ce-id": "12180675488418773",
        "ce-source": "//pubsub.googleapis.com/projects/cdip-prod1-78ca/topics/raw-observations-prod",
        "ce-specversion": "1.0",
        "ce-type": "google.cloud.pubsub.topic.v1.messagePublished",
        "ce-time": timestamp,
    }
