import datetime
import aiohttp
import httpx
import pytest
import uuid
import asyncio
import gundi_core.schemas.v2 as schemas_v2
import gundi_core.schemas.v1 as schemas_v1
from aiohttp.client_reqrep import ConnectionKey
from cdip_connector.core.schemas import OutboundConfiguration
from cdip_connector.core.routing import TopicEnum
from pydantic.types import UUID
from smartconnect.async_client import SMARTClientException


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
    with open("app/transform_service/tests/test_datamodel.xml", "r") as f:
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
def mock_smart_async_client_class(mocker, mock_smart_async_client):
    mock_smart_async_client_class = mocker.MagicMock()
    mock_smart_async_client_class.return_value = mock_smart_async_client
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
def mock_pubsub_client(mocker, gcp_pubsub_publish_response):
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
def mock_kafka_topic(new_kafka_topic):
    return new_kafka_topic()


@pytest.fixture
def mock_dead_letter_kafka_topic(new_kafka_topic):
    return new_kafka_topic()


@pytest.fixture
def new_kafka_topic(mocker, kafka_topic_send_response):
    def _make_topic():
        mock_topic = mocker.MagicMock()
        mock_topic.send.return_value = async_return(kafka_topic_send_response)
        return mock_topic

    return _make_topic


@pytest.fixture
def mock_kafka_topics_dic(new_kafka_topic):
    topics_dict = {
        TopicEnum.observations_unprocessed.value: new_kafka_topic(),
        TopicEnum.observations_unprocessed_retry_short: new_kafka_topic(),
        TopicEnum.observations_unprocessed_retry_long: new_kafka_topic(),
        TopicEnum.observations_unprocessed_deadletter: new_kafka_topic(),
        TopicEnum.observations_transformed: new_kafka_topic(),
        TopicEnum.observations_transformed_retry_short: new_kafka_topic(),
        TopicEnum.observations_transformed_retry_long: new_kafka_topic(),
        TopicEnum.observations_transformed_deadletter: new_kafka_topic(),
    }
    return topics_dict


@pytest.fixture
def gcp_pubsub_publish_response():
    return {"messageIds": ["7061707768812258"]}


@pytest.fixture
def kafka_topic_send_response():
    return {}


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
        "additional": {},
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
            "additional": {},
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
            "password": "***REMOVED***",
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
def unprocessed_observation_position():
    return b'{"attributes": {"observation_type": "ps"}, "data": {"id": null, "owner": "na", "integration_id": "36485b4f-88cd-49c4-a723-0ddff1f580c4", "device_id": "018910980", "name": "Logistics Truck test", "type": "tracking-device", "subject_type": null, "recorded_at": "2023-03-03 09:34:00+02:00", "location": {"x": 35.43935, "y": -1.59083, "z": 0.0, "hdop": null, "vdop": null}, "additional": {"voltage": "7.4", "fuel_level": 71, "speed": "41 kph"}, "voltage": null, "temperature": null, "radio_status": null, "observation_type": "ps"}}'


@pytest.fixture
def unprocessed_observation_geoevent():
    return b'{"attributes": {"observation_type": "ge"}, "data": {"id": null, "owner": "na", "integration_id": "anonymous_consumer", "device_id": "003", "recorded_at": "2023-07-05 09:16:02-03:00", "location": {"x": -55.784992, "y": 20.806785, "z": 0.0, "hdop": null, "vdop": null}, "additional": null, "title": "Rainfall", "event_type": "rainfall_rep", "event_details": {"amount_mm": 6, "height_m": 3}, "geometry": null, "observation_type": "ge"}}'


@pytest.fixture
def unprocessed_observation_cameratrap():
    return b'{"attributes": {"observation_type": "ct"}, "data": {"id": null, "owner": "integration:17e7a1e0-168b-4f68-9392-35ec29222f13", "integration_id": "17e7a1e0-168b-4f68-9392-35ec29222f13", "device_id": "test_cam", "name": null, "type": "camerea-trap", "recorded_at": "2023-03-21 09:29:00-03:00", "location": {"x": -122.5, "y": 48.65, "z": 0.0, "hdop": null, "vdop": null}, "additional": null, "image_uri": "2023-07-04-1851_leopard.jpg", "camera_name": "test_cam", "camera_description": "Test camera", "camera_version": null, "observation_type": "ct"}}'


@pytest.fixture
def unprocessed_observation_er_event():
    return b'{"attributes": {}, "data": {"id": "d3109853-747e-4c39-b821-6c897a992744", "owner": "na", "integration_id": "1055c18c-ae2f-4609-aa2f-dde86419c701", "er_uuid": "d3109853-747e-4c39-b821-6c897a992744", "location": {"latitude": -41.145108, "longitude": -71.262104}, "time": "2024-02-08 06:08:00-06:00", "created_at": "2024-02-08 06:08:51.424788-06:00", "updated_at": "2024-02-08 06:08:51.424124-06:00", "serial_number": 49534, "event_type": "169361d0-62b8-411d-a8e6-019823805016_animals_sign", "priority": 0, "priority_label": "Gray", "title": null, "state": "active", "url": "https://gundi-er.pamdas.org/api/v1.0/activity/event/d3109853-747e-4c39-b821-6c897a992744", "event_details": {"comments": "Mm test", "updates": []}, "patrols": [], "files": [], "uri": "", "device_id": "d3109853-747e-4c39-b821-6c897a992744", "observation_type": "er_event"}}'


@pytest.fixture
def unprocessed_observation_er_patrol():
    return b'{"attributes": {}, "data": {"id": "2ce0001b-f2a3-4a4d-a108-a051019fc27d", "owner": "na", "integration_id": "1055c18c-ae2f-4609-aa2f-dde86419c701", "files": [], "serial_number": 14, "title": "Routine Patrol", "device_id": "2ce0001b-f2a3-4a4d-a108-a051019fc27d", "notes": [], "objective": "Routine Patrol", "patrol_segments": [{"end_location": null, "events": [{"id": "c9ca269e-c873-423c-ab12-b72791b303b0", "event_type": "169361d0-62b8-411d-a8e6-019823805016_animals_sign", "updated_at": "2024-02-07 14:42:36.638888-06:00", "geojson": null}, {"id": "b8e589fb-baf2-476a-96a6-0c87cf2ed847", "event_type": "169361d0-62b8-411d-a8e6-019823805016_animals_sign", "updated_at": "2024-02-08 10:04:25.052375-06:00", "geojson": {"type": "Feature", "geometry": {"type": "Point", "coordinates": [-122.3589888, 47.6839936]}}}], "event_details": [{"id": "c9ca269e-c873-423c-ab12-b72791b303b0", "owner": "na", "integration_id": null, "er_uuid": "c9ca269e-c873-423c-ab12-b72791b303b0", "location": null, "time": "2024-02-07 14:42:09.709000-06:00", "created_at": "2024-02-07 14:42:36.638857-06:00", "updated_at": "2024-02-07 14:42:36.637713-06:00", "serial_number": 49532, "event_type": "169361d0-62b8-411d-a8e6-019823805016_animals_sign", "priority": 0, "priority_label": "Gray", "title": null, "state": "active", "url": "https://gundi-er.pamdas.org/api/v1.0/activity/event/c9ca269e-c873-423c-ab12-b72791b303b0", "event_details": {"animalpoached": "carcass", "targetspecies": "birds.greyheron", "actiontakenanimal": "conficated", "numberofanimalpoached": 4, "updates": []}, "patrols": ["2ce0001b-f2a3-4a4d-a108-a051019fc27d"], "files": [], "uri": "", "device_id": "none", "observation_type": "er_event"}, {"id": "b8e589fb-baf2-476a-96a6-0c87cf2ed847", "owner": "na", "integration_id": null, "er_uuid": "b8e589fb-baf2-476a-96a6-0c87cf2ed847", "location": {"latitude": 47.6839936, "longitude": -122.3589888}, "time": "2024-02-08 10:04:11.203000-06:00", "created_at": "2024-02-08 10:04:25.052343-06:00", "updated_at": "2024-02-08 10:04:25.051476-06:00", "serial_number": 49539, "event_type": "169361d0-62b8-411d-a8e6-019823805016_animals_sign", "priority": 0, "priority_label": "Gray", "title": null, "state": "active", "url": "https://gundi-er.pamdas.org/api/v1.0/activity/event/b8e589fb-baf2-476a-96a6-0c87cf2ed847", "event_details": {"affiliation_whn": "ranger_whn", "nameprotectedarea_whn": "phousithonesca_whn", "patrolleaderorfocalpoint_whn": "ronniemartinez_whn", "updates": []}, "patrols": ["2ce0001b-f2a3-4a4d-a108-a051019fc27d"], "files": [], "uri": "", "device_id": "none", "observation_type": "er_event"}], "id": "b575aeb0-4736-4df3-86ac-6a9ae47bcb4c", "leader": {"id": "10ce2200-6565-401a-837d-fc153fb9db41", "name": "Chris Doehring (Gundi)", "subject_subtype": "ranger", "additional": {"ca_uuid": "test1230-62b8-411d-a8e6-019823805016", "smart_member_id": "a99bbb3959d04ba7b02250b21b8a3d2b"}, "is_active": true}, "patrol_type": "routine_patrol", "scheduled_start": null, "scheduled_end": "2024-02-09 02:00:00-06:00", "start_location": {"latitude": 47.686076965642606, "longitude": -122.35928648394253}, "time_range": {"start_time": "2024-02-07T14:40:24-06:00", "end_time": null}, "updates": [{"message": "Updated fields: ", "time": "2024-02-08 16:04:27.683350+00:00", "user": {"username": "***REMOVED***", "first_name": "Chris", "last_name": "Doehring", "id": "8ffee729-7ff5-490e-9206-6f33a4038926", "content_type": "accounts.user"}, "type": "update_segment"}, {"message": "Report Added", "time": "2024-02-08 16:04:25.087190+00:00", "user": {"username": "***REMOVED***", "first_name": "Chris", "last_name": "Doehring", "id": "8ffee729-7ff5-490e-9206-6f33a4038926", "content_type": "accounts.user"}, "type": "add_event"}, {"message": "Updated fields: ", "time": "2024-02-08 16:03:33.073148+00:00", "user": {"username": "***REMOVED***", "first_name": "Chris", "last_name": "Doehring", "id": "8ffee729-7ff5-490e-9206-6f33a4038926", "content_type": "accounts.user"}, "type": "update_segment"}, {"message": "Report Added", "time": "2024-02-07 20:42:36.667571+00:00", "user": {"username": "***REMOVED***", "first_name": "Chris", "last_name": "Doehring", "id": "8ffee729-7ff5-490e-9206-6f33a4038926", "content_type": "accounts.user"}, "type": "add_event"}, {"message": "Updated fields: Scheduled End", "time": "2024-02-07 20:41:55.477668+00:00", "user": {"username": "***REMOVED***", "first_name": "Chris", "last_name": "Doehring", "id": "8ffee729-7ff5-490e-9206-6f33a4038926", "content_type": "accounts.user"}, "type": "update_segment"}], "track_points": [{"id": "c37a9148-f061-439d-ac60-5e4e4a8f03c8", "location": {"latitude": 47.68607630950026, "longitude": -122.3592951927474}, "created_at": "2024-02-09 04:33:53+00:00", "recorded_at": "2024-02-09 04:33:52+00:00", "source": "cf72a238-b4d1-46c8-8b80-01f46c474e22", "observation_details": {"accuracy": 4.7}}, {"id": "091a74bb-014a-44cf-8713-56ed866205ef", "location": {"latitude": 47.68612342657477, "longitude": -122.3592770754273}, "created_at": "2024-02-09 04:43:53+00:00", "recorded_at": "2024-02-09 04:43:52+00:00", "source": "cf72a238-b4d1-46c8-8b80-01f46c474e22", "observation_details": {"accuracy": 4.7}}, {"id": "cf6fd52f-37d4-4411-a761-a1721b792deb", "location": {"latitude": 47.68612342657477, "longitude": -122.3592770754273}, "created_at": "2024-02-09 04:53:53+00:00", "recorded_at": "2024-02-09 04:53:52+00:00", "source": "cf72a238-b4d1-46c8-8b80-01f46c474e22", "observation_details": {"accuracy": 4.7}}]}], "observation_type": "er_patrol", "state": "open", "updates": [{"message": "Patrol Added", "time": "2024-02-07 20:40:24.507001+00:00", "user": {"username": "***REMOVED***", "first_name": "Chris", "last_name": "Doehring", "id": "8ffee729-7ff5-490e-9206-6f33a4038926", "content_type": "accounts.user"}, "type": "add_patrol"}, {"message": "Updated fields: ", "time": "2024-02-08 16:04:27.683350+00:00", "user": {"username": "***REMOVED***", "first_name": "Chris", "last_name": "Doehring", "id": "8ffee729-7ff5-490e-9206-6f33a4038926", "content_type": "accounts.user"}, "type": "update_segment"}, {"message": "Report Added", "time": "2024-02-08 16:04:25.087190+00:00", "user": {"username": "***REMOVED***", "first_name": "Chris", "last_name": "Doehring", "id": "8ffee729-7ff5-490e-9206-6f33a4038926", "content_type": "accounts.user"}, "type": "add_event"}, {"message": "Updated fields: ", "time": "2024-02-08 16:03:33.073148+00:00", "user": {"username": "***REMOVED***", "first_name": "Chris", "last_name": "Doehring", "id": "8ffee729-7ff5-490e-9206-6f33a4038926", "content_type": "accounts.user"}, "type": "update_segment"}, {"message": "Report Added", "time": "2024-02-07 20:42:36.667571+00:00", "user": {"username": "***REMOVED***", "first_name": "Chris", "last_name": "Doehring", "id": "8ffee729-7ff5-490e-9206-6f33a4038926", "content_type": "accounts.user"}, "type": "add_event"}, {"message": "Updated fields: Scheduled End", "time": "2024-02-07 20:41:55.477668+00:00", "user": {"username": "***REMOVED***", "first_name": "Chris", "last_name": "Doehring", "id": "8ffee729-7ff5-490e-9206-6f33a4038926", "content_type": "accounts.user"}, "type": "update_segment"}]}}'


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
def transformed_observation_kafka_message():
    # ToDo: complete the implementation
    return b'{"attributes": {"observation_type": "ps", "device_id": "018910980", "outbound_config_id": "5f658487-67f7-43f1-8896-d78778e49c30", "integration_id": "cf28f902-23b8-4c91-8843-554ca1ecac1a"}, "data": {"manufacturer_id": "018910980", "source_type": "tracking-device", "subject_name": "Logistics Truck A", "recorded_at": "2023-04-11 12:40:00-03:00", "location": {"lon": 35.43902, "lat": -1.59083}, "additional": {"voltage": "7.4", "fuel_level": 71, "speed": "41 kph"}}}'


@pytest.fixture
def outbound_configuration_gcp_pubsub():
    return OutboundConfiguration.parse_obj(
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
def outbound_configuration_kafka():
    return OutboundConfiguration.parse_obj(
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
            "additional": {"broker": "kafka"},
        }
    )


@pytest.fixture
def smart_outbound_configuration_kafka():
    return OutboundConfiguration.parse_obj(
        {
            "id": "1c19dc7e-73e2-4af3-93f5-a1cb322e5add",
            "type": "f61b0c60-c863-44d7-adc6-d9b49b389e69",
            "owner": "088a191a-bcf3-471b-9e7d-6ba8bc71be9e",
            "name": "ER to SMART smartdemoconnect Test",
            "endpoint": "https://smartdemoconnect.smartconservationtools.org/server",
            "state": {},
            "login": "",
            "password": "",
            "token": "test123681cd1d01ad07c2d0f57d15d6079ae7d7",
            "type_slug": "smart_connect",
            "inbound_type_slug": "earth_ranger",
            "additional": {
                "ca_uuids": ["test1230-62b8-411d-a8e6-019823805016"],
                "configurable_models_lists": {
                    "test1230-62b8-411d-a8e6-019823805016": [
                        {
                            "ca_id": "SMART",
                            "ca_name": "Demo Conservation Area",
                            "ca_uuid": "test1230-62b8-411d-a8e6-019823805016",
                            "name": "\u1782\u17bc\u179b\u17c2\u1793 \u1796\u17d2\u179a\u17a0\u17d2\u1798\u1791\u17c1\u1796 072022",
                            "translations": [
                                {
                                    "language_code": "en",
                                    "value": "\u1782\u17bc\u179b\u17c2\u1793 \u1796\u17d2\u179a\u17a0\u17d2\u1798\u1791\u17c1\u1796 072022",
                                },
                                {
                                    "language_code": "km",
                                    "value": "\u1782\u17bc\u179b\u17c2\u1793 \u1796\u17d2\u179a\u17a0\u17d2\u1798\u1791\u17c1\u1796 072022",
                                },
                            ],
                            "use_with_earth_ranger": False,
                            "uuid": "303b2e0a-d4b7-41b8-b6dc-065c9a661c7b",
                        },
                        {
                            "ca_id": "SMART",
                            "ca_name": "Demo Conservation Area",
                            "ca_uuid": "169361d0-62b8-411d-a8e6-019823805016",
                            "name": "\u1781\u1793\u17b7\u1780 \u1782\u17bc\u179b\u17c2\u1793 \u1796\u17d2\u179a\u17a0\u17d2\u1798\u1791\u17c1\u1796 092022",
                            "translations": [
                                {
                                    "language_code": "en",
                                    "value": "\u1781\u1793\u17b7\u1780 \u1782\u17bc\u179b\u17c2\u1793 \u1796\u17d2\u179a\u17a0\u17d2\u1798\u1791\u17c1\u1796 092022",
                                }
                            ],
                            "use_with_earth_ranger": False,
                            "uuid": "a645f302-7fb0-4a29-a0ce-9d0092652803",
                        },
                    ]
                },
                "version": "7.5.7",
            },
        }
    )


@pytest.fixture
def outbound_configuration_default():
    return OutboundConfiguration.parse_obj(
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
            "status": {
                "id": "mockid-b16a-4dbd-ad32-197c58aeef59",
                "is_healthy": True,
                "details": "Last observation has been delivered with success.",
                "observation_delivered_24hrs": 50231,
                "last_observation_delivered_at": "2023-03-31T11:20:00+0200",
            },
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
def unprocessed_event_v2():
    return b'{"attributes": {"observation_type": "ev", "gundi_version": "v2", "gundi_id": "5b793d17-cd79-49c8-abaa-712cb40f2b54"}, "data": {"gundi_id": "5b793d17-cd79-49c8-abaa-712cb40f2b54", "related_to": "None", "owner": "e2d1b0fc-69fe-408b-afc5-7f54872730c0", "data_provider_id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6", "annotations": {}, "source_id": "afa0d606-c143-4705-955d-68133645db6d", "external_source_id": "Xyz123", "recorded_at": "2023-07-04T21:38:00+00:00", "location": {"lat": -51.667875, "lon": -72.71195, "alt": 1800.0, "hdop": null, "vdop": null}, "title": "Animal Detected", "event_type": null, "event_details": {"site_name": "Camera2G", "species": "Leopard", "tags": ["female adult", "male child"], "animal_count": 2}, "geometry": {}, "observation_type": "ev"}}'


@pytest.fixture
def unprocessed_attachment_v2():
    return b'{"attributes": {"observation_type": "att", "gundi_version": "v2", "gundi_id": "8b62fdd5-2e70-40e1-b202-f80c6014d596"}, "data": {"gundi_id": "8b62fdd5-2e70-40e1-b202-f80c6014d596", "related_to": "5b793d17-cd79-49c8-abaa-712cb40f2b54", "owner": "na", "data_provider_id": "ddd0946d-15b0-4308-b93d-e0470b6d33b6", "annotations": null, "source_id": "None", "external_source_id": "None", "file_path": "attachments/8b62fdd5-2e70-40e1-b202-f80c6014d596_2023-07-04-1851_leopard.jpg", "observation_type": "att"}}'


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
            "site_name": "MM Spot",
            "species": "lion",
            "tags": ["adult", "male"],
            "animal_count": 2,
            "ageofsign": "days",
        },
        geometry={},
        observation_type="ev",
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
                action=schemas_v2.IntegrationActionSummery(
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
                action=schemas_v2.IntegrationActionSummery(
                    id=UUID("b0a0e7ed-d668-41b5-96d2-397f026c4ecb"),
                    type="auth",
                    name="Authenticate",
                    value="auth",
                ),
                data={
                    "login": "fakeusername",
                    "endpoint": "https://integrationx.smartconservationtools.org/server",
                    "password": "***REMOVED***",
                },
            ),
        ],
        default_route=None,
        additional={},
        status={
            "id": "mockid-b16a-4dbd-ad32-197c58aeef59",
            "is_healthy": True,
            "details": "Last observation has been delivered with success.",
            "observation_delivered_24hrs": 50231,
            "last_observation_delivered_at": "2023-03-31T11:20:00+0200",
        },
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
                            },
                            "default": "wildlife_sighting_rep",
                            "provider_field": "event_details__species",
                            "destination_field": "event_type",
                        }
                    }
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
