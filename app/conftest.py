import datetime
import aiohttp
import pytest
import asyncio
from aiohttp.client_reqrep import ConnectionKey
from gundi_client.schemas import OutboundConfiguration
from cdip_connector.core.routing import TopicEnum


def async_return(result):
    f = asyncio.Future()
    f.set_result(result)
    return f


@pytest.fixture
def mock_cache(mocker):
    mock_cache = mocker.MagicMock()
    mock_cache.get.return_value = None
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
def mock_gundi_client_with_client_connector_error_once(mocker, gcp_pubsub_publish_response):
    mock_client = mocker.MagicMock()
    # Simulate a connection error
    client_connector_error = aiohttp.ClientConnectorError(
        connection_key=ConnectionKey(
            host="cdip-portal.pamdas.org",
            port=443,
            is_ssl=True,
            ssl=True,
            proxy=None,
            proxy_auth=None,
            proxy_headers_hash=None
        ),
        os_error=ConnectionError()
    )
    # Side effects to raise an exception only the first time each method is called
    mock_client.get_inbound_integration.side_effect = [
        client_connector_error,
        async_return(inbound_integration_config)
    ]
    mock_client.get_outbound_integration.side_effect = [
        client_connector_error,
        async_return(outbound_integration_config)
    ]
    mock_client.get_outbound_integration_list.side_effect = [
        client_connector_error,
        async_return(outbound_integration_config_list)
    ]
    mock_client.ensure_device.side_effect = [
        client_connector_error,
        async_return(device)
    ]
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
    mock_publisher.publish.side_effect = [asyncio.TimeoutError(), async_return(gcp_pubsub_publish_response)]
    mock_client.PublisherClient.return_value = mock_publisher
    return mock_client


@pytest.fixture
def mock_pubsub_client_with_client_error_once(mocker, gcp_pubsub_publish_response):
    mock_client = mocker.MagicMock()
    mock_publisher = mocker.MagicMock()
    # Side effects to raise an exception only the first time it's called
    mock_publisher.publish.side_effect = [aiohttp.ClientError(), async_return(gcp_pubsub_publish_response)]
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
        TopicEnum.observations_transformed_deadletter: new_kafka_topic()
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
            "login": "ZULULANDRR",
            "password": "Rh1n0#@!",
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
