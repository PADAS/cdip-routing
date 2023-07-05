import datetime
import aiohttp
import pytest
import asyncio
import gundi_core.schemas.v2 as schemas_v2
from aiohttp.client_reqrep import ConnectionKey
from cdip_connector.core.schemas import OutboundConfiguration
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


def _set_side_effect_error_on_gundi_client_once(mock_client, error):
    # Side effects to raise an exception only the first time each method is called
    mock_client.get_inbound_integration.side_effect = [
        error,
        async_return(inbound_integration_config)
    ]
    mock_client.get_outbound_integration.side_effect = [
        error,
        async_return(outbound_integration_config)
    ]
    mock_client.get_outbound_integration_list.side_effect = [
        error,
        async_return(outbound_integration_config_list)
    ]
    mock_client.ensure_device.side_effect = [
        error,
        async_return(device)
    ]


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
    _set_side_effect_error_on_gundi_client_once(mock_client=mock_client, error=client_connector_error)
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
    server_disconnected_error = aiohttp.ServerDisconnectedError()
    _set_side_effect_error_on_gundi_client_once(mock_client=mock_client, error=server_disconnected_error)
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
    # Simulate a server disconnected error
    server_timeout_error = aiohttp.ServerTimeoutError()
    _set_side_effect_error_on_gundi_client_once(mock_client=mock_client, error=server_timeout_error)
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


# ToDo: Find a common place for mocks and testing utilities that can be reused across services
@pytest.fixture
def mock_gundi_client_v2(
        mocker,
        connection_v2,
        destination_integration_v2,
        route_v2,
):
    mock_client = mocker.MagicMock()
    mock_client.get_connection_details.return_value = async_return(
        connection_v2
    )
    mock_client.get_integration_details.return_value = async_return(
        destination_integration_v2
    )
    mock_client.get_route_details.return_value = async_return(
        route_v2
    )
    mock_client.__aenter__.return_value = mock_client
    return mock_client


@pytest.fixture
def mock_gundi_client_v2_class(mocker, mock_gundi_client_v2):
    mock_gundi_client_v2_class = mocker.MagicMock()
    mock_gundi_client_v2_class.return_value = mock_gundi_client_v2
    return mock_gundi_client_v2_class


@pytest.fixture
def destination_integration_v2():
    return schemas_v2.Integration.parse_obj(
        {
            'id': '338225f3-91f9-4fe1-b013-353a229ce504',
            'name': 'ER Load Testing',
            'base_url': 'https://gundi-load-testing.pamdas.org', 'enabled': True,
            'type': {'id': '46c66a61-71e4-4664-a7f2-30d465f87aae', 'name': 'EarthRanger',
                     'description': 'Integration type for Earth Ranger Sites', 'actions': [
                    {'id': '42ec4163-2f40-43fc-af62-bca1db77c06c', 'type': 'auth', 'name': 'Authenticate',
                     'value': 'auth', 'description': 'Authenticate against Earth Ranger',
                     'schema': {'type': 'object', 'required': ['token'], 'properties': {'token': {'type': 'string'}}}},
                    {'id': '016c2098-f494-40ec-a595-710b314d5eaf', 'type': 'pull', 'name': 'Pull Positions',
                     'value': 'pull_positions', 'description': 'Pull position data from an Earth Ranger site',
                     'schema': {'type': 'object', 'required': ['endpoint'],
                                'properties': {'endpoint': {'type': 'string'}}}},
                    {'id': '8886bb71-9aca-425a-881f-7fe0b2dba4f5', 'type': 'push', 'name': 'Push Events',
                     'value': 'push_events', 'description': 'EarthRanger sites support sending Events (a.k.a Reports)',
                     'schema': {}},
                    {'id': 'abe0cf50-fbc7-4810-84fd-53fb75020a55', 'type': 'push', 'name': 'Push Positions',
                     'value': 'push_positions', 'description': 'Push position data to an Earth Ranger site',
                     'schema': {'type': 'object', 'required': ['endpoint'],
                                'properties': {'endpoint': {'type': 'string'}}}}]},
            'owner': {'id': '12d1b0fc-69fe-408b-afc5-8f54872730c1', 'name': 'Test Organization', 'description': ''},
            'configurations': [
                {'id': '013ea7ce-4944-4f7e-8a2f-e5338b3741ce', 'integration': '228225f3-91f9-4fe1-b013-353a229ce505',
                 'action': {'id': '43ec4163-2f40-43fc-af62-bca1db77c06b', 'type': 'auth', 'name': 'Authenticate',
                            'value': 'auth'}, 'data': {'token': '0890d87681cd1d01ad07c2d0f57d15d6079ae7d7'}},
                {'id': '5de91c7b-f28a-4ce7-8137-273ac10674d2', 'integration': '228225f3-91f9-4fe1-b013-353a229ce505',
                 'action': {'id': 'aae0cf50-fbc7-4810-84fd-53fb75020a43', 'type': 'push', 'name': 'Push Positions',
                            'value': 'push_positions'}, 'data': {'endpoint': 'api/v1/positions'}},
                {'id': '7947b19e-1d2d-4ca3-bd6c-74976ae1de68', 'integration': '228225f3-91f9-4fe1-b013-353a229ce505',
                 'action': {'id': '036c2098-f494-40ec-a595-710b314d5ea5', 'type': 'pull', 'name': 'Pull Positions',
                            'value': 'pull_positions'}, 'data': {'endpoint': 'api/v1/positions'}}],
            'additional': {'topic': 'destination-v2-228225f3-91f9-4fe1-b013-353a229ce505-dev', 'broker': 'gcp_pubsub'},
            'default_route': {'id': '38dd8ec2-b3ee-4c31-940e-b6cc9c1f4326', 'name': 'Mukutan - Load Testing'},
            'status': {'id': 'mockid-b16a-4dbd-ad32-197c58aeef59', 'is_healthy': True,
                       'details': 'Last observation has been delivered with success.',
                       'observation_delivered_24hrs': 50231,
                       'last_observation_delivered_at': '2023-03-31T11:20:00+0200'}
        }
    )


@pytest.fixture
def connection_v2():
    return schemas_v2.Connection.parse_obj(
        {
            'id': 'bbd0946d-15b0-4308-b93d-e0470b6c33b7',
            'provider': {'id': 'bbd0946d-15b0-4308-b93d-e0470b6c33b7', 'name': 'Trap Tagger', 'type': 'traptagger',
                         'base_url': 'https://test.traptagger.com', 'status': 'healthy'}, 'destinations': [
            {'id': '338225f3-91f9-4fe1-b013-353a229ce504', 'name': 'ER Load Testing', 'type': 'earth_ranger',
             'base_url': 'https://gundi-load-testing.pamdas.org', 'status': 'healthy'}],
            'routing_rules': [{'id': '945897f9-1ef2-7d55-9c6c-ea2663380ca5', 'name': 'TrapTagger Default Route'}],
            'default_route': {'id': '945897f9-1ef2-7d55-9c6c-ea2663380ca5', 'name': 'TrapTagger Default Route'},
            'owner': {'id': 'b3d1b0fc-69fe-408b-afc5-7f54872730c1', 'name': 'Test Organization', 'description': ''},
            'status': 'healthy'
        }
    )


@pytest.fixture
def route_v2():
    return schemas_v2.Route.parse_obj(
        {
            'id': '775897f9-1ef2-4d10-9c6c-ea2663380c5b', 'name': 'TrapTagger Default Route',
            'owner': 'b3d1b0fc-69fe-408b-afc5-7f54872730c1', 'data_providers': [
            {'id': 'ccd0946d-15b0-4308-b93d-e0470b6d33b5', 'name': 'Trap Tagger', 'type': 'traptagger',
             'base_url': 'https://test.traptagger.com', 'status': 'healthy'}], 'destinations': [
            {'id': '228225f3-91f9-4fe1-b013-353a229ce512', 'name': 'ER Load Testing', 'type': 'earth_ranger',
             'base_url': 'https://gundi-load-testing.pamdas.org', 'status': 'healthy'}],
            'configuration': {
                'id': '5b3e3e73-94ad-42cb-a765-09a7193ae0b6',
                'name': 'Trap Tagger to ER - Event Type Mapping',
                'data': {
                    'field_mappings': {
                        'ddd0946d-15b0-4308-b93d-e0470b6d33b6': {
                            'ev': {
                                '558225f3-91f9-4fe1-b013-353a229ce503': {
                                    'map': {
                                        'Leopard':  'leopard_sighting',
                                        'Wilddog':  'wild_dog_sighting',
                                    },
                                    'default': 'wildlife_sighting_rep',
                                    'provider_field': 'event_details__species',
                                    'destination_field': 'event_type'
                                }
                            }
                        }
                    }
                }
            },
            'additional': {}
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
        gundi_id='b9b46dc1-e033-447d-a99b-0fe373ca04c9',
        related_to='None',
        owner='e2d1b0fc-69fe-408b-afc5-7f54872730c0',
        data_provider_id='ddd0946d-15b0-4308-b93d-e0470b6d33b6',
        annotations={},
        source_id='afa0d606-c143-4705-955d-68133645db6d',
        external_source_id='Xyz123',
        recorded_at=datetime.datetime(2023, 7, 4, 21, 38, tzinfo=datetime.timezone.utc),
        location=schemas_v2.Location(
            lat=-51.667875,
            lon=-72.71195,
            alt=1800.0,
            hdop=None,
            vdop=None
        ),
        title='Leopard Detected',
        event_type=None,
        event_details={
            'site_name': 'Camera2G',
            'species': 'Leopard',
            'tags': ['female adult', 'male child'],
            'animal_count': 2
        },
        geometry={},
        observation_type='ev'
    )


@pytest.fixture
def unmapped_animal_detected_event_v2():
    return schemas_v2.Event(
        gundi_id='b9b46dc1-e033-447d-a99b-0fe373ca04c9',
        related_to='None',
        owner='e2d1b0fc-69fe-408b-afc5-7f54872730c0',
        data_provider_id='ddd0946d-15b0-4308-b93d-e0470b6d33b6',
        annotations={},
        source_id='afa0d606-c143-4705-955d-68133645db6d',
        external_source_id='Xyz123',
        recorded_at=datetime.datetime(2023, 7, 4, 21, 38, tzinfo=datetime.timezone.utc),
        location=schemas_v2.Location(
            lat=-51.667875,
            lon=-72.71195,
            alt=1800.0,
            hdop=None,
            vdop=None
        ),
        title='Animal Detected',
        event_type=None,
        event_details={
            'site_name': 'Camera2G',
            'species': 'Unknown',
            'tags': ['female adult', 'male child'],
            'animal_count': 2
        },
        geometry={},
        observation_type='ev'
    )


@pytest.fixture
def destination_integration_v2_er():
    return schemas_v2.ConnectionIntegration(
        id='558225f3-91f9-4fe1-b013-353a229ce503',
        name='ER Load Testing',
        type='earth_ranger',
        base_url='https://gundi-load-testing.pamdas.org',
        status='healthy'
    )


@pytest.fixture
def route_config_with_event_type_mappings():
    return schemas_v2.RouteConfiguration(
        id='1a3e3e73-94ad-42cb-a765-09a7193ae0b1',
        name='Trap Tagger to ER - Event Type Mapping',
        data={
            'field_mappings': {
                'ddd0946d-15b0-4308-b93d-e0470b6d33b6': {
                    'ev': {
                        '558225f3-91f9-4fe1-b013-353a229ce503': {
                            'map': {
                                'Leopard': 'leopard_sighting',
                                'Wilddog': 'wild_dog_sighting'
                            },
                            'default': 'wildlife_sighting_rep',
                            'provider_field': 'event_details__species',
                            'destination_field': 'event_type'
                        }
                    }
                }
            }
        }
    )
