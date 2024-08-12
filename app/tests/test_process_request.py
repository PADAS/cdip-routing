import unittest.mock

import pytest
from fastapi.testclient import TestClient

from app.conftest import async_return
from app.core import settings
from app.core.deduplication import EventProcessingStatus, get_event_status_key
from app.main import app
from app.services.transformers import extract_fields_from_message

api_client = TestClient(app)


@pytest.mark.asyncio
async def test_process_geoevent_v1_successfully(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_pubsub,
    pubsub_request_headers,
    geoevent_v1_request_payload,
):
    # Mock external dependencies
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi._portal", mock_gundi_client)
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    response = api_client.post(
        "/",
        headers=pubsub_request_headers,
        json=geoevent_v1_request_payload,
    )
    assert response.status_code == 200
    # Check that the report was sent to a pubsub topic
    assert mock_pubsub.PublisherClient.called
    assert mock_pubsub.PublisherClient.return_value.publish.called


@pytest.mark.asyncio
async def test_process_event_v2_successfully(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub,
    pubsub_request_headers,
    event_v2_request_payload,
    destination_integration_v2
):
    # Mock external dependencies
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    response = api_client.post(
        "/",
        headers=pubsub_request_headers,
        json=event_v2_request_payload,
    )
    assert response.status_code == 200
    # Check that the report was sent to a pubsub topic once
    assert mock_pubsub.PublisherClient.called
    mocked_publish = mock_pubsub.PublisherClient.return_value.publish
    assert mocked_publish.call_count == 1
    # Check that the message was sent to the right topic
    topic = destination_integration_v2.additional["topic"]
    mocked_topic = mock_pubsub.PublisherClient.return_value.topic_path
    mocked_topic.assert_called_once_with(settings.GCP_PROJECT_ID, topic)


@pytest.mark.asyncio
async def test_process_event_update_v2_successfully(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub,
    pubsub_request_headers,
    event_update_v2_request_payload,
):
    # Mock external dependencies
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    response = api_client.post(
        "/",
        headers=pubsub_request_headers,
        json=event_update_v2_request_payload,
    )
    assert response.status_code == 200
    # Check that an event was sent to a pubsub topic with ordering key
    gundi_id = event_update_v2_request_payload["message"]["attributes"]["gundi_id"]
    assert mock_pubsub.PubsubMessage.called
    assert mock_pubsub.PubsubMessage.call_args.kwargs["ordering_key"] == gundi_id
    assert mock_pubsub.PublisherClient.called
    mocked_publish = mock_pubsub.PublisherClient.return_value.publish
    assert mocked_publish.call_count == 1


@pytest.mark.asyncio
async def test_message_deduplication(
    mocker,
    mock_cache,
    mock_deduplication_cache,
    mock_gundi_client_v2,
    mock_pubsub,
    pubsub_request_headers,
    event_update_v2_request_payload,
    destination_integration_v2
):
    # Mock external dependencies
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.deduplication._cache_db", mock_deduplication_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    response = api_client.post(
        "/",
        headers=pubsub_request_headers,
        json=event_update_v2_request_payload,
    )
    assert response.status_code == 200
    # Check that the message was sent to routing
    topic = destination_integration_v2.additional["topic"]
    mocked_topic = mock_pubsub.PublisherClient.return_value.topic_path
    mocked_topic.assert_called_once_with(settings.GCP_PROJECT_ID, topic)
    mocked_publish = mock_pubsub.PublisherClient.return_value.publish
    assert mocked_publish.call_count == 1
    # Check that the status was set to processed
    data, attributes = extract_fields_from_message(event_update_v2_request_payload["message"])
    event_id = data["event_id"]
    key = get_event_status_key(event_id=event_id)
    ttl = settings.EVENT_PROCESSING_STATUS_TTL
    value = EventProcessingStatus.PROCESSED.value
    mock_deduplication_cache.setex.assert_called_once_with(key, ttl, value)
    mock_deduplication_cache.get.return_value = async_return(1)  # Message already processed
    # Send the same request a second time
    response = api_client.post(
        "/",
        headers=pubsub_request_headers,
        json=event_update_v2_request_payload,
    )
    assert response.status_code == 200
    # Check that the message was sent to the dead-letter
    mocked_topic.assert_called_with(settings.GCP_PROJECT_ID, settings.DEAD_LETTER_TOPIC)
    assert mocked_publish.call_count == 2
