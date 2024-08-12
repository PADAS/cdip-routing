import pytest
from fastapi.testclient import TestClient
from app.main import app

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
    # Check that the report was sent to a pubsub topic
    assert mock_pubsub.PublisherClient.called
    assert mock_pubsub.PublisherClient.return_value.publish.called


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

