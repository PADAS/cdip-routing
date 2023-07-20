import pytest
from app.subscribers.kafka_subscriber import process_observation


@pytest.mark.asyncio
async def test_process_events_v2(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    unprocessed_event_v2,
):
    # Mock external dependencies
    mocker.patch("app.transform_service.services._cache_db", mock_cache)
    mocker.patch("app.transform_service.services.portal_v2", mock_gundi_client_v2)
    mocker.patch("app.subscribers.kafka_subscriber.pubsub", mock_pubsub_client)
    await process_observation(None, unprocessed_event_v2)
    # Check that the right methods, to publish to PuSub, were called
    assert mock_pubsub_client.PublisherClient.called
    assert mock_pubsub_client.PublisherClient.return_value.publish.called


@pytest.mark.asyncio
async def test_process_attachments_v2(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub_client,
    unprocessed_attachment_v2,
):
    # Mock external dependencies
    mocker.patch("app.transform_service.services._cache_db", mock_cache)
    mocker.patch("app.transform_service.services.portal_v2", mock_gundi_client_v2)
    mocker.patch("app.subscribers.kafka_subscriber.pubsub", mock_pubsub_client)
    await process_observation(None, unprocessed_attachment_v2)
    # Check that the right methods, to publish to PuSub, were called
    assert mock_pubsub_client.PublisherClient.called
    assert mock_pubsub_client.PublisherClient.return_value.publish.called

