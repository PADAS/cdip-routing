import pytest
from app.services.process_messages import process_observation_event
from app.core.utils import get_provider_key


@pytest.mark.asyncio
async def test_process_observations_v2(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub,
    raw_observation_v2,
    raw_observation_v2_attributes,
):
    # Mock external dependencies
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    await process_observation_event(raw_observation_v2, raw_observation_v2_attributes)
    # Check that the right methods, to publish to PuSub, were called
    assert mock_pubsub.PublisherClient.called
    assert mock_pubsub.PublisherClient.return_value.publish.called


@pytest.mark.asyncio
async def test_process_events_v2(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub,
    raw_event_v2,
    raw_event_v2_attributes,
):
    # Mock external dependencies
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    await process_observation_event(raw_event_v2, raw_event_v2_attributes)
    # Check that the right methods, to publish to PuSub, were called
    assert mock_pubsub.PublisherClient.called
    assert mock_pubsub.PublisherClient.return_value.publish.called


@pytest.mark.asyncio
async def test_process_event_update(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub,
    raw_event_update,
    raw_event_update_attributes,
):
    # Mock external dependencies
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    await process_observation_event(raw_event_update, raw_event_update_attributes)
    # Check that the right methods, to publish to PuSub, were called
    assert mock_pubsub.PublisherClient.called
    assert mock_pubsub.PublisherClient.return_value.publish.called


@pytest.mark.asyncio
async def test_process_attachments_v2(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    mock_pubsub,
    raw_attachment_v2,
    raw_attachment_v2_attributes,
):
    # Mock external dependencies
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    await process_observation_event(raw_attachment_v2, raw_attachment_v2_attributes)
    # Check that the right methods, to publish to PuSub, were called
    assert mock_pubsub.PublisherClient.called
    assert mock_pubsub.PublisherClient.return_value.publish.called


@pytest.mark.asyncio
async def test_default_provider_key(
    connection_v2,
):
    provider = connection_v2.provider
    provider_key = get_provider_key(provider)
    assert provider_key == f"gundi_{provider.type.value}_{str(provider.id)}"
