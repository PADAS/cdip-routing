import pytest

from app.conftest import async_return
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
async def test_process_observations_v2_with_provider_key_field_mapping(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    connection_v2,
    route_v2_with_provider_key_field_mapping,
    mock_pubsub,
    raw_observation_v2,
    raw_observation_v2_attributes,
):
    # Set a field mapping for the provider key
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        route_v2_with_provider_key_field_mapping
    )
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    raw_observation_v2["payload"]["data_provider_id"] = provider_id
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    mock_send_message_to_gcp_pubsub_dispatcher = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher",
        mock_send_message_to_gcp_pubsub_dispatcher,
    )

    await process_observation_event(raw_observation_v2, raw_observation_v2_attributes)

    # Check that the message for the dispatcher was sent with the correct provider key
    assert mock_send_message_to_gcp_pubsub_dispatcher.call_count == 1
    route_config_data = route_v2_with_provider_key_field_mapping.configuration.data
    expected_provider_key = route_config_data["field_mappings"][provider_id]["obv"][
        destination_id
    ]["default"]
    final_provider_key = mock_send_message_to_gcp_pubsub_dispatcher.call_args[1][
        "attributes"
    ]["provider_key"]
    assert final_provider_key == expected_provider_key


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
