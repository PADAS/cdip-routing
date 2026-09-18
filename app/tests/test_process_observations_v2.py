import pytest

from app.conftest import async_return
from app.services.event_handlers import transform_and_route_observation
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


@pytest.mark.asyncio
async def test_observation_without_default_route_is_discarded_and_logged(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    connection_v2_without_default_route,
    raw_observation_v2,
    raw_observation_v2_attributes,
):
    """A provider with no default route is a portal configuration error.

    The observation must be dropped (not retried forever by PubSub), the
    route lookup must not be attempted with a null id, and an ERROR activity
    log must be emitted for the provider integration.
    """
    provider_id = str(connection_v2_without_default_route.provider.id)
    raw_observation_v2["payload"]["data_provider_id"] = provider_id
    mock_gundi_client_v2.get_connection_details.return_value = async_return(
        connection_v2_without_default_route
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    dispatcher_send = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher",
        dispatcher_send,
    )
    activity_cache = mocker.MagicMock()
    activity_cache.set.return_value = async_return(True)
    mocker.patch("app.services.activity_logger._cache_db", activity_cache)
    activity_publish = mocker.patch(
        "app.services.activity_logger.send_event_to_integration_events_topic",
        return_value=async_return(None),
    )

    # Must not raise (raising makes PubSub retry the message).
    await process_observation_event(raw_observation_v2, raw_observation_v2_attributes)

    dispatcher_send.assert_not_called()
    mock_gundi_client_v2.get_route_details.assert_not_called()
    activity_publish.assert_called_once()
    payload = activity_publish.call_args.args[0].payload
    assert str(payload.integration_id) == provider_id
    assert payload.data["reason"] == "missing_default_route"
    assert payload.data["gundi_ids"] == [raw_observation_v2["payload"]["gundi_id"]]


@pytest.mark.asyncio
async def test_observation_is_retried_when_destination_integration_lookup_fails(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    connection_v2,
    raw_observation_v2,
    raw_observation_v2_attributes,
):
    """A transient portal failure (timeout, 5xx) makes get_integration return
    None. That must surface as a ReferenceDataError so PubSub retries the
    message once the portal recovers, not as an AttributeError on `.additional`.
    """
    from app.core.errors import ReferenceDataError

    destination_id = str(connection_v2.destinations[0].id)
    raw_observation_v2["payload"]["data_provider_id"] = str(connection_v2.provider.id)
    mock_gundi_client_v2.get_integration_details.side_effect = TimeoutError("portal read timeout")
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    dispatcher_send = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher",
        dispatcher_send,
    )
    activity_cache = mocker.MagicMock()
    activity_cache.set.return_value = async_return(True)
    mocker.patch("app.services.activity_logger._cache_db", activity_cache)
    activity_publish = mocker.patch(
        "app.services.activity_logger.send_event_to_integration_events_topic",
        return_value=async_return(None),
    )

    with pytest.raises(ReferenceDataError) as excinfo:
        await process_observation_event(raw_observation_v2, raw_observation_v2_attributes)

    assert destination_id in str(excinfo.value)
    dispatcher_send.assert_not_called()
    # The portal-lookup activity log still names the destination that failed.
    activity_publish.assert_called_once()
    payload = activity_publish.call_args.args[0].payload
    assert payload.action_id == "get_integration"
    assert str(payload.integration_id) == destination_id


@pytest.mark.asyncio
async def test_text_message_without_default_route_is_discarded_and_logged(
    mocker,
    connection_v2_er_to_inreach,
    text_message_from_earthranger,
):
    """The text-message stream through a connection with no default route.

    Same configuration error as the observation and batch cases, on the stream
    type that hit it in production (an ER -> inReach message). The message must
    be dropped without a route lookup or dispatch, and the discard must be
    reported for the provider with the message's stream type and gundi_id.
    Calls transform_and_route_observation directly so the check is independent
    of the PubSub envelope shape.
    """
    connection = connection_v2_er_to_inreach.copy(
        update={"default_route": None, "routing_rules": [], "destinations": []}
    )
    provider_id = str(connection.provider.id)
    mocker.patch(
        "app.services.event_handlers.get_connection",
        mocker.AsyncMock(return_value=connection),
    )
    mock_get_route = mocker.AsyncMock()
    mocker.patch("app.services.event_handlers.get_route", mock_get_route)
    dispatcher_send = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher",
        dispatcher_send,
    )
    activity_cache = mocker.MagicMock()
    activity_cache.set.return_value = async_return(True)
    mocker.patch("app.services.activity_logger._cache_db", activity_cache)
    activity_publish = mocker.patch(
        "app.services.activity_logger.send_event_to_integration_events_topic",
        return_value=async_return(None),
    )

    # Must not raise (raising makes PubSub retry a message that can never succeed).
    await transform_and_route_observation(observation=text_message_from_earthranger)

    mock_get_route.assert_not_called()
    dispatcher_send.assert_not_called()
    activity_publish.assert_called_once()
    payload = activity_publish.call_args.args[0].payload
    assert str(payload.integration_id) == provider_id
    assert payload.action_id == "route_observation"
    assert payload.data["reason"] == "missing_default_route"
    assert payload.data["observation_type"] == "txt"
    assert payload.data["discarded_count"] == 1
    assert payload.data["gundi_ids"] == [str(text_message_from_earthranger.gundi_id)]
