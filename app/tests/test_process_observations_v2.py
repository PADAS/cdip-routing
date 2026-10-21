import pytest

from app.conftest import async_return
from app.services.event_handlers import transform_and_route_observation
from app.services.process_messages import event_schemas, process_observation_event
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
    mock_gundi_client_v2.get_integration_details.side_effect = TimeoutError(
        "portal read timeout"
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

    with pytest.raises(ReferenceDataError) as excinfo:
        await process_observation_event(
            raw_observation_v2, raw_observation_v2_attributes
        )

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


BROKEN_DESTINATION_ID = "0eecad72-1d2e-43b1-a169-c419c81fc957"


def _two_destination_setup(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    connection_v2,
    destination_integration_v2,
    broken_state=None,
):
    """connection_v2 with a second destination; transform_observation_v2 raises
    a ReferenceDataError for that one (while broken_state["fails"]) and runs for
    real for the other."""
    from app.core.errors import ReferenceDataError
    from app.services.transformers import transform_observation_v2

    broken_state = broken_state if broken_state is not None else {"fails": True}

    healthy = connection_v2.destinations[0]
    broken = healthy.copy(update={"id": BROKEN_DESTINATION_ID, "name": "Broken"})
    connection = connection_v2.copy(update={"destinations": [broken, healthy]})
    integrations = {
        str(healthy.id): destination_integration_v2,
        BROKEN_DESTINATION_ID: destination_integration_v2.copy(
            update={"id": BROKEN_DESTINATION_ID}
        ),
    }
    mock_gundi_client_v2.get_connection_details.return_value = async_return(connection)
    mock_gundi_client_v2.get_integration_details.side_effect = (
        lambda integration_id: async_return(integrations[str(integration_id)])
    )

    async def transform(*, observation, destination, provider, route_configuration):
        if str(destination.id) == BROKEN_DESTINATION_ID and broken_state["fails"]:
            raise ReferenceDataError("Error getting data model for SMART CA: abc")
        return await transform_observation_v2(
            observation=observation,
            destination=destination,
            provider=provider,
            route_configuration=route_configuration,
        )

    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    transform_mock = mocker.patch(
        "app.services.event_handlers.transform_observation_v2", side_effect=transform
    )
    return connection, healthy, transform_mock


def _patch_activity_log(mocker):
    activity_cache = mocker.MagicMock()
    activity_cache.set.return_value = async_return(True)
    mocker.patch("app.services.activity_logger._cache_db", activity_cache)
    return mocker.patch(
        "app.services.activity_logger.send_event_to_integration_events_topic",
        return_value=async_return(None),
    )


@pytest.mark.asyncio
async def test_event_is_retried_when_transformer_raises_reference_data_error(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    connection_v2,
    destination_integration_v2,
    raw_event_v2,
    raw_event_v2_attributes,
):
    """A ReferenceDataError from a transformer (e.g. the SMART data model can't
    be fetched) must reach PubSub so the message is retried, not be discarded.
    The other destination is still delivered before the retry is signalled."""
    from app.core.errors import ReferenceDataError

    connection, healthy, _ = _two_destination_setup(
        mocker,
        mock_cache,
        mock_gundi_client_v2,
        connection_v2,
        destination_integration_v2,
    )
    dispatcher_send = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher",
        dispatcher_send,
    )
    activity_publish = _patch_activity_log(mocker)

    with pytest.raises(ReferenceDataError):
        await process_observation_event(raw_event_v2, raw_event_v2_attributes)

    assert [
        str(c.kwargs["destination"].id) for c in dispatcher_send.call_args_list
    ] == [str(healthy.id)]
    activity_publish.assert_called_once()
    payload = activity_publish.call_args.args[0].payload
    assert str(payload.integration_id) == str(connection.provider.id)
    assert payload.action_id == "transform_observation"
    assert payload.data["reason"] == "transform_error"
    assert payload.data["destination_id"] == BROKEN_DESTINATION_ID
    assert payload.data["error_type"] == "ReferenceDataError"
    assert payload.data["will_retry"] is True
    assert payload.data["gundi_ids"] == [raw_event_v2["payload"]["gundi_id"]]


@pytest.mark.asyncio
async def test_event_retry_does_not_republish_to_completed_destinations(
    mocker,
    mock_cache,
    mock_deduplication_cache_stateful,
    mock_gundi_client_v2,
    connection_v2,
    destination_integration_v2,
    raw_event_v2,
    raw_event_v2_attributes,
):
    """Redelivering the same envelope after a ReferenceDataError skips the
    destination that was already published and only retries the failed one."""
    from app.core.deduplication import EventProcessingStatus, get_event_status_key
    from app.core.errors import ReferenceDataError

    broken_state = {"fails": True}
    _, healthy, transform_mock = _two_destination_setup(
        mocker,
        mock_cache,
        mock_gundi_client_v2,
        connection_v2,
        destination_integration_v2,
        broken_state=broken_state,
    )
    mocker.patch("app.core.deduplication._cache_db", mock_deduplication_cache_stateful)
    dispatcher_send = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher",
        dispatcher_send,
    )
    _patch_activity_log(mocker)

    def published_to(destination_id):
        return [
            c
            for c in dispatcher_send.call_args_list
            if str(c.kwargs["destination"].id) == destination_id
        ]

    def broken_transform_attempts():
        return [
            c
            for c in transform_mock.call_args_list
            if str(c.kwargs["destination"].id) == BROKEN_DESTINATION_ID
        ]

    status_key = get_event_status_key(raw_event_v2["event_id"])

    for attempt in (1, 2):
        with pytest.raises(ReferenceDataError):
            await process_observation_event(raw_event_v2, raw_event_v2_attributes)
        assert len(published_to(str(healthy.id))) == 1
        assert published_to(BROKEN_DESTINATION_ID) == []
        assert len(broken_transform_attempts()) == attempt
        assert status_key not in mock_deduplication_cache_stateful.store

    broken_state["fails"] = False
    await process_observation_event(raw_event_v2, raw_event_v2_attributes)

    assert len(published_to(str(healthy.id))) == 1
    assert len(published_to(BROKEN_DESTINATION_ID)) == 1
    assert mock_deduplication_cache_stateful.store[status_key] == str(
        EventProcessingStatus.PROCESSED.value
    )


@pytest.mark.asyncio
async def test_destination_marker_read_error_is_treated_as_not_complete(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    raw_event_v2,
    raw_event_v2_attributes,
):
    """A Redis failure reading a completion marker must never drop data: the
    destination is published (worst case a duplicate) and nothing raises."""
    from redis.exceptions import RedisError

    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    dedup_cache = mocker.MagicMock()
    dedup_cache.get = mocker.AsyncMock(side_effect=RedisError("connection lost"))
    dedup_cache.setex = mocker.AsyncMock(side_effect=RedisError("connection lost"))
    mocker.patch("app.core.deduplication._cache_db", dedup_cache)
    # Skips backoff's 10s retry window, which would otherwise stall the test.
    no_retry = mocker.MagicMock()
    no_retry.on_exception.return_value = lambda f: f
    mocker.patch("app.core.deduplication.backoff", no_retry)
    dispatcher_send = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher",
        dispatcher_send,
    )

    await process_observation_event(raw_event_v2, raw_event_v2_attributes)

    assert dedup_cache.get.called
    dispatcher_send.assert_called_once()


@pytest.mark.asyncio
async def test_direct_call_without_event_id_does_not_touch_destination_markers(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    raw_event_v2,
):
    from gundi_core.events import EventReceived

    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    dedup_cache = mocker.MagicMock()
    mocker.patch("app.core.deduplication._cache_db", dedup_cache)
    dispatcher_send = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher",
        dispatcher_send,
    )

    await transform_and_route_observation(
        observation=EventReceived.parse_obj(raw_event_v2).payload, event_id=None
    )

    dispatcher_send.assert_called_once()
    dedup_cache.get.assert_not_called()
    dedup_cache.setex.assert_not_called()


@pytest.mark.asyncio
async def test_event_is_discarded_and_logged_on_permanent_transformer_error(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    connection_v2,
    raw_event_v2,
    raw_event_v2_attributes,
):
    """Any other transformer error can't be fixed by a retry: the destination's
    item is discarded, but the discard is reported in the portal."""
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    mocker.patch(
        "app.services.event_handlers.transform_observation_v2",
        side_effect=ValueError("bad location"),
    )
    dispatcher_send = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher",
        dispatcher_send,
    )
    activity_publish = _patch_activity_log(mocker)

    # Must not raise (raising makes PubSub retry a message that can never succeed).
    await process_observation_event(raw_event_v2, raw_event_v2_attributes)

    dispatcher_send.assert_not_called()
    activity_publish.assert_called_once()
    payload = activity_publish.call_args.args[0].payload
    assert str(payload.integration_id) == str(connection_v2.provider.id)
    assert payload.action_id == "transform_observation"
    assert payload.data["destination_id"] == str(connection_v2.destinations[0].id)
    assert payload.data["error_type"] == "ValueError"
    assert payload.data["error_message"] == "bad location"
    assert payload.data["will_retry"] is False
    assert payload.data["discarded_count"] == 1


@pytest.mark.asyncio
async def test_event_type_without_schema_is_discarded(
    mocker, caplog, raw_event_v2, raw_event_v2_attributes
):
    """Raising here would 500 and Pub/Sub would redeliver a message that can
    never be parsed, until it's too old."""
    handler = mocker.AsyncMock()
    mocker.patch.dict(
        "app.services.process_messages.event_handlers", {"EventReceived": handler}
    )
    mocker.patch.dict(event_schemas)
    del event_schemas["EventReceived"]

    with caplog.at_level("WARNING"):
        result = await process_observation_event(raw_event_v2, raw_event_v2_attributes)

    assert result is None
    handler.assert_not_called()
    assert not [r for r in caplog.records if r.levelname == "ERROR"]
    assert any("Message discarded" in r.getMessage() for r in caplog.records)
