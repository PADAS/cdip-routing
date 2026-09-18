import asyncio

import pytest
from gundi_core.events.integrations import LogLevel

from app.services import activity_logger
from app.services.activity_logger import (
    _dedup_key,
    log_missing_default_route,
    log_portal_lookup_error,
)


def async_return(value):
    f = asyncio.Future()
    f.set_result(value)
    return f


@pytest.fixture
def mock_dedup_cache(mocker):
    """Cache where the dedup key has not been seen yet (set with nx=True returns truthy)."""
    cache = mocker.MagicMock()
    cache.set.return_value = async_return(True)
    return cache


@pytest.fixture
def mock_dedup_cache_already_seen(mocker):
    """Cache where the dedup key already exists (set with nx=True returns None)."""
    cache = mocker.MagicMock()
    cache.set.return_value = async_return(None)
    return cache


@pytest.mark.asyncio
async def test_logs_first_time_failure(mocker, mock_dedup_cache):
    mocker.patch.object(activity_logger, "_cache_db", mock_dedup_cache)
    mock_publish = mocker.patch.object(
        activity_logger,
        "send_event_to_integration_events_topic",
        return_value=async_return(None),
    )

    await log_portal_lookup_error(
        action_id="get_connection",
        resource_id="abc-123",
        exception=ValueError("bad slug"),
    )

    mock_publish.assert_called_once()
    event = mock_publish.call_args.args[0]
    assert event.payload.action_id == "get_connection"
    assert str(event.payload.integration_id) == "abc-123"
    assert event.payload.data["error_type"] == "ValueError"
    assert event.payload.data["error_message"] == "bad slug"


@pytest.mark.asyncio
async def test_dedup_within_ttl_does_not_publish(
    mocker, mock_dedup_cache_already_seen
):
    mocker.patch.object(activity_logger, "_cache_db", mock_dedup_cache_already_seen)
    mock_publish = mocker.patch.object(
        activity_logger,
        "send_event_to_integration_events_topic",
        return_value=async_return(None),
    )

    await log_portal_lookup_error(
        action_id="get_connection",
        resource_id="abc-123",
        exception=ValueError("bad slug"),
    )

    mock_publish.assert_not_called()


@pytest.mark.asyncio
async def test_different_error_signature_logs_independently(
    mocker, mock_dedup_cache
):
    mocker.patch.object(activity_logger, "_cache_db", mock_dedup_cache)
    mocker.patch.object(
        activity_logger,
        "send_event_to_integration_events_topic",
        return_value=async_return(None),
    )

    key1 = _dedup_key("get_connection", "abc-123", ValueError("bad slug"))
    key2 = _dedup_key("get_connection", "abc-123", ValueError("missing field foo"))
    assert key1 != key2

    key3 = _dedup_key("get_connection", "abc-123", TypeError("bad slug"))
    assert key1 != key3


@pytest.mark.asyncio
async def test_publish_failure_is_swallowed(mocker, mock_dedup_cache):
    mocker.patch.object(activity_logger, "_cache_db", mock_dedup_cache)
    mocker.patch.object(
        activity_logger,
        "send_event_to_integration_events_topic",
        side_effect=RuntimeError("pubsub down"),
    )

    # Must not raise.
    await log_portal_lookup_error(
        action_id="get_connection",
        resource_id="abc-123",
        exception=ValueError("bad slug"),
    )


@pytest.mark.asyncio
async def test_redis_failure_is_swallowed(mocker):
    failing_cache = mocker.MagicMock()
    failing_cache.set.side_effect = RuntimeError("redis down")
    mocker.patch.object(activity_logger, "_cache_db", failing_cache)
    mock_publish = mocker.patch.object(
        activity_logger,
        "send_event_to_integration_events_topic",
        return_value=async_return(None),
    )

    # Must not raise.
    await log_portal_lookup_error(
        action_id="get_connection",
        resource_id="abc-123",
        exception=ValueError("bad slug"),
    )
    mock_publish.assert_not_called()


@pytest.mark.asyncio
async def test_missing_default_route_publishes_error_log_for_provider(
    mocker, mock_dedup_cache, connection_v2_without_default_route
):
    mocker.patch.object(activity_logger, "_cache_db", mock_dedup_cache)
    mock_publish = mocker.patch.object(
        activity_logger,
        "send_event_to_integration_events_topic",
        return_value=async_return(None),
    )

    await log_missing_default_route(
        connection=connection_v2_without_default_route,
        observation_type="obv",
        gundi_ids=["9573c2b0-3fd7-4502-884b-43d5628ce7a8"],
    )

    mock_publish.assert_called_once()
    payload = mock_publish.call_args.args[0].payload
    assert str(payload.integration_id) == "ddd0946d-15b0-4308-b93d-e0470b6d33b6"
    assert payload.action_id == "route_observation"
    assert payload.level == LogLevel.ERROR
    assert payload.data["reason"] == "missing_default_route"
    assert payload.data["provider"] == "Trap Tagger"
    assert payload.data["owner"] == "Test Organization"
    assert payload.data["destinations"] == []
    assert payload.data["routing_rules"] == []
    assert payload.data["observation_type"] == "obv"
    assert payload.data["discarded_count"] == 1
    assert payload.data["gundi_ids"] == ["9573c2b0-3fd7-4502-884b-43d5628ce7a8"]


@pytest.mark.asyncio
async def test_missing_default_route_is_deduped_per_provider(
    mocker, mock_dedup_cache_already_seen, connection_v2_without_default_route
):
    mocker.patch.object(activity_logger, "_cache_db", mock_dedup_cache_already_seen)
    mock_publish = mocker.patch.object(
        activity_logger,
        "send_event_to_integration_events_topic",
        return_value=async_return(None),
    )

    await log_missing_default_route(
        connection=connection_v2_without_default_route,
        observation_type="obv",
        gundi_ids=["9573c2b0-3fd7-4502-884b-43d5628ce7a8"],
    )

    mock_publish.assert_not_called()
    dedup_key = mock_dedup_cache_already_seen.set.call_args.args[0]
    assert "ddd0946d-15b0-4308-b93d-e0470b6d33b6" in dedup_key


@pytest.mark.asyncio
async def test_missing_default_route_publish_failure_is_swallowed(
    mocker, mock_dedup_cache, connection_v2_without_default_route
):
    mocker.patch.object(activity_logger, "_cache_db", mock_dedup_cache)
    mocker.patch.object(
        activity_logger,
        "send_event_to_integration_events_topic",
        side_effect=RuntimeError("pubsub down"),
    )

    # Must not raise.
    await log_missing_default_route(
        connection=connection_v2_without_default_route,
        observation_type="obv",
        gundi_ids=["9573c2b0-3fd7-4502-884b-43d5628ce7a8"],
    )
