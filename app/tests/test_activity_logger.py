import asyncio

import pytest

from app.services import activity_logger
from app.services.activity_logger import _dedup_key, log_portal_lookup_error


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
