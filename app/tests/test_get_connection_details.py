import asyncio
import pytest
from app.core.gundi import get_connection
from gundi_core import schemas


def _async_return(value):
    f = asyncio.Future()
    f.set_result(value)
    return f


@pytest.mark.asyncio
async def test_get_connection_from_cache(
    mocker, mock_cache_with_cached_connection, connection_v2, mock_gundi_client_v2
):
    # Mock external dependencies
    mocker.patch("app.core.gundi._cache_db", mock_cache_with_cached_connection)  # empty cache
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    connection = await get_connection(
        connection_id=str(connection_v2.id)
    )
    assert mock_cache_with_cached_connection.get.called
    assert not mock_gundi_client_v2.get_connection_details.called
    assert connection == connection_v2


@pytest.mark.asyncio
async def test_get_connection_from_gundi_on_cache_miss(
    mocker, mock_cache, connection_v2, mock_gundi_client_v2
):
    # Mock external dependencies
    mocker.patch("app.core.gundi._cache_db", mock_cache)  # faulty cache
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    connection = await get_connection(
        connection_id=str(connection_v2.id)
    )
    assert mock_cache.get.called
    mock_gundi_client_v2.get_connection_details.assert_called_once_with(integration_id=str(connection_v2.id))
    assert connection == connection_v2


@pytest.mark.asyncio
async def test_get_connection_from_gundi_on_cache_error(
    mocker, mock_cache_with_connection_error, connection_v2, mock_gundi_client_v2
):
    # Mock external dependencies
    mocker.patch("app.core.gundi._cache_db", mock_cache_with_connection_error)  # empty cache
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    connection = await get_connection(
        connection_id=str(connection_v2.id)
    )
    assert mock_cache_with_connection_error.get.called
    mock_gundi_client_v2.get_connection_details.assert_called_once_with(integration_id=str(connection_v2.id))
    assert connection == connection_v2


@pytest.mark.asyncio
async def test_portal_failure_emits_activity_log(
    mocker, mock_cache, connection_v2
):
    # Cache miss forces a portal call, portal raises a validation-style error.
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    failing_portal = mocker.MagicMock()
    failing_portal.get_connection_details.side_effect = ValueError("1 validation error for Connection")
    mocker.patch("app.core.gundi.portal_v2", failing_portal)
    mock_log = mocker.patch(
        "app.core.gundi.log_portal_lookup_error",
        return_value=_async_return(None),
    )

    connection = await get_connection(connection_id=str(connection_v2.id))

    assert connection is None
    mock_log.assert_called_once()
    kwargs = mock_log.call_args.kwargs
    assert kwargs["action_id"] == "get_connection"
    assert kwargs["resource_id"] == str(connection_v2.id)
    assert isinstance(kwargs["exception"], ValueError)

