import pytest
from app.core.gundi import get_connection
from gundi_core import schemas


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
    mocker.patch("app.core.gundi._cache_db", mock_cache)  # empty cache
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

