import pytest
from app.subscribers.services import (
    get_inbound_integration_detail,
    get_outbound_config_detail,
)
from cdip_connector.core import schemas


@pytest.mark.asyncio
async def test_get_inbound_integration_detail(
    mocker, mock_cache, mock_gundi_client, inbound_integration_config
):
    # Mock external dependencies
    mocker.patch("app.subscribers.services._cache_db", mock_cache)
    mocker.patch("app.subscribers.services._portal", mock_gundi_client)
    integration_info = await get_inbound_integration_detail(
        integration_id="12345b4f-88cd-49c4-a723-0ddff1f580c4"
    )
    assert integration_info == schemas.IntegrationInformation.parse_obj(
        inbound_integration_config
    )


@pytest.mark.asyncio
async def test_get_outbound_config_detail(
    mocker, mock_cache, mock_gundi_client, outbound_integration_config
):
    # Mock external dependencies
    mocker.patch("app.subscribers.services._cache_db", mock_cache)
    mocker.patch("app.subscribers.services._portal", mock_gundi_client)
    integration_info = await get_outbound_config_detail(
        outbound_id="56785b4f-88cd-49c4-a723-0ddff1f580c4"
    )
    assert integration_info == schemas.OutboundConfiguration.parse_obj(
        outbound_integration_config
    )
