import pytest
from app.transform_service.services import get_all_outbound_configs_for_id
from gundi_core import schemas


@pytest.mark.asyncio
async def test_get_all_outbound_configs_for_id(
    mocker, mock_cache, mock_gundi_client, outbound_integration_config_list
):
    # Mock external dependencies
    mocker.patch("app.transform_service.services._cache_db", mock_cache)
    mocker.patch("app.transform_service.services._portal", mock_gundi_client)
    destinations = await get_all_outbound_configs_for_id(
        inbound_id="12345b4f-88cd-49c4-a723-0ddff1f580c4", device_id="12345"
    )
    assert destinations == [
        schemas.OutboundConfiguration.parse_obj(conf)
        for conf in outbound_integration_config_list
    ]
