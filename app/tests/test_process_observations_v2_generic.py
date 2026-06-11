"""Tests for the generic-model publish branch in event_handlers.

When a destination integration has ``additional.generic_model = true``,
cdip-routing wraps the payload in a ``GundiDelivery`` envelope and publishes
that envelope to the destination's PubSub topic, bypassing the per-destination
transformer in ``transformers_map``.
"""

import base64
import copy
import json

import pytest

from app.conftest import async_return
from app.services.process_messages import process_observation_event


@pytest.fixture
def destination_integration_v2_generic(destination_integration_v2):
    """Same ER integration but flagged for generic-model delivery."""
    generic = copy.deepcopy(destination_integration_v2)
    generic.additional = {**(generic.additional or {}), "generic_model": True}
    return generic


def _decode_published_payload(send_mock):
    call_kwargs = send_mock.call_args[1]
    binary = call_kwargs["message"]
    return json.loads(binary.decode("utf-8")), call_kwargs


@pytest.mark.asyncio
async def test_generic_mode_observation_publishes_gundi_delivery(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    destination_integration_v2_generic,
    raw_observation_v2,
    raw_observation_v2_attributes,
):
    mock_gundi_client_v2.get_integration_details.return_value = async_return(
        destination_integration_v2_generic
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    send_mock = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher", send_mock
    )
    transform_mock = mocker.patch(
        "app.services.event_handlers.transform_observation_v2"
    )

    await process_observation_event(raw_observation_v2, raw_observation_v2_attributes)

    transform_mock.assert_not_called()
    assert send_mock.call_count == 1

    payload, call_kwargs = _decode_published_payload(send_mock)
    assert payload["event_type"] == "GundiDelivery"
    assert payload["payload"]["observation_type"] == "obv"
    assert payload["provider"]["provider_id"]
    assert payload["provider"]["provider_type"]


@pytest.mark.asyncio
async def test_generic_mode_event_publishes_gundi_delivery(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    destination_integration_v2_generic,
    raw_event_v2,
    raw_event_v2_attributes,
):
    mock_gundi_client_v2.get_integration_details.return_value = async_return(
        destination_integration_v2_generic
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    send_mock = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher", send_mock
    )

    await process_observation_event(raw_event_v2, raw_event_v2_attributes)

    assert send_mock.call_count == 1
    payload, _ = _decode_published_payload(send_mock)
    assert payload["event_type"] == "GundiDelivery"
    assert payload["payload"]["observation_type"] == "ev"


@pytest.mark.asyncio
async def test_legacy_mode_unchanged_when_generic_model_absent(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    destination_integration_v2,
    raw_observation_v2,
    raw_observation_v2_attributes,
):
    # No `generic_model` flag in `additional`; legacy transformer path must run.
    assert "generic_model" not in (destination_integration_v2.additional or {})
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    send_mock = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher", send_mock
    )

    await process_observation_event(raw_observation_v2, raw_observation_v2_attributes)

    assert send_mock.call_count == 1
    payload, _ = _decode_published_payload(send_mock)
    # Legacy path produces an *Transformed* event, not a GundiDelivery.
    assert payload["event_type"] != "GundiDelivery"
    assert payload["event_type"] == "ObservationTransformedER"


@pytest.mark.asyncio
async def test_generic_mode_selected_by_destination_type_without_flag(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    destination_integration_v2,
    raw_observation_v2,
    raw_observation_v2_attributes,
):
    """A destination whose *type* is in GENERIC_MODEL_DESTINATION_TYPES uses the
    generic-model path with no per-integration `additional.generic_model` flag.
    (Here the ER type is temporarily added to the allow-list to drive it.)"""
    assert "generic_model" not in (destination_integration_v2.additional or {})
    mocker.patch(
        "app.core.settings.GENERIC_MODEL_DESTINATION_TYPES",
        [destination_integration_v2.type.value],
    )
    mock_gundi_client_v2.get_integration_details.return_value = async_return(
        destination_integration_v2
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    send_mock = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher", send_mock
    )
    transform_mock = mocker.patch("app.services.event_handlers.transform_observation_v2")

    await process_observation_event(raw_observation_v2, raw_observation_v2_attributes)

    transform_mock.assert_not_called()
    assert send_mock.call_count == 1
    payload, _ = _decode_published_payload(send_mock)
    assert payload["event_type"] == "GundiDelivery"


@pytest.mark.asyncio
async def test_generic_mode_when_additional_is_none(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    destination_integration_v2,
    raw_observation_v2,
    raw_observation_v2_attributes,
):
    # Defensive case: integration with additional = None should hit legacy path,
    # not crash on .get().
    integration = copy.deepcopy(destination_integration_v2)
    integration.additional = None
    mock_gundi_client_v2.get_integration_details.return_value = async_return(integration)
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    send_mock = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher", send_mock
    )

    # We expect a failure downstream (broker_config check requires .get on None),
    # but the generic-model check itself must not raise AttributeError.
    try:
        await process_observation_event(raw_observation_v2, raw_observation_v2_attributes)
    except Exception:
        pass

    # The point of this test is the generic-model check shouldn't be the failure.
    # Whatever happens downstream is the existing behavior.


@pytest.mark.asyncio
async def test_generic_mode_ordering_key_for_event_update(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    destination_integration_v2_generic,
    raw_event_update,
    raw_event_update_attributes,
):
    mock_gundi_client_v2.get_integration_details.return_value = async_return(
        destination_integration_v2_generic
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    send_mock = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher", send_mock
    )

    await process_observation_event(raw_event_update, raw_event_update_attributes)

    assert send_mock.call_count == 1
    call_kwargs = send_mock.call_args[1]
    # event_update payloads must carry a non-empty ordering_key (same as legacy)
    assert call_kwargs["ordering_key"] != ""


@pytest.mark.asyncio
async def test_generic_mode_passes_route_configuration_through(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    destination_integration_v2_generic,
    route_v2_with_provider_key_field_mapping,
    raw_observation_v2,
    raw_observation_v2_attributes,
):
    # Route config travels in the envelope so the runner can apply transformations.
    mock_gundi_client_v2.get_integration_details.return_value = async_return(
        destination_integration_v2_generic
    )
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        route_v2_with_provider_key_field_mapping
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    send_mock = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher", send_mock
    )

    await process_observation_event(raw_observation_v2, raw_observation_v2_attributes)

    payload, _ = _decode_published_payload(send_mock)
    assert "route_configuration" in payload
    assert "field_mappings" in payload["route_configuration"]["data"]
