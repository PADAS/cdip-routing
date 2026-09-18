"""Tests for the observations batch branch in event_handlers.

An ObservationsBatchReceived envelope is transformed per item, grouped per
(destination, effective provider_key), and published as one
ObservationsBatchTransformedER message per group.
"""

import copy
import json
import uuid

import pytest

from app.conftest import async_return
from app.core.errors import ReferenceDataError
from app.services.process_messages import process_observation_event


def _make_batch_event_dict(observations_count=3, data_provider_id=None):
    data_provider_id = data_provider_id or "f870e228-4a65-40f0-888c-41bdc1124c3c"
    observations = [
        {
            "gundi_id": str(uuid.uuid4()),
            "data_provider_id": data_provider_id,
            "source_id": str(uuid.uuid4()),
            "external_source_id": f"device-{i}",
            "recorded_at": f"2026-07-22 11:5{i}:05+00:00",
            "location": {"lon": -72.7, "lat": -51.6},
            "observation_type": "obv",
        }
        for i in range(observations_count)
    ]
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": "2026-07-29 13:23:43.952056+00:00",
        "schema_version": "v1",
        "event_type": "ObservationsBatchReceived",
        "payload": {
            "batch_id": str(uuid.uuid4()),
            "data_provider_id": data_provider_id,
            "stream_type": "obv",
            "observations": observations,
        },
    }


def _batch_attributes(count):
    return {
        "observation_type": "obv",
        "gundi_version": "v2",
        "batch": "true",
        "batch_count": str(count),
        "tracing_context": "{}",
    }


def _decode_published_payload(send_mock, call_index=0):
    call_kwargs = send_mock.call_args_list[call_index][1]
    return json.loads(call_kwargs["message"].decode("utf-8")), call_kwargs


@pytest.mark.asyncio
async def test_batch_publishes_one_transformed_envelope_per_destination(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    destination_integration_v2,
    connection_v2,
    route_v2,
):
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    send_mock = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher", send_mock
    )

    event_dict = _make_batch_event_dict(
        observations_count=3,
        data_provider_id=str(connection_v2.provider.id),
    )
    await process_observation_event(event_dict, _batch_attributes(3))

    # ONE publish for the whole batch (single ER destination in connection_v2)
    assert send_mock.call_count == 1
    payload, call_kwargs = _decode_published_payload(send_mock)
    assert payload["event_type"] == "ObservationsBatchTransformedER"
    assert len(payload["payload"]["items"]) == 3
    assert payload["payload"]["provider_key"]
    attrs = call_kwargs["attributes"]
    assert attrs["batch"] == "true"
    assert attrs["batch_count"] == "3"
    assert attrs["stream_type"] == "obv"
    assert attrs["destination_id"]
    # Every item pairs a gundi_id with the transformed ER observation
    source_gundi_ids = {o["gundi_id"] for o in event_dict["payload"]["observations"]}
    item_gundi_ids = {i["gundi_id"] for i in payload["payload"]["items"]}
    assert item_gundi_ids == source_gundi_ids


@pytest.mark.asyncio
async def test_batch_shrinks_on_transform_failure(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    destination_integration_v2,
    connection_v2,
    route_v2,
):
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    send_mock = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher", send_mock
    )
    real_transform = mocker.patch("app.services.event_handlers.transform_observation_v2")
    # Middle item fails to transform; batch must shrink, not abort
    # NOTE: `transform_observation_v2` is an `async def`, so `mocker.patch`
    # auto-creates an AsyncMock (Python 3.8+ unittest.mock behavior). An
    # AsyncMock's `side_effect` list values are returned directly as the
    # awaited result — wrapping them in `async_return` (an already-resolved
    # Future) would make the *Future itself* the "transformed" value instead
    # of unwrapping it, which breaks the ERObservation isinstance check below.
    from gundi_core.schemas.v2 import ERObservation
    ok_obs = ERObservation(
        manufacturer_id="device-ok",
        recorded_at="2026-07-22 11:51:05+00:00",
        location={"lon": -72.7, "lat": -51.6},
    )
    real_transform.side_effect = [
        ok_obs,
        Exception("boom"),
        ok_obs,
    ]

    event_dict = _make_batch_event_dict(
        observations_count=3,
        data_provider_id=str(connection_v2.provider.id),
    )
    await process_observation_event(event_dict, _batch_attributes(3))

    assert send_mock.call_count == 1
    payload, _ = _decode_published_payload(send_mock)
    assert len(payload["payload"]["items"]) == 2


@pytest.mark.asyncio
async def test_batch_with_all_items_failing_publishes_nothing(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    destination_integration_v2,
    connection_v2,
    route_v2,
):
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    send_mock = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher", send_mock
    )
    mocker.patch(
        "app.services.event_handlers.transform_observation_v2",
        side_effect=Exception("boom"),
    )

    event_dict = _make_batch_event_dict(
        observations_count=2,
        data_provider_id=str(connection_v2.provider.id),
    )
    await process_observation_event(event_dict, _batch_attributes(2))

    send_mock.assert_not_called()


@pytest.mark.asyncio
async def test_batch_raises_on_unsupported_broker(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    destination_integration_v2,
    connection_v2,
    route_v2,
):
    # A destination configured with a legacy/unsupported broker must abort the
    # whole batch (ReferenceDataError, so the envelope is retried) before any
    # transform or publish work happens for that destination — same as the
    # single-item path and the generic-model publish path already do per item.
    unsupported_broker_integration = copy.deepcopy(destination_integration_v2)
    unsupported_broker_integration.additional = {
        **(unsupported_broker_integration.additional or {}),
        "broker": "kafka",
    }
    mock_gundi_client_v2.get_integration_details.return_value = async_return(
        unsupported_broker_integration
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    send_mock = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher", send_mock
    )
    transform_mock = mocker.patch("app.services.event_handlers.transform_observation_v2")

    event_dict = _make_batch_event_dict(
        observations_count=2,
        data_provider_id=str(connection_v2.provider.id),
    )

    with pytest.raises(ReferenceDataError):
        await process_observation_event(event_dict, _batch_attributes(2))

    transform_mock.assert_not_called()
    send_mock.assert_not_called()


@pytest.mark.asyncio
async def test_batch_without_default_route_is_discarded_and_logged(
    mocker,
    mock_cache,
    mock_gundi_client_v2,
    connection_v2_without_default_route,
):
    """Same configuration error as the single-item path, for a whole batch:
    drop the envelope, skip the null route lookup, emit one activity log
    listing every discarded gundi_id."""
    provider_id = str(connection_v2_without_default_route.provider.id)
    mock_gundi_client_v2.get_connection_details.return_value = async_return(
        connection_v2_without_default_route
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    send_mock = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher", send_mock
    )
    activity_cache = mocker.MagicMock()
    activity_cache.set.return_value = async_return(True)
    mocker.patch("app.services.activity_logger._cache_db", activity_cache)
    activity_publish = mocker.patch(
        "app.services.activity_logger.send_event_to_integration_events_topic",
        return_value=async_return(None),
    )
    event_dict = _make_batch_event_dict(observations_count=3, data_provider_id=provider_id)
    expected_gundi_ids = [o["gundi_id"] for o in event_dict["payload"]["observations"]]

    # Must not raise (raising makes PubSub retry the whole envelope).
    await process_observation_event(event_dict, _batch_attributes(3))

    send_mock.assert_not_called()
    mock_gundi_client_v2.get_route_details.assert_not_called()
    activity_publish.assert_called_once()
    payload = activity_publish.call_args.args[0].payload
    assert str(payload.integration_id) == provider_id
    assert payload.data["reason"] == "missing_default_route"
    assert payload.data["discarded_count"] == 3
    assert payload.data["gundi_ids"] == expected_gundi_ids
