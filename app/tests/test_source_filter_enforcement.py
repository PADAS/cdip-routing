"""Device routing filters (GUNDI-5712): the rule is evaluated at the top of the
destination fan-out, before any portal lookup or transform for that destination,
and only an explicit allows() == False drops — everything else fails open.
Each drop publishes ObservationFiltered to the routing-events topic, which is
what marks the GundiTrace in the portal (GUNDI-5711's consumer).
"""

import json
import uuid

import pytest

from app.conftest import async_return
from app.services.process_messages import process_observation_event
from gundi_core.schemas import v2 as schemas_v2


def _route_with_filter(route, destination_id, provider_id, *, mode, ids, enabled=True):
    return schemas_v2.Route.parse_obj(
        {
            **json.loads(route.json()),
            "filters": {
                str(destination_id): {
                    "type": "list",
                    "mode": mode,
                    "enabled": enabled,
                    "by_provider": {str(provider_id): ids},
                }
            },
        }
    )


def _observation_event_dict(data_provider_id, external_source_id):
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": "2026-07-29 13:23:43.952056+00:00",
        "schema_version": "v1",
        "event_type": "ObservationReceived",
        "payload": {
            "gundi_id": str(uuid.uuid4()),
            "data_provider_id": data_provider_id,
            "source_id": str(uuid.uuid4()),
            "external_source_id": external_source_id,
            "recorded_at": "2026-07-22 11:51:05+00:00",
            "location": {"lon": -72.7, "lat": -51.6},
            "observation_type": "obv",
        },
    }


_ATTRIBUTES = {
    "observation_type": "obv",
    "gundi_version": "v2",
    "tracing_context": "{}",
}


def _batch_event_dict(data_provider_id, external_source_ids):
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": "2026-07-29 13:23:43.952056+00:00",
        "schema_version": "v1",
        "event_type": "ObservationsBatchReceived",
        "payload": {
            "batch_id": str(uuid.uuid4()),
            "data_provider_id": data_provider_id,
            "stream_type": "obv",
            "observations": [
                {
                    "gundi_id": str(uuid.uuid4()),
                    "data_provider_id": data_provider_id,
                    "source_id": str(uuid.uuid4()),
                    "external_source_id": external_id,
                    "recorded_at": f"2026-07-22 11:5{i}:05+00:00",
                    "location": {"lon": -72.7, "lat": -51.6},
                    "observation_type": "obv",
                }
                for i, external_id in enumerate(external_source_ids)
            ],
        },
    }


@pytest.fixture
def enforcement_mocks(mocker, mock_cache, mock_gundi_client_v2):
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger._cache_db", mock_cache)
    mocker.patch("app.core.deduplication._cache_db", mock_cache)
    dispatcher = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher", dispatcher
    )
    routing_events = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_event_to_routing_events_topic",
        routing_events,
    )
    return dispatcher, routing_events


def _enable(mocker):
    mocker.patch("app.core.settings.ROUTING_FILTERS_ENABLED", True)


@pytest.mark.asyncio
async def test_flag_off_delivers_even_when_a_rule_would_drop(
    mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    dispatcher, routing_events = enforcement_mocks
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, destination_id, provider_id, mode="whitelist", ids=[]
        )
    )

    await process_observation_event(
        _observation_event_dict(provider_id, "collar-001"), _ATTRIBUTES
    )

    assert dispatcher.call_count == 1
    assert routing_events.call_count == 0


@pytest.mark.asyncio
async def test_whitelist_drops_an_unlisted_device_and_publishes_the_event(
    mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, destination_id, provider_id, mode="whitelist", ids=["collar-A"]
        )
    )
    event = _observation_event_dict(provider_id, "collar-001")

    await process_observation_event(event, _ATTRIBUTES)

    # Nothing reached the dispatcher for this destination; the drop record went
    # to the routing-events topic instead.
    assert dispatcher.call_count == 0
    assert routing_events.call_count == 1
    filtered = routing_events.call_args[0][0]
    assert str(filtered.payload.gundi_id) == event["payload"]["gundi_id"]
    assert str(filtered.payload.destination_id) == destination_id
    assert filtered.payload.external_source_id == "collar-001"
    assert filtered.payload.filtered_by == "device_whitelist"


@pytest.mark.asyncio
async def test_whitelist_delivers_a_listed_device(
    mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, destination_id, provider_id, mode="whitelist", ids=["collar-001"]
        )
    )

    await process_observation_event(
        _observation_event_dict(provider_id, "collar-001"), _ATTRIBUTES
    )

    assert dispatcher.call_count == 1
    assert routing_events.call_count == 0


@pytest.mark.asyncio
async def test_blacklist_drops_only_the_listed_device(
    mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, destination_id, provider_id, mode="blacklist", ids=["collar-001"]
        )
    )

    await process_observation_event(
        _observation_event_dict(provider_id, "collar-001"), _ATTRIBUTES
    )
    await process_observation_event(
        _observation_event_dict(provider_id, "collar-002"), _ATTRIBUTES
    )

    assert dispatcher.call_count == 1  # only collar-002 got through
    assert routing_events.call_count == 1
    assert routing_events.call_args[0][0].payload.filtered_by == "device_blacklist"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rule_overrides",
    [
        # A rule that does not name this provider leaves its feed untouched
        # (decision 9) ...
        {"by_provider": {str(uuid.uuid4()): ["collar-001"]}},
        # ... a paused rule never speaks ...
        {"enabled": False},
        # ... and an unknown mode from a newer producer fails open.
        {"mode": "graylist"},
    ],
)
async def test_fail_open_paths_deliver(
    mocker,
    enforcement_mocks,
    mock_gundi_client_v2,
    connection_v2,
    route_v2,
    rule_overrides,
):
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    rule = {
        "type": "list",
        "mode": "whitelist",
        "enabled": True,
        "by_provider": {provider_id: []},
        **rule_overrides,
    }
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        schemas_v2.Route.parse_obj(
            {**json.loads(route_v2.json()), "filters": {destination_id: rule}}
        )
    )

    await process_observation_event(
        _observation_event_dict(provider_id, "collar-001"), _ATTRIBUTES
    )

    assert dispatcher.call_count == 1
    assert routing_events.call_count == 0


@pytest.mark.asyncio
async def test_a_provider_named_with_zero_devices_ships_closed(
    mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    # {P: []} is a whitelist that allows nothing — the cascade-emptied state
    # must keep restricting instead of silently reopening the feed.
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, destination_id, provider_id, mode="whitelist", ids=[]
        )
    )

    await process_observation_event(
        _observation_event_dict(provider_id, "collar-001"), _ATTRIBUTES
    )

    assert dispatcher.call_count == 0
    assert routing_events.call_count == 1


@pytest.mark.asyncio
async def test_a_failed_event_publish_never_retries_the_envelope(
    mocker,
    enforcement_mocks,
    mock_gundi_client_v2,
    connection_v2,
    route_v2,
    mock_pubsub,
):
    # The publisher is best-effort by contract: the observation is already
    # dropped by policy, and raising here would re-deliver it everywhere else.
    # Exercise the REAL publisher wrapper with a failing pubsub client.
    dispatcher, _ = enforcement_mocks
    _enable(mocker)
    from app.core.pubsub import send_event_to_routing_events_topic

    mocker.patch(
        "app.services.event_handlers.send_event_to_routing_events_topic",
        send_event_to_routing_events_topic,
    )
    mock_pubsub.PublisherClient.return_value.publish.side_effect = Exception("boom")
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, destination_id, provider_id, mode="whitelist", ids=[]
        )
    )

    # No exception may escape: the message must be acked, not retried.
    await process_observation_event(
        _observation_event_dict(provider_id, "collar-001"), _ATTRIBUTES
    )

    assert dispatcher.call_count == 0


@pytest.mark.asyncio
async def test_batch_shrinks_to_the_allowed_items(
    mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2,
            destination_id,
            provider_id,
            mode="whitelist",
            ids=["device-0", "device-2"],
        )
    )
    attributes = {**_ATTRIBUTES, "batch": "true", "batch_count": "3"}

    await process_observation_event(
        _batch_event_dict(provider_id, ["device-0", "device-1", "device-2"]),
        attributes,
    )

    # One grouped publish carrying only the two allowed items; the dropped one
    # produced its ObservationFiltered instead of aborting the batch.
    assert dispatcher.call_count == 1
    payload = json.loads(dispatcher.call_args[1]["message"].decode("utf-8"))
    assert len(payload["payload"]["items"]) == 2
    assert routing_events.call_count == 1
    assert routing_events.call_args[0][0].payload.external_source_id == "device-1"
