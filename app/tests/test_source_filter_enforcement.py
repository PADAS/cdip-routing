"""Device routing filters (GUNDI-5712): the rule is evaluated at the top of the
destination fan-out, before any portal lookup or transform for that destination,
and only an explicit allows() == False drops — everything else fails open.
Each drop publishes ObservationFiltered to the routing-events topic, which is
what marks the GundiTrace in the portal (GUNDI-5711's consumer).
"""

import json
import logging
import uuid

import pytest

from app.conftest import async_return
from app.core import settings
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
    # One mock for both publishers: the single-item path sends one event, the
    # batch path sends a destination's drops as one list.
    routing_events = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_event_to_routing_events_topic",
        routing_events,
    )
    mocker.patch(
        "app.services.event_handlers.send_events_to_routing_events_topic",
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
    mocker.patch("app.core.settings.ROUTING_FILTERS_ENABLED", False)
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
    # The gate runs first: a dropped destination costs no portal lookup.
    assert mock_gundi_client_v2.get_integration_details.call_count == 0


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
    events = routing_events.call_args[0][0]
    assert [e.payload.external_source_id for e in events] == ["device-1"]


@pytest.mark.asyncio
async def test_batch_drops_publish_their_events_in_one_call(
    mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    # One publish per dropped item, each with its own session and a 60s
    # timeout, can outlast the push ack deadline on a large batch and get the
    # allowed items redelivered; a destination's drops go out together.
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, destination_id, provider_id, mode="whitelist", ids=[]
        )
    )
    attributes = {**_ATTRIBUTES, "batch": "true", "batch_count": "3"}

    await process_observation_event(
        _batch_event_dict(provider_id, ["device-0", "device-1", "device-2"]),
        attributes,
    )

    assert dispatcher.call_count == 0
    assert routing_events.call_count == 1
    events = routing_events.call_args[0][0]
    assert [e.payload.external_source_id for e in events] == [
        "device-0",
        "device-1",
        "device-2",
    ]


@pytest.mark.asyncio
async def test_batch_generic_model_destination_delivers_only_allowed_items(
    mocker,
    enforcement_mocks,
    mock_gundi_client_v2,
    connection_v2,
    route_v2,
    destination_integration_v2,
):
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    generic = destination_integration_v2.copy(deep=True)
    generic.additional = {**(generic.additional or {}), "generic_model": True}
    mock_gundi_client_v2.get_integration_details.return_value = async_return(generic)
    publish_delivery = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers._publish_gundi_delivery", publish_delivery
    )
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

    delivered = [
        c[1]["observation"].external_source_id for c in publish_delivery.call_args_list
    ]
    assert delivered == ["device-0", "device-2"]
    assert dispatcher.call_count == 0
    assert routing_events.call_count == 1
    events = routing_events.call_args[0][0]
    assert [e.payload.external_source_id for e in events] == ["device-1"]


@pytest.mark.asyncio
async def test_a_drop_is_logged_at_info_without_attention_needed(
    caplog, mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    # Until the trace consumer is deployed the log line is the only place an
    # operator can see a drop, so it must be visible at the default level; a
    # working rule is not an operator problem, so it must not page anyone.
    _enable(mocker)
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, destination_id, provider_id, mode="blacklist", ids=["collar-001"]
        )
    )
    event = _observation_event_dict(provider_id, "collar-001")

    with caplog.at_level(logging.INFO, logger="app.services.event_handlers"):
        await process_observation_event(event, _ATTRIBUTES)

    drops = [r for r in caplog.records if "dropped" in r.getMessage()]
    assert len(drops) == 1
    record = drops[0]
    assert record.levelno == logging.INFO
    assert event["payload"]["gundi_id"] in record.getMessage()
    assert destination_id in record.getMessage()
    assert "collar-001" in record.getMessage()
    assert not getattr(record, "attention_needed", False)


@pytest.mark.asyncio
async def test_sourceless_observation_is_reported_without_a_device_id(
    mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    # The v2 schemas default external_source_id to the string "None"/"none",
    # not null. The drop record must not name a device called "None".
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, destination_id, provider_id, mode="whitelist", ids=["collar-001"]
        )
    )
    event = _observation_event_dict(provider_id, "unused")
    del event["payload"]["external_source_id"]

    await process_observation_event(event, _ATTRIBUTES)

    assert dispatcher.call_count == 0
    assert routing_events.call_count == 1
    assert routing_events.call_args[0][0].payload.external_source_id is None


@pytest.mark.asyncio
async def test_device_ids_compare_verbatim_with_the_rule(
    mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    # gundi-core's contract: ids compare verbatim against Source.external_id.
    # Only the exact schema defaults read as "no device"; a device really named
    # "NONE" must still be whitelistable.
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, destination_id, provider_id, mode="whitelist", ids=["NONE"]
        )
    )

    await process_observation_event(
        _observation_event_dict(provider_id, "NONE"), _ATTRIBUTES
    )

    assert dispatcher.call_count == 1
    assert routing_events.call_count == 0


@pytest.mark.asyncio
async def test_a_rule_drops_only_its_own_destination(
    mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    connection = connection_v2.copy(deep=True)
    connection.destinations.append(
        connection.destinations[0].copy(
            update={"id": uuid.uuid4(), "name": "Second ER"}
        )
    )
    filtered_id, open_id = connection.destinations[0].id, connection.destinations[1].id
    provider_id = str(connection.provider.id)
    mock_gundi_client_v2.get_connection_details.return_value = async_return(connection)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, filtered_id, provider_id, mode="blacklist", ids=["collar-001"]
        )
    )

    await process_observation_event(
        _observation_event_dict(provider_id, "collar-001"), _ATTRIBUTES
    )

    assert dispatcher.call_count == 1
    assert dispatcher.call_args[1]["destination"].id == open_id
    assert routing_events.call_count == 1
    assert str(routing_events.call_args[0][0].payload.destination_id) == str(
        filtered_id
    )


@pytest.mark.asyncio
async def test_a_generic_model_destination_is_filtered_too(
    mocker,
    enforcement_mocks,
    mock_gundi_client_v2,
    connection_v2,
    route_v2,
    destination_integration_v2,
):
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    generic = destination_integration_v2.copy(deep=True)
    generic.additional = {**(generic.additional or {}), "generic_model": True}
    mock_gundi_client_v2.get_integration_details.return_value = async_return(generic)
    publish_delivery = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers._publish_gundi_delivery", publish_delivery
    )
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

    assert publish_delivery.call_count == 0
    assert dispatcher.call_count == 0
    assert routing_events.call_count == 1


@pytest.mark.asyncio
async def test_the_event_is_published_to_the_routing_events_topic(
    mocker,
    enforcement_mocks,
    mock_gundi_client_v2,
    connection_v2,
    route_v2,
    mock_pubsub,
):
    _enable(mocker)
    from app.core.pubsub import send_event_to_routing_events_topic

    mocker.patch(
        "app.services.event_handlers.send_event_to_routing_events_topic",
        send_event_to_routing_events_topic,
    )
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, destination_id, provider_id, mode="blacklist", ids=["collar-001"]
        )
    )
    event = _observation_event_dict(provider_id, "collar-001")

    await process_observation_event(event, _ATTRIBUTES)

    publisher = mock_pubsub.PublisherClient.return_value
    assert [c[0][1] for c in publisher.topic_path.call_args_list] == [
        settings.ROUTING_EVENTS_TOPIC
    ]
    published = json.loads(mock_pubsub.PubsubMessage.call_args[0][0].decode("utf-8"))
    assert published["event_type"] == "ObservationFiltered"
    assert published["payload"]["gundi_id"] == event["payload"]["gundi_id"]


@pytest.mark.asyncio
async def test_attachments_are_exempt_from_filtering(
    mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    # An attachment has no device identity of its own (ingestion often leaves a
    # sentinel external_source_id); it follows its parent event, which was
    # already gated - a closed whitelist must not strand delivered events
    # without their images.
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, destination_id, provider_id, mode="whitelist", ids=[]
        )
    )
    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": "2026-07-29 13:23:43.952056+00:00",
        "schema_version": "v1",
        "event_type": "AttachmentReceived",
        "payload": {
            "gundi_id": str(uuid.uuid4()),
            "related_to": str(uuid.uuid4()),
            "data_provider_id": provider_id,
            "external_source_id": "None",
            "file_path": "attachments/123_file.jpg",
            "observation_type": "att",
        },
    }
    attributes = {**_ATTRIBUTES, "observation_type": "att"}

    await process_observation_event(event, attributes)

    assert dispatcher.call_count == 1
    assert routing_events.call_count == 0


@pytest.mark.asyncio
async def test_events_are_filtered_like_observations(
    mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    # The rule gates the route arrow, not one stream type: a camera-trap event
    # from an unlisted device drops the same way an observation does.
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, destination_id, provider_id, mode="whitelist", ids=["camera-A"]
        )
    )
    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": "2026-07-29 13:23:43.952056+00:00",
        "schema_version": "v1",
        "event_type": "EventReceived",
        "payload": {
            "gundi_id": str(uuid.uuid4()),
            "data_provider_id": provider_id,
            "source_id": str(uuid.uuid4()),
            "external_source_id": "camera-B",
            "recorded_at": "2026-07-22 11:51:05+00:00",
            "title": "Animal detected",
            "event_type": "wildlife_sighting_rep",
            "observation_type": "ev",
        },
    }
    attributes = {**_ATTRIBUTES, "observation_type": "ev"}

    await process_observation_event(event, attributes)

    assert dispatcher.call_count == 0
    assert routing_events.call_count == 1
    assert routing_events.call_args[0][0].payload.observation_type == "ev"


@pytest.mark.asyncio
async def test_a_rule_on_one_destination_leaves_the_other_untouched(
    mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    # Per-destination keying is the core of the feature: absence of a rule on
    # D2 means allow, whatever D1's rule says.
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    provider_id = str(connection_v2.provider.id)
    destination_a = str(connection_v2.destinations[0].id)
    second = json.loads(connection_v2.destinations[0].json())
    destination_b = str(uuid.uuid4())
    second["id"] = destination_b
    two_destination_connection = schemas_v2.Connection.parse_obj(
        {
            **json.loads(connection_v2.json()),
            "destinations": [json.loads(connection_v2.destinations[0].json()), second],
        }
    )
    mock_gundi_client_v2.get_connection_details.return_value = async_return(
        two_destination_connection
    )
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, destination_a, provider_id, mode="whitelist", ids=[]
        )
    )

    await process_observation_event(
        _observation_event_dict(provider_id, "collar-001"), _ATTRIBUTES
    )

    assert dispatcher.call_count == 1  # destination B delivered
    assert str(dispatcher.call_args[1]["destination"].id) == destination_b
    assert routing_events.call_count == 1  # destination A dropped
    assert str(routing_events.call_args[0][0].payload.destination_id) == destination_a


@pytest.mark.asyncio
async def test_generic_model_batch_items_are_filtered_too(
    mocker, enforcement_mocks, mock_gundi_client_v2, connection_v2, route_v2
):
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    mocker.patch("app.services.event_handlers._uses_generic_model", return_value=True)
    gundi_delivery = mocker.AsyncMock()
    mocker.patch("app.services.event_handlers._publish_gundi_delivery", gundi_delivery)
    provider_id = str(connection_v2.provider.id)
    destination_id = str(connection_v2.destinations[0].id)
    mock_gundi_client_v2.get_route_details.return_value = async_return(
        _route_with_filter(
            route_v2, destination_id, provider_id, mode="whitelist", ids=["device-0"]
        )
    )
    attributes = {**_ATTRIBUTES, "batch": "true", "batch_count": "2"}

    await process_observation_event(
        _batch_event_dict(provider_id, ["device-0", "device-1"]), attributes
    )

    assert gundi_delivery.call_count == 1
    assert routing_events.call_count == 1
    events = routing_events.call_args[0][0]
    assert [e.payload.external_source_id for e in events] == ["device-1"]


@pytest.mark.asyncio
async def test_a_route_cached_before_filters_existed_fails_open(
    mocker, enforcement_mocks, mock_gundi_client_v2, mock_cache, connection_v2, route_v2
):
    # Redis can hold a route serialized by gundi-core 1.13, which had no
    # filters key: parse_raw must default it and deliver.
    dispatcher, routing_events = enforcement_mocks
    _enable(mocker)
    provider_id = str(connection_v2.provider.id)
    legacy = json.loads(route_v2.json())
    legacy.pop("filters", None)

    def cached(key):
        if str(key).startswith("route_detail."):
            return async_return(json.dumps(legacy))
        return async_return(None)

    mock_cache.get.side_effect = cached

    await process_observation_event(
        _observation_event_dict(provider_id, "collar-001"), _ATTRIBUTES
    )

    assert dispatcher.call_count == 1
    assert routing_events.call_count == 0
