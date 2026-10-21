"""Log output as Cloud Logging receives it: one JSON line per entry, with
`severity` and the integration ids, and one ERROR entry per failure."""

import base64
import datetime
import json
import logging
import sys

import pytest
from fastapi.testclient import TestClient
from pythonjsonlogger.json import JsonFormatter
from smartconnect.exceptions import SMARTClientException

from app.conftest import async_return
from app.core.local_logging import ExtraKeys
from app.main import app

api_client = TestClient(app)


def _configured_json_formatter():
    formatters = [
        h.formatter
        for h in logging.getLogger().handlers
        if isinstance(h.formatter, JsonFormatter)
    ]
    assert (
        formatters
    ), "the root logger has no JSON handler; logging was never initialized"
    return formatters[0]


def test_error_with_traceback_is_one_json_line_with_severity_and_ids():
    logger = logging.getLogger("app.tests.structured_logging")
    try:
        try:
            raise TimeoutError("SMART read timeout")
        except TimeoutError as e:
            raise ValueError("Error getting data model") from e
    except ValueError:
        record = logger.makeRecord(
            logger.name,
            logging.ERROR,
            __file__,
            0,
            "Error transforming observation",
            None,
            exc_info=sys.exc_info(),
            extra={
                ExtraKeys.InboundIntId: "provider-id",
                ExtraKeys.OutboundIntId: "destination-id",
            },
        )

    line = _configured_json_formatter().format(record)

    assert "\n" not in line
    entry = json.loads(line)
    assert entry["severity"] == "ERROR"
    assert entry["message"] == "Error transforming observation"
    assert entry["inbound_integration_id"] == "provider-id"
    assert entry["outbound_integration_id"] == "destination-id"
    assert "TimeoutError: SMART read timeout" in entry["stack_trace"]
    assert "ValueError: Error getting data model" in entry["stack_trace"]


@pytest.mark.asyncio
async def test_failed_observation_logs_one_error_entry(
    mocker,
    caplog,
    mock_cache,
    mock_deduplication_cache_empty,
    mock_gundi_client_v2,
    mock_smart_async_client,
    connection_v2_traptagger_to_smart,
    destination_integration_v2_smart,
    pubsub_request_headers,
    event_v2_request_payload,
):
    """The prod case: SMART's data model can't be fetched. Every layer used to
    log its own traceback; now only the routing boundary does, and the
    message is nacked with a 500 instead of an exception uvicorn logs again."""
    mock_gundi_client_v2.get_connection_details.return_value = async_return(
        connection_v2_traptagger_to_smart
    )
    mock_gundi_client_v2.get_integration_details.return_value = async_return(
        destination_integration_v2_smart
    )
    mock_smart_async_client.get_data_model.side_effect = SMARTClientException(
        "401 Unauthorized"
    )
    mocker.patch(
        "app.services.transformers.AsyncSmartClient",
        mocker.MagicMock(return_value=mock_smart_async_client),
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.deduplication._cache_db", mock_deduplication_cache_empty)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    dispatcher_send = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher",
        dispatcher_send,
    )
    activity_cache = mocker.MagicMock()
    activity_cache.set.return_value = async_return(True)
    mocker.patch("app.services.activity_logger._cache_db", activity_cache)
    mocker.patch(
        "app.services.activity_logger.send_event_to_integration_events_topic",
        return_value=async_return(None),
    )

    with caplog.at_level(logging.ERROR):
        response = api_client.post(
            "/", headers=pubsub_request_headers, json=event_v2_request_payload
        )

    assert response.status_code == 500
    dispatcher_send.assert_not_called()
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1, [r.getMessage() for r in errors]
    entry = json.loads(_configured_json_formatter().format(errors[0]))
    assert entry["severity"] == "ERROR"
    assert entry["inbound_integration_id"] == str(
        connection_v2_traptagger_to_smart.provider.id
    )
    assert entry["outbound_integration_id"] == str(
        connection_v2_traptagger_to_smart.destinations[0].id
    )
    assert "SMARTClientException: 401 Unauthorized" in entry["stack_trace"]


def test_error_nobody_logged_is_logged_once_at_the_endpoint(mocker, caplog):
    mocker.patch("app.main.process_request", side_effect=RuntimeError("bad envelope"))

    with caplog.at_level(logging.ERROR):
        response = api_client.post("/", json={"message": {}})

    # A 5xx nacks the message so Pub/Sub retries it.
    assert response.status_code == 500
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert "RuntimeError: bad envelope" in errors[0].getMessage()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload_fixture",
    ["event_v2_request_payload", "event_update_v2_request_payload"],
    ids=["event", "event_update"],
)
async def test_missing_category_logs_one_error_entry(
    request,
    mocker,
    caplog,
    mock_cache,
    mock_deduplication_cache_empty,
    mock_gundi_client_v2,
    mock_smart_async_client,
    connection_v2_traptagger_to_smart,
    destination_integration_v2_smart,
    pubsub_request_headers,
    payload_fixture,
):
    """A SMART destination with no category for the event type: the
    transformer raises a ReferenceDataError and only the routing boundary logs
    it, for both new events and event updates."""
    payload = request.getfixturevalue(payload_fixture)
    mock_gundi_client_v2.get_connection_details.return_value = async_return(
        connection_v2_traptagger_to_smart
    )
    mock_gundi_client_v2.get_integration_details.return_value = async_return(
        destination_integration_v2_smart
    )
    mocker.patch(
        "app.services.transformers.AsyncSmartClient",
        mocker.MagicMock(return_value=mock_smart_async_client),
    )
    mocker.patch(
        "app.services.transformers.SMARTTransformerV2.resolve_category_path_for_event",
        return_value=None,  # patching an async def yields an AsyncMock
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.deduplication._cache_db", mock_deduplication_cache_empty)
    mocker.patch("app.core.gundi.portal_v2", mock_gundi_client_v2)
    dispatcher_send = mocker.AsyncMock()
    mocker.patch(
        "app.services.event_handlers.send_message_to_gcp_pubsub_dispatcher",
        dispatcher_send,
    )
    activity_cache = mocker.MagicMock()
    activity_cache.set.return_value = async_return(True)
    mocker.patch("app.services.activity_logger._cache_db", activity_cache)
    mocker.patch(
        "app.services.activity_logger.send_event_to_integration_events_topic",
        return_value=async_return(None),
    )

    with caplog.at_level(logging.ERROR):
        response = api_client.post("/", headers=pubsub_request_headers, json=payload)

    assert response.status_code == 500
    dispatcher_send.assert_not_called()
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1, [r.getMessage() for r in errors]
    entry = json.loads(_configured_json_formatter().format(errors[0]))
    assert entry["severity"] == "ERROR"
    assert entry["inbound_integration_id"] == str(
        connection_v2_traptagger_to_smart.provider.id
    )
    assert entry["outbound_integration_id"] == str(
        connection_v2_traptagger_to_smart.destinations[0].id
    )
    assert "No category found for event_type" in entry["stack_trace"]


@pytest.mark.asyncio
async def test_v1_missing_category_logs_one_error_entry(
    mocker,
    caplog,
    mock_cache,
    mock_deduplication_cache_empty,
    mock_gundi_client,
    mock_smart_async_client,
    mock_send_observation_to_dead_letter_topic,
    raw_observation_geoevent_with_valid_uuid,
    smart_outbound_configuration_gcp_pubsub,
    pubsub_request_headers,
):
    """v1 dead-letters the observation per destination, so the request
    succeeds, but the failure is still logged once, with both ids."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    payload = {
        "message": {
            "attributes": {"observation_type": "ge"},
            "data": base64.b64encode(
                json.dumps(raw_observation_geoevent_with_valid_uuid).encode()
            ).decode(),
            "message_id": "11960027960451652",
            "publish_time": timestamp,
        },
        "subscription": "projects/MY-PROJECT/subscriptions/MY-SUB",
    }
    mock_gundi_client.get_outbound_integration.return_value = async_return(
        smart_outbound_configuration_gcp_pubsub
    )
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [smart_outbound_configuration_gcp_pubsub]
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi._portal", mock_gundi_client)
    mocker.patch("app.core.deduplication._cache_db", mock_deduplication_cache_empty)
    mocker.patch(
        "app.services.transformers.AsyncSmartClient",
        mocker.MagicMock(return_value=mock_smart_async_client),
    )
    mocker.patch(
        "app.services.transformers.SMARTTransformer.resolve_category_path_for_event",
        return_value=None,  # patching an async def yields an AsyncMock
    )
    mocker.patch(
        "app.services.process_messages.send_observation_to_dead_letter_topic",
        mock_send_observation_to_dead_letter_topic,
    )
    dispatcher_send = mocker.AsyncMock()
    mocker.patch(
        "app.services.process_messages.send_message_to_gcp_pubsub_dispatcher",
        dispatcher_send,
    )

    with caplog.at_level(logging.ERROR):
        response = api_client.post("/", headers=pubsub_request_headers, json=payload)

    assert response.status_code == 200
    dispatcher_send.assert_not_called()
    assert mock_send_observation_to_dead_letter_topic.call_count == 1
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1, [r.getMessage() for r in errors]
    entry = json.loads(_configured_json_formatter().format(errors[0]))
    assert entry["severity"] == "ERROR"
    assert entry["inbound_integration_id"] == str(
        raw_observation_geoevent_with_valid_uuid["integration_id"]
    )
    assert entry["outbound_integration_id"] == str(
        smart_outbound_configuration_gcp_pubsub.id
    )
    assert "No category found for event_type" in entry["stack_trace"]


def test_request_body_is_not_logged_or_printed(
    mocker, caplog, capsys, pubsub_request_headers, event_v2_request_payload
):
    mocker.patch(
        "app.main.process_request", return_value={"status": "processed"}
    )  # patching an async def yields an AsyncMock

    with caplog.at_level(logging.INFO):
        response = api_client.post(
            "/", headers=pubsub_request_headers, json=event_v2_request_payload
        )

    assert response.status_code == 200
    raw_data = event_v2_request_payload["message"]["data"]
    assert not [r for r in caplog.records if raw_data in r.getMessage()]
    assert capsys.readouterr().out == ""
