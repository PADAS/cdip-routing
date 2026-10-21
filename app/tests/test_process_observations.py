import httpx
import pytest
from smartconnect.exceptions import (
    SMARTClientException,
    SMARTClientServerError,
    SMARTClientUnauthorizedError,
)

from app.conftest import async_return
from app.services.process_messages import process_observation


@pytest.mark.asyncio
async def test_process_observation_and_route_to_gcp_pubsub_dispatcher(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_pubsub,
    raw_observation_position,
    raw_observation_position_attributes,
    outbound_configuration_gcp_pubsub,
):
    # Mock external dependencies
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_gcp_pubsub]
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi._portal", mock_gundi_client)
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    await process_observation(
        raw_observation_position, raw_observation_position_attributes
    )
    # Check that the right methods, to publish to PuSub, were called
    assert mock_pubsub.PublisherClient.called
    assert mock_pubsub.PublisherClient.return_value.publish.called


@pytest.mark.asyncio
async def test_retry_observations_sent_to_gcp_pubsub_on_timeout(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_pubsub_client_with_timeout_once,
    raw_observation_position,
    raw_observation_position_attributes,
    outbound_configuration_gcp_pubsub,
):
    # Mock external dependencies
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_gcp_pubsub]
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi._portal", mock_gundi_client)
    # The mocked PubSub client raises an asyncio.TimeoutError in the first call, and returns success in a second call
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub_client_with_timeout_once)
    await process_observation(
        raw_observation_position, raw_observation_position_attributes
    )
    # Check that the right methods, to publish to PuSub, were called
    assert mock_pubsub_client_with_timeout_once.PublisherClient.called
    # The Publish method should be called twice due to the retry
    assert (
        mock_pubsub_client_with_timeout_once.PublisherClient.return_value.publish.called
    )
    assert (
        mock_pubsub_client_with_timeout_once.PublisherClient.return_value.publish.call_count
        == 2
    )


@pytest.mark.asyncio
async def test_retry_observations_sent_to_gcp_pubsub_on_client_error(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_pubsub_client_with_client_error_once,
    raw_observation_position,
    raw_observation_position_attributes,
    outbound_configuration_gcp_pubsub,
):
    # Mock external dependencies
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_gcp_pubsub]
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi._portal", mock_gundi_client)
    # The mocked PubSub client raises an asyncio.TimeoutError in the first call, and returns success in a second call
    mocker.patch(
        "app.core.pubsub.pubsub",
        mock_pubsub_client_with_client_error_once,
    )
    await process_observation(
        raw_observation_position, raw_observation_position_attributes
    )
    # Check that the right methods, to publish to PuSub, were called
    assert mock_pubsub_client_with_client_error_once.PublisherClient.called
    # The Publish method should be called twice due to the retry
    assert (
        mock_pubsub_client_with_client_error_once.PublisherClient.return_value.publish.called
    )
    assert (
        mock_pubsub_client_with_client_error_once.PublisherClient.return_value.publish.call_count
        == 2
    )


async def _test_retry_unprocessed_observations_on_portal_error(
    mocker,
    mock_cache,
    mock_gundi_client_with_errors,
    mock_send_observation_to_dead_letter_topic,
    raw_observation_position,
    raw_observation_position_attributes,
):
    # Mock external dependencies
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi._portal", mock_gundi_client_with_errors)
    with pytest.raises(
        Exception
    ):  # An exception must be raised to activate the GCP retry mechanism
        await process_observation(
            raw_observation_position, raw_observation_position_attributes
        )

    # Check that is not sent to the dead letter topic
    assert not mock_send_observation_to_dead_letter_topic.called


@pytest.mark.asyncio
async def test_retry_unprocessed_observations_on_portal_client_connector_error(
    mocker,
    mock_cache,
    mock_gundi_client_with_client_connector_error_once,
    mock_send_observation_to_dead_letter_topic,
    raw_observation_position,
    raw_observation_position_attributes,
    outbound_configuration_default,
):
    await _test_retry_unprocessed_observations_on_portal_error(
        mocker=mocker,
        mock_cache=mock_cache,
        mock_gundi_client_with_errors=mock_gundi_client_with_client_connector_error_once,
        mock_send_observation_to_dead_letter_topic=mock_send_observation_to_dead_letter_topic,
        raw_observation_position=raw_observation_position,
        raw_observation_position_attributes=raw_observation_position_attributes,
    )


@pytest.mark.asyncio
async def test_retry_unprocessed_observations_on_portal_server_disconnected_error(
    mocker,
    mock_cache,
    mock_gundi_client_with_server_disconnected_error_once,
    mock_send_observation_to_dead_letter_topic,
    raw_observation_position,
    raw_observation_position_attributes,
    outbound_configuration_default,
):
    await _test_retry_unprocessed_observations_on_portal_error(
        mocker=mocker,
        mock_cache=mock_cache,
        mock_gundi_client_with_errors=mock_gundi_client_with_server_disconnected_error_once,
        mock_send_observation_to_dead_letter_topic=mock_send_observation_to_dead_letter_topic,
        raw_observation_position=raw_observation_position,
        raw_observation_position_attributes=raw_observation_position_attributes,
    )


@pytest.mark.asyncio
async def test_retry_unprocessed_observations_on_portal_server_timeout_error_once(
    mocker,
    mock_cache,
    mock_gundi_client_with_server_timeout_error_once,
    mock_send_observation_to_dead_letter_topic,
    raw_observation_position,
    raw_observation_position_attributes,
    outbound_configuration_default,
):
    await _test_retry_unprocessed_observations_on_portal_error(
        mocker=mocker,
        mock_cache=mock_cache,
        mock_gundi_client_with_errors=mock_gundi_client_with_server_timeout_error_once,
        mock_send_observation_to_dead_letter_topic=mock_send_observation_to_dead_letter_topic,
        raw_observation_position=raw_observation_position,
        raw_observation_position_attributes=raw_observation_position_attributes,
    )


@pytest.mark.asyncio
async def test_process_observation_geoevent_and_route_to_gcp_pubsub_dispatcher(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_pubsub,
    raw_observation_geoevent,
    raw_observation_geoevent_attributes,
    outbound_configuration_gcp_pubsub,
):
    # Mock external dependencies
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_gcp_pubsub]
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi._portal", mock_gundi_client)
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    await process_observation(
        raw_observation_geoevent, raw_observation_geoevent_attributes
    )
    # Check that the right methods, to publish to PuSub, were called
    assert mock_pubsub.PublisherClient.called
    assert mock_pubsub.PublisherClient.return_value.publish.called


@pytest.mark.asyncio
async def test_process_observation_cameratrap_and_route_to_gcp_pubsub_dispatcher(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_pubsub,
    raw_observation_cameratrap,
    raw_observation_cameratrap_attributes,
    outbound_configuration_gcp_pubsub,
):
    # Mock external dependencies
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_gcp_pubsub]
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi._portal", mock_gundi_client)
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    await process_observation(
        raw_observation_cameratrap, raw_observation_cameratrap_attributes
    )
    # Check that the right methods, to publish to PuSub, were called
    assert mock_pubsub.PublisherClient.called
    assert mock_pubsub.PublisherClient.return_value.publish.called


@pytest.mark.asyncio
async def test_retry_process_event_on_smart_error_and_send_to_dlq(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_smart_async_client_class_with_server_error,
    mock_send_observation_to_dead_letter_topic,
    raw_observation_geoevent_with_valid_uuid,
    raw_observation_geoevent_attributes,
    smart_outbound_configuration_gcp_pubsub,
):
    # Override gundi client mock to return a smart destination
    mock_gundi_client.get_outbound_integration.return_value = async_return(
        smart_outbound_configuration_gcp_pubsub
    )
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [smart_outbound_configuration_gcp_pubsub]
    )
    # Mock external dependencies
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi._portal", mock_gundi_client)
    mocker.patch(
        "app.services.transformers.AsyncSmartClient",
        mock_smart_async_client_class_with_server_error,
    )
    mocker.patch(
        "app.services.process_messages.send_observation_to_dead_letter_topic",
        mock_send_observation_to_dead_letter_topic,
    )
    await process_observation(
        raw_observation_geoevent_with_valid_uuid, raw_observation_geoevent_attributes
    )

    # Check that is retried and then sent to the dead letter topic
    assert (
        mock_smart_async_client_class_with_server_error.return_value.get_incident.call_count
        == 3
    )
    assert mock_send_observation_to_dead_letter_topic.called


@pytest.mark.asyncio
async def test_process_event_doesnt_call_smart_with_invalid_uuid(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_pubsub,
    mock_smart_async_client_class,
    raw_observation_geoevent_for_smart,
    raw_observation_geoevent_attributes,
    mock_send_observation_to_dead_letter_topic,
    smart_outbound_configuration_gcp_pubsub,
):
    # Override gundi client mock to return a smart destination
    mock_gundi_client.get_outbound_integration.return_value = async_return(
        smart_outbound_configuration_gcp_pubsub
    )
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [smart_outbound_configuration_gcp_pubsub]
    )
    # Mock external dependencies
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi._portal", mock_gundi_client)
    mocker.patch(
        "app.services.transformers.AsyncSmartClient",
        mock_smart_async_client_class,
    )
    mocker.patch(
        "app.services.process_messages.send_observation_to_dead_letter_topic",
        mock_send_observation_to_dead_letter_topic,
    )
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    await process_observation(
        raw_observation_geoevent_for_smart, raw_observation_geoevent_attributes
    )

    # Check that we don't make extra requests to smart
    assert not mock_smart_async_client_class.return_value.get_incident.called
    # Check that message was Not sent to teh dead letter
    assert not mock_send_observation_to_dead_letter_topic.called


@pytest.mark.asyncio
async def test_process_event_publishes_smart_event(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_pubsub,
    mock_smart_async_client_class,
    raw_observation_geoevent_for_smart,
    raw_observation_geoevent_attributes,
    mock_send_observation_to_dead_letter_topic,
    smart_outbound_configuration_gcp_pubsub,
):
    # Override gundi client mock to return a smart destination
    mock_gundi_client.get_outbound_integration.return_value = async_return(
        smart_outbound_configuration_gcp_pubsub
    )
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [smart_outbound_configuration_gcp_pubsub]
    )
    # Mock external dependencies
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi._portal", mock_gundi_client)
    mocker.patch(
        "app.services.transformers.AsyncSmartClient",
        mock_smart_async_client_class,
    )
    mocker.patch(
        "app.services.process_messages.send_observation_to_dead_letter_topic",
        mock_send_observation_to_dead_letter_topic,
    )
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)
    await process_observation(
        raw_observation_geoevent_for_smart, raw_observation_geoevent_attributes
    )

    assert mock_pubsub.PublisherClient.called
    assert mock_pubsub.PublisherClient.return_value.publish.called


def _route_to_smart_destination(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_smart_client_class,
    mock_send_observation_to_dead_letter_topic,
    smart_destination,
):
    mock_gundi_client.get_outbound_integration.return_value = async_return(
        smart_destination
    )
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [smart_destination]
    )
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi._portal", mock_gundi_client)
    mocker.patch("app.services.transformers.AsyncSmartClient", mock_smart_client_class)
    mocker.patch(
        "app.services.process_messages.send_observation_to_dead_letter_topic",
        mock_send_observation_to_dead_letter_topic,
    )


_smart_request = httpx.Request("GET", "https://fakesmartconnectsite.org/server")


@pytest.mark.parametrize(
    "error",
    [
        SMARTClientException("Failed to get Conservation Areas"),
        SMARTClientServerError("Failed to login to SMART Connect. Status code: 503"),
        httpx.ConnectTimeout("timed out", request=_smart_request),
        httpx.ConnectError("connection refused", request=_smart_request),
    ],
    ids=lambda e: type(e).__name__,
)
@pytest.mark.asyncio
async def test_retry_smart_transform_on_transient_error_then_dead_letter(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_smart_async_client,
    mock_smart_async_client_class,
    mock_send_observation_to_dead_letter_topic,
    raw_observation_geoevent_with_valid_uuid,
    raw_observation_geoevent_attributes,
    smart_outbound_configuration_gcp_pubsub,
    error,
):
    mock_smart_async_client.get_incident.side_effect = error
    _route_to_smart_destination(
        mocker,
        mock_cache,
        mock_gundi_client,
        mock_smart_async_client_class,
        mock_send_observation_to_dead_letter_topic,
        smart_outbound_configuration_gcp_pubsub,
    )

    await process_observation(
        raw_observation_geoevent_with_valid_uuid, raw_observation_geoevent_attributes
    )

    assert mock_smart_async_client.get_incident.call_count == 3
    assert mock_send_observation_to_dead_letter_topic.call_count == 1


@pytest.mark.parametrize(
    "error",
    [
        SMARTClientUnauthorizedError("Failed to login. Status code: 401"),
        ValueError("Invalid isoformat string: 'not-a-date'"),
        TypeError("'NoneType' object is not subscriptable"),
    ],
    ids=lambda e: type(e).__name__,
)
@pytest.mark.asyncio
async def test_smart_transform_permanent_error_goes_to_dead_letter_without_retry(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_smart_async_client,
    mock_smart_async_client_class,
    mock_send_observation_to_dead_letter_topic,
    raw_observation_geoevent_with_valid_uuid,
    raw_observation_geoevent_attributes,
    smart_outbound_configuration_gcp_pubsub,
    error,
):
    mock_smart_async_client.get_incident.side_effect = error
    _route_to_smart_destination(
        mocker,
        mock_cache,
        mock_gundi_client,
        mock_smart_async_client_class,
        mock_send_observation_to_dead_letter_topic,
        smart_outbound_configuration_gcp_pubsub,
    )

    await process_observation(
        raw_observation_geoevent_with_valid_uuid, raw_observation_geoevent_attributes
    )

    assert mock_smart_async_client.get_incident.call_count == 1
    assert mock_send_observation_to_dead_letter_topic.call_count == 1


@pytest.mark.asyncio
async def test_smart_event_type_without_category_goes_to_dead_letter_without_retry(
    mocker,
    caplog,
    mock_cache,
    mock_gundi_client,
    mock_pubsub,
    mock_smart_async_client,
    mock_smart_async_client_class,
    mock_send_observation_to_dead_letter_topic,
    raw_observation_geoevent_with_valid_uuid,
    raw_observation_geoevent_attributes,
    smart_outbound_configuration_gcp_pubsub,
):
    raw_observation_geoevent_with_valid_uuid["event_type"] = "cameratrap_rep"
    _route_to_smart_destination(
        mocker,
        mock_cache,
        mock_gundi_client,
        mock_smart_async_client_class,
        mock_send_observation_to_dead_letter_topic,
        smart_outbound_configuration_gcp_pubsub,
    )
    mocker.patch("app.core.pubsub.pubsub", mock_pubsub)

    with caplog.at_level("ERROR"):
        await process_observation(
            raw_observation_geoevent_with_valid_uuid,
            raw_observation_geoevent_attributes,
        )

    assert mock_smart_async_client.get_incident.call_count == 1
    assert mock_send_observation_to_dead_letter_topic.call_count == 1
    assert not mock_pubsub.PublisherClient.return_value.publish.called
    dead_letter_logs = [
        r.getMessage() for r in caplog.records if "dead-letter" in r.getMessage()
    ]
    assert len(dead_letter_logs) == 1
    for expected in (
        "No category found for event_type: cameratrap_rep",
        raw_observation_geoevent_with_valid_uuid["integration_id"],
        str(smart_outbound_configuration_gcp_pubsub.id),
    ):
        assert expected in dead_letter_logs[0]


@pytest.mark.parametrize(
    "failing_method",
    ["get_data_model", "get_configurable_data_model"],
)
@pytest.mark.asyncio
async def test_retry_smart_transform_when_data_model_download_fails(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_smart_async_client,
    mock_smart_async_client_class,
    mock_send_observation_to_dead_letter_topic,
    raw_observation_geoevent_with_valid_uuid,
    raw_observation_geoevent_attributes,
    smart_outbound_configuration_gcp_pubsub,
    failing_method,
):
    # an unknown category makes the transformer consult the configurable models too
    raw_observation_geoevent_with_valid_uuid["event_type"] = "cameratrap_rep"
    smart_outbound_configuration_gcp_pubsub.additional[
        "configurable_models_enabled"
    ] = ["b0ec1e4a-08bb-4d4d-9a3e-7dbf0e4d4b0c"]
    mock_smart_async_client.get_configurable_data_model.return_value = async_return(
        None
    )
    getattr(mock_smart_async_client, failing_method).side_effect = SMARTClientException(
        "Failed downloading SMART Datamodel"
    )
    _route_to_smart_destination(
        mocker,
        mock_cache,
        mock_gundi_client,
        mock_smart_async_client_class,
        mock_send_observation_to_dead_letter_topic,
        smart_outbound_configuration_gcp_pubsub,
    )

    await process_observation(
        raw_observation_geoevent_with_valid_uuid, raw_observation_geoevent_attributes
    )

    assert getattr(mock_smart_async_client, failing_method).call_count == 3
    assert mock_send_observation_to_dead_letter_topic.call_count == 1


@pytest.mark.asyncio
async def test_reference_data_error_is_retried_with_a_string_error_message(
    mocker,
    raw_observation_position,
    raw_observation_position_attributes,
):
    from app.core.errors import ReferenceDataError

    mocker.patch(
        "app.services.process_messages.update_observation_with_device_configuration",
        side_effect=ReferenceDataError("portal down"),
    )
    mock_logger = mocker.patch("app.services.process_messages.logger")

    with pytest.raises(ReferenceDataError):
        await process_observation(
            raw_observation_position, raw_observation_position_attributes
        )

    error_msg = mock_logger.exception.call_args.args[0]
    assert isinstance(error_msg, str)
    assert "portal down" in error_msg
