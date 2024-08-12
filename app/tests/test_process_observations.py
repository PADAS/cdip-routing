import pytest
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
    mocker.patch(
        "app.core.pubsub.pubsub"
, mock_pubsub_client_with_timeout_once
    )
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
        "app.core.pubsub.pubsub"
,
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
