import pytest
from cdip_connector.core.routing import TopicEnum
from app.conftest import async_return
from app.subscribers.kafka_subscriber import process_observation, process_transformed_observation


@pytest.mark.asyncio
async def test_process_observation_and_route_to_gcp_pubsub_dispatcher(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_pubsub_client,
    mock_kafka_topic,
    unprocessed_observation_position,
    outbound_configuration_gcp_pubsub,
):
    # Mock external dependencies
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_gcp_pubsub]
    )
    mocker.patch("app.transform_service.services._cache_db", mock_cache)
    mocker.patch("app.transform_service.services._portal", mock_gundi_client)
    mocker.patch("app.subscribers.kafka_subscriber.pubsub", mock_pubsub_client)
    mocker.patch(
        "app.subscribers.kafka_subscriber.observations_transformed_topic",
        mock_kafka_topic,
    )
    await process_observation(None, unprocessed_observation_position)
    # Check that the right methods, to publish to PuSub, were called
    assert mock_pubsub_client.PublisherClient.called
    assert mock_pubsub_client.PublisherClient.return_value.publish.called


@pytest.mark.asyncio
async def test_process_observation_and_route_to_kafka_dispatcher(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_pubsub_client,
    mock_kafka_topic,
    unprocessed_observation_position,
    outbound_configuration_kafka,
):
    # Mock external dependencies
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_kafka]
    )
    mocker.patch("app.transform_service.services._cache_db", mock_cache)
    mocker.patch("app.transform_service.services._portal", mock_gundi_client)
    mocker.patch("app.subscribers.kafka_subscriber.pubsub", mock_pubsub_client)
    mocker.patch(
        "app.subscribers.kafka_subscriber.observations_transformed_topic",
        mock_kafka_topic,
    )
    await process_observation(None, unprocessed_observation_position)
    # Check that the right method, to publish to kafka, was called
    assert mock_kafka_topic.send.called


@pytest.mark.asyncio
async def test_process_observation_and_route_to_kafka_dispatcher_by_default(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_pubsub_client,
    mock_kafka_topic,
    unprocessed_observation_position,
    outbound_configuration_default,
):
    # Mock external dependencies
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_default]
    )
    mocker.patch("app.transform_service.services._cache_db", mock_cache)
    mocker.patch("app.transform_service.services._portal", mock_gundi_client)
    mocker.patch("app.subscribers.kafka_subscriber.pubsub", mock_pubsub_client)
    mocker.patch(
        "app.subscribers.kafka_subscriber.observations_transformed_topic",
        mock_kafka_topic,
    )
    await process_observation(None, unprocessed_observation_position)
    # Check that the right method, to publish to kafka, was called
    assert mock_kafka_topic.send.called


@pytest.mark.asyncio
async def test_retry_observations_sent_to_gcp_pubsub_on_timeout(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_pubsub_client_with_timeout_once,
    mock_kafka_topic,
    unprocessed_observation_position,
    outbound_configuration_gcp_pubsub,
):
    # Mock external dependencies
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_gcp_pubsub]
    )
    mocker.patch("app.transform_service.services._cache_db", mock_cache)
    mocker.patch("app.transform_service.services._portal", mock_gundi_client)
    # The mocked PubSub client raises an asyncio.TimeoutError in the first call, and returns success in a second call
    mocker.patch("app.subscribers.kafka_subscriber.pubsub", mock_pubsub_client_with_timeout_once)
    mocker.patch(
        "app.subscribers.kafka_subscriber.observations_transformed_topic",
        mock_kafka_topic,
    )
    mocker.patch(
        "app.subscribers.kafka_subscriber.observations_unprocessed_deadletter",
        mock_kafka_topic,
    )
    await process_observation(None, unprocessed_observation_position)
    # Check that the right methods, to publish to PuSub, were called
    assert mock_pubsub_client_with_timeout_once.PublisherClient.called
    # The Publish method should be called twice due to the retry
    assert mock_pubsub_client_with_timeout_once.PublisherClient.return_value.publish.called
    assert mock_pubsub_client_with_timeout_once.PublisherClient.return_value.publish.call_count == 2


@pytest.mark.asyncio
async def test_retry_observations_sent_to_gcp_pubsub_on_client_error(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_pubsub_client_with_client_error_once,
    mock_kafka_topic,
    unprocessed_observation_position,
    outbound_configuration_gcp_pubsub,
):
    # Mock external dependencies
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [outbound_configuration_gcp_pubsub]
    )
    mocker.patch("app.transform_service.services._cache_db", mock_cache)
    mocker.patch("app.transform_service.services._portal", mock_gundi_client)
    # The mocked PubSub client raises an asyncio.TimeoutError in the first call, and returns success in a second call
    mocker.patch("app.subscribers.kafka_subscriber.pubsub", mock_pubsub_client_with_client_error_once)
    mocker.patch(
        "app.subscribers.kafka_subscriber.observations_transformed_topic",
        mock_kafka_topic,
    )
    mocker.patch(
        "app.subscribers.kafka_subscriber.observations_unprocessed_deadletter",
        mock_kafka_topic,
    )
    await process_observation(None, unprocessed_observation_position)
    # Check that the right methods, to publish to PuSub, were called
    assert mock_pubsub_client_with_client_error_once.PublisherClient.called
    # The Publish method should be called twice due to the retry
    assert mock_pubsub_client_with_client_error_once.PublisherClient.return_value.publish.called
    assert mock_pubsub_client_with_client_error_once.PublisherClient.return_value.publish.call_count == 2


async def _test_retry_unprocessed_observations_on_portal_error(
    mocker,
    mock_cache,
    mock_gundi_client_with_errors,
    mock_kafka_topics_dic,
    mock_dead_letter_kafka_topic,
    unprocessed_observation_position
):
    # Mock external dependencies
    mocker.patch("app.transform_service.services._cache_db", mock_cache)
    mocker.patch("app.transform_service.services._portal", mock_gundi_client_with_errors)
    mocker.patch(
        "app.subscribers.kafka_subscriber.observations_unprocessed_deadletter",
        mock_dead_letter_kafka_topic,
    )
    mocker.patch(
        "app.subscribers.kafka_subscriber.topics_dict",
        mock_kafka_topics_dic,
    )
    await process_observation(None, unprocessed_observation_position)
    # Check that the message is sent to the retry topic
    assert mock_kafka_topics_dic[TopicEnum.observations_unprocessed_retry_short].send.called
    # And is not sent to the dead letter topic
    assert not mock_dead_letter_kafka_topic.send.called
    assert not mock_kafka_topics_dic[TopicEnum.observations_unprocessed_deadletter].send.called


@pytest.mark.asyncio
async def test_retry_unprocessed_observations_on_portal_client_connector_error(
    mocker,
    mock_cache,
    mock_gundi_client_with_client_connector_error_once,
    mock_kafka_topics_dic,
    mock_dead_letter_kafka_topic,
    unprocessed_observation_position,
    outbound_configuration_default,
):
    await _test_retry_unprocessed_observations_on_portal_error(
        mocker=mocker,
        mock_cache=mock_cache,
        mock_gundi_client_with_errors=mock_gundi_client_with_client_connector_error_once,
        mock_kafka_topics_dic=mock_kafka_topics_dic,
        mock_dead_letter_kafka_topic=mock_dead_letter_kafka_topic,
        unprocessed_observation_position=unprocessed_observation_position
    )
    # And is not sent to the transformed topic
    assert not mock_kafka_topics_dic[TopicEnum.observations_transformed].send.called


@pytest.mark.asyncio
async def test_retry_unprocessed_observations_on_portal_server_disconnected_error(
    mocker,
    mock_cache,
    mock_gundi_client_with_server_disconnected_error_once,
    mock_kafka_topics_dic,
    mock_dead_letter_kafka_topic,
    unprocessed_observation_position,
    outbound_configuration_default,
):
    await _test_retry_unprocessed_observations_on_portal_error(
        mocker=mocker,
        mock_cache=mock_cache,
        mock_gundi_client_with_errors=mock_gundi_client_with_server_disconnected_error_once,
        mock_kafka_topics_dic=mock_kafka_topics_dic,
        mock_dead_letter_kafka_topic=mock_dead_letter_kafka_topic,
        unprocessed_observation_position=unprocessed_observation_position
    )
    # And is not sent to the transformed topic
    assert not mock_kafka_topics_dic[TopicEnum.observations_transformed].send.called


@pytest.mark.asyncio
async def test_retry_unprocessed_observations_on_portal_server_timeout_error_once(
    mocker,
    mock_cache,
    mock_gundi_client_with_server_timeout_error_once,
    mock_kafka_topics_dic,
    mock_dead_letter_kafka_topic,
    unprocessed_observation_position,
    outbound_configuration_default,
):
    await _test_retry_unprocessed_observations_on_portal_error(
        mocker=mocker,
        mock_cache=mock_cache,
        mock_gundi_client_with_errors=mock_gundi_client_with_server_timeout_error_once,
        mock_kafka_topics_dic=mock_kafka_topics_dic,
        mock_dead_letter_kafka_topic=mock_dead_letter_kafka_topic,
        unprocessed_observation_position=unprocessed_observation_position
    )
    # And is not sent to the transformed topic
    assert not mock_kafka_topics_dic[TopicEnum.observations_transformed].send.called


async def _test_retry_transformed_observations_on_portal_error(
    mocker,
    mock_cache,
    mock_gundi_client_with_errors,
    mock_kafka_topics_dic,
    mock_dead_letter_kafka_topic,
    transformed_observation_kafka_message
):
    # Mock external dependencies
    mocker.patch("app.subscribers.services._cache_db", mock_cache)
    mocker.patch("app.subscribers.services._portal", mock_gundi_client_with_errors)
    mocker.patch("app.subscribers.kafka_subscriber.observations_transformed_deadletter", mock_dead_letter_kafka_topic)
    mocker.patch(
        "app.subscribers.kafka_subscriber.topics_dict",
        mock_kafka_topics_dic,
    )
    await process_transformed_observation(None, transformed_observation_kafka_message)
    # Check that the message is sent to the retry topic
    assert mock_kafka_topics_dic[TopicEnum.observations_transformed_retry_short].send.called
    # And is not sent to the dead letter topic
    assert not mock_dead_letter_kafka_topic.send.called
    assert not mock_kafka_topics_dic[TopicEnum.observations_transformed_deadletter].send.called


@pytest.mark.asyncio
async def test_retry_transformed_observations_on_portal_client_connector_error(
    mocker,
    mock_cache,
    mock_gundi_client_with_client_connector_error_once,
    mock_kafka_topics_dic,
    mock_dead_letter_kafka_topic,
    transformed_observation_kafka_message,
    outbound_configuration_default,
):
    await _test_retry_transformed_observations_on_portal_error(
        mocker=mocker,
        mock_cache=mock_cache,
        mock_gundi_client_with_errors=mock_gundi_client_with_client_connector_error_once,
        mock_kafka_topics_dic=mock_kafka_topics_dic,
        mock_dead_letter_kafka_topic=mock_dead_letter_kafka_topic,
        transformed_observation_kafka_message=transformed_observation_kafka_message
    )


@pytest.mark.asyncio
async def test_retry_transformed_observations_on_portal_server_disconnected_error(
    mocker,
    mock_cache,
    mock_gundi_client_with_server_disconnected_error_once,
    mock_kafka_topics_dic,
    mock_dead_letter_kafka_topic,
    transformed_observation_kafka_message,
    outbound_configuration_default,
):
    await _test_retry_transformed_observations_on_portal_error(
        mocker=mocker,
        mock_cache=mock_cache,
        mock_gundi_client_with_errors=mock_gundi_client_with_server_disconnected_error_once,
        mock_kafka_topics_dic=mock_kafka_topics_dic,
        mock_dead_letter_kafka_topic=mock_dead_letter_kafka_topic,
        transformed_observation_kafka_message=transformed_observation_kafka_message
    )


@pytest.mark.asyncio
async def test_retry_transformed_observations_on_portal_server_timeout_error(
    mocker,
    mock_cache,
    mock_gundi_client_with_server_timeout_error_once,
    mock_kafka_topics_dic,
    mock_dead_letter_kafka_topic,
    transformed_observation_kafka_message,
    outbound_configuration_default,
):
    await _test_retry_transformed_observations_on_portal_error(
        mocker=mocker,
        mock_cache=mock_cache,
        mock_gundi_client_with_errors=mock_gundi_client_with_server_timeout_error_once,
        mock_kafka_topics_dic=mock_kafka_topics_dic,
        mock_dead_letter_kafka_topic=mock_dead_letter_kafka_topic,
        transformed_observation_kafka_message=transformed_observation_kafka_message
    )
