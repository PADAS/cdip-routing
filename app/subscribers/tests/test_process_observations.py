import pytest

from app.conftest import async_return
from app.subscribers.kafka_subscriber import process_observation


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
