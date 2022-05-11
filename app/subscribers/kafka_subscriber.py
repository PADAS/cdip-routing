import logging
from datetime import datetime

import certifi
import faust
from aiokafka.helpers import create_ssl_context
from cdip_connector.core.routing import TopicEnum

from cdip_connector.core import cdip_settings
from app.core.local_logging import DEFAULT_LOGGING, ExtraKeys
from app.core.utils import get_redis_db
from app.subscribers.services import (
    extract_fields_from_message,
    convert_observation_to_cdip_schema,
    create_transformed_message,
    get_key_for_transformed_observation,
    dispatch_transformed_observation,
    wait_until_retry_at,
    update_attributes_for_retry,
    create_retry_transformed_message,
)
from app.transform_service.services import (
    get_all_outbound_configs_for_id,
    update_observation_with_device_configuration,
)

logger = logging.getLogger(__name__)

APP_ID = "cdip-routing"

cloud_enabled = cdip_settings.CONFLUENT_CLOUD_ENABLED
if cloud_enabled:
    logger.debug(f"Entering Confluent Cloud Enabled Flow")
    cert_path = certifi.where()
    logger.debug(f"cert path: {cert_path}")
    ssl_context = create_ssl_context(cafile=cert_path)

    """ Currently there are limitations on the basic Confluent Cloud account. Automatic topic creation is restricted
        which requires the disabling of the leader topic. This may have repercussions regarding the durability of this
        process. Any topics that are specified in code and utilized in the flow must be created ahead of time in the
        cloud.
    """
    logger.debug(
        f"username: {cdip_settings.CONFLUENT_CLOUD_USERNAME}, pw: {cdip_settings.CONFLUENT_CLOUD_PASSWORD}"
    )

    app = faust.App(
        APP_ID,
        broker=f"{cdip_settings.KAFKA_BROKER}",
        broker_credentials=faust.SASLCredentials(
            username=cdip_settings.CONFLUENT_CLOUD_USERNAME,
            password=cdip_settings.CONFLUENT_CLOUD_PASSWORD,
            ssl_context=ssl_context,
            mechanism="PLAIN",
        ),
        value_serializer="raw",
        logging_config=DEFAULT_LOGGING,
        topic_disable_leader=True,
    )
else:
    app = faust.App(
        APP_ID,
        broker=f"{cdip_settings.KAFKA_BROKER}",
        value_serializer="raw",
        logging_config=DEFAULT_LOGGING,
    )

observations_unprocessed_topic = app.topic(TopicEnum.observations_unprocessed.value)
observations_transformed_topic = app.topic(TopicEnum.observations_transformed.value)
observations_transformed_retry_short_topic = app.topic(
    TopicEnum.observations_transformed_retry_short.value
)
observations_transformed_retry_long_topic = app.topic(
    TopicEnum.observations_transformed_retry_long.value
)
observations_transformed_deadletter = app.topic(
    TopicEnum.observations_transformed_deadletter.value
)

topics_dict = {
    TopicEnum.observations_unprocessed.value: observations_unprocessed_topic,
    TopicEnum.observations_transformed: observations_transformed_topic,
    TopicEnum.observations_transformed_retry_short: observations_transformed_retry_short_topic,
    TopicEnum.observations_transformed_retry_long: observations_transformed_retry_long_topic,
    TopicEnum.observations_transformed_deadletter: observations_transformed_deadletter,
}


async def process_observation(key, message):
    try:
        logger.debug(f"message received: {message}")
        raw_observation, attributes = extract_fields_from_message(message)
        logger.debug(f"observation: {raw_observation}")
        logger.debug(f"attributes: {attributes}")

        db = get_redis_db()

        observation = convert_observation_to_cdip_schema(raw_observation)
        logger.info(
            "received unprocessed observation",
            extra={
                ExtraKeys.DeviceId: observation.device_id,
                ExtraKeys.InboundIntId: observation.integration_id,
                ExtraKeys.StreamType: observation.observation_type,
            },
        )
    except Exception as e:
        logger.exception(
            f"Exception occurred prior to processing observation",
            extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.Observation: message},
        )
        return
    try:
        if observation:
            observation = await update_observation_with_device_configuration(
                observation
            )
            int_id = observation.integration_id
            destinations = get_all_outbound_configs_for_id(db, int_id)

            for destination in destinations:
                jsonified_data = create_transformed_message(
                    observation=observation,
                    destination=destination,
                    prefix=observation.observation_type,
                )
                if jsonified_data:
                    key = get_key_for_transformed_observation(key, destination.id)
                    await observations_transformed_topic.send(
                        key=key, value=jsonified_data
                    )
    except Exception as e:
        logger.exception(
            f"Exception occurred processing observation",
            extra={
                ExtraKeys.AttentionNeeded: True,
                ExtraKeys.DeviceId: observation.device_id,
                ExtraKeys.InboundIntId: observation.integration_id,
                ExtraKeys.StreamType: observation.observation_type,
            },
        )
        # TODO: Implement Retry Logic


async def process_transformed_observation(key, transformed_message):
    try:
        transformed_observation, attributes = extract_fields_from_message(
            transformed_message
        )

        observation_type = attributes.get("observation_type")
        device_id = attributes.get("device_id")
        integration_id = attributes.get("integration_id")
        outbound_config_id = attributes.get("outbound_config_id")
        retry_attempt: int = attributes.get("retry_attempt") or 0

        logger.info(
            "received transformed observation",
            extra={
                ExtraKeys.DeviceId: device_id,
                ExtraKeys.InboundIntId: integration_id,
                ExtraKeys.OutboundIntId: outbound_config_id,
                ExtraKeys.StreamType: observation_type,
                ExtraKeys.RetryAttempt: retry_attempt,
            },
        )

    except Exception as e:
        logger.exception(
            f"Exception occurred prior to dispatching transformed observation",
            extra={
                ExtraKeys.AttentionNeeded: True,
                ExtraKeys.Observation: transformed_message,
            },
        )
        raise e
    try:
        logger.info(
            "Dispatching for transformed observation.",
            extra={
                "transformed_observation": transformed_observation,
                "integration_id": integration_id,
                "outbound_config_id": outbound_config_id,
                "observation_type": observation_type,
            },
        )
        dispatch_transformed_observation(
            observation_type,
            outbound_config_id,
            integration_id,
            transformed_observation,
        )
    except Exception as e:
        logger.exception(
            f"Exception occurred processing transformed observation",
            extra={
                ExtraKeys.AttentionNeeded: True,
                ExtraKeys.DeviceId: device_id,
                ExtraKeys.InboundIntId: integration_id,
                ExtraKeys.OutboundIntId: outbound_config_id,
                ExtraKeys.StreamType: observation_type,
            },
        )
        await process_failed_transformed_observation(key, transformed_message)


async def process_failed_transformed_observation(key, transformed_message):
    try:
        transformed_observation, attributes = extract_fields_from_message(
            transformed_message
        )
        attributes = update_attributes_for_retry(attributes)
        observation_type = attributes.get("observation_type")
        device_id = attributes.get("device_id")
        integration_id = attributes.get("integration_id")
        outbound_config_id = attributes.get("outbound_config_id")
        retry_topic_str = attributes.get("retry_topic")
        retry_attempt = attributes.get("retry_attempt")
        retry_transformed_message = create_retry_transformed_message(
            transformed_observation, attributes
        )
        retry_topic: faust.Topic = topics_dict.get(retry_topic_str)
        logger.info(
            "Putting failed transformed observation back on queue",
            extra={
                ExtraKeys.DeviceId: device_id,
                ExtraKeys.InboundIntId: integration_id,
                ExtraKeys.OutboundIntId: outbound_config_id,
                ExtraKeys.StreamType: observation_type,
                ExtraKeys.RetryTopic: retry_topic_str,
                ExtraKeys.RetryAttempt: retry_attempt,
            },
        )
        await retry_topic.send(value=retry_transformed_message)
    except Exception as e:
        # When all else fails post to dead letter
        await observations_transformed_deadletter.send(value=transformed_message)


async def process_transformed_retry_observation(key, transformed_message):
    try:
        transformed_observation, attributes = extract_fields_from_message(
            transformed_message
        )
        retry_at = datetime.fromisoformat(attributes.get("retry_at"))
        await wait_until_retry_at(retry_at)
        await process_transformed_observation(key, transformed_message)
    except Exception as e:
        # When all else fails post to dead letter
        await observations_transformed_deadletter.send(value=transformed_message)


@app.agent(observations_unprocessed_topic)
async def process_observations(streaming_data):
    async for key, message in streaming_data.items():
        await process_observation(key, message)


@app.agent(observations_transformed_topic)
async def process_transformed_observations(streaming_transformed_data):
    async for key, transformed_message in streaming_transformed_data.items():
        await process_transformed_observation(key, transformed_message)


@app.agent(observations_transformed_retry_short_topic)
async def process_transformed_retry_short_observations(streaming_transformed_data):
    async for key, transformed_message in streaming_transformed_data.items():
        await process_transformed_retry_observation(key, transformed_message)


@app.agent(observations_transformed_retry_long_topic)
async def process_transformed_retry_long_observations(streaming_transformed_data):
    async for key, transformed_message in streaming_transformed_data.items():
        await process_transformed_retry_observation(key, transformed_message)


@app.timer(interval=120.0)
async def log_metrics(app):
    m = app.monitor
    metrics_dict = {
        "rebalances": m.rebalances,
        "rebalance_return_avg": m.rebalance_return_avg,
        # 'messages_received_by_topic': m.messages_received_by_topic,
    }
    logger.info(f"Metrics heartbeat for Consumer", extra=metrics_dict)


# @app.on_rebalance_start()
# async def on_rebalance_start():
#     pass


if __name__ == "__main__":
    logger.info("Application getting started")
    app.main()
