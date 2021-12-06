import logging

import certifi
import faust
from aiokafka.helpers import create_ssl_context
from cdip_connector.core.schemas import models_by_stream_type
from cdip_connector.core.routing import TopicEnum

from app import settings
from app.core import local_logging
from app.core.utils import get_redis_db
from app.subscribers.services import extract_fields_from_message, convert_observation_to_cdip_schema, \
    create_transformed_message, get_key_for_transformed_observation, dispatch_transformed_observation
from app.transform_service.services import get_all_outbound_configs_for_id

local_logging.init()
logger = logging.getLogger(__name__)

APP_ID = 'cdip-routing'

cloud_enabled = settings.CONFLUENT_CLOUD_ENABLED
if cloud_enabled:
    logger.debug(f'Entering Confluent Cloud Enabled Flow')
    cert_path = certifi.where()
    logger.debug(f'cert path: {cert_path}')
    ssl_context = create_ssl_context(cafile=cert_path)

    ''' Currently there are limitations on the basic Confluent Cloud account. Automatic topic creation is restricted
        which requires the disabling of the leader topic. This may have repercussions regarding the durability of this
        process. Any topics that are specified in code and utilized in the flow must be created ahead of time in the
        cloud.
    '''
    logger.debug(f'username: {settings.CONFLUENT_CLOUD_USERNAME}, pw: {settings.CONFLUENT_CLOUD_PASSWORD}')

    app = faust.App(
            APP_ID,
            broker=f'{settings.KAFKA_BROKER}',
            broker_credentials=faust.SASLCredentials(
                username=settings.CONFLUENT_CLOUD_USERNAME,
                password=settings.CONFLUENT_CLOUD_PASSWORD,
                ssl_context=ssl_context,
                mechanism="PLAIN",
            ),
            value_serializer='raw',
            topic_disable_leader=True,
        )
else:
    app = faust.App(
        APP_ID,
        broker=f'{settings.KAFKA_BROKER}',
        value_serializer='raw',
    )

observations_unprocessed_topic = app.topic(TopicEnum.observations_unprocessed.value)
observations_transformed_topic = app.topic(TopicEnum.observations_transformed.value)


async def process_observation(key, message):
    if key:
        logger.info(f'received unprocessed observation with key: {key}')
    logger.debug(f'message received: {message}')
    raw_observation, attributes = extract_fields_from_message(message)
    logger.debug(f'observation: {raw_observation}')
    logger.debug(f'attributes: {attributes}')

    db = get_redis_db()

    observation = convert_observation_to_cdip_schema(raw_observation)

    if observation:
        int_id = observation.integration_id
        destinations = get_all_outbound_configs_for_id(db, int_id)

        for destination in destinations:
            jsonified_data = create_transformed_message(observation, destination, observation.observation_type)

            if destination.id:
                key = get_key_for_transformed_observation(key, destination.id)
            await observations_transformed_topic.send(key=key, value=jsonified_data)


async def process_transformed_observation(key, transformed_message):
    logger.info(f'received transformed observation with key: {key}')
    logger.debug(f'message received: {transformed_message}')
    transformed_observation, attributes = extract_fields_from_message(transformed_message)
    logger.debug(f'observation: {transformed_observation}')
    logger.debug(f'attributes: {attributes}')

    if not transformed_observation:
        logger.warning(f'No observation was obtained from {transformed_message}')
        return
    if not attributes:
        logger.warning(f'No attributes were obtained from {transformed_message}')
        return
    observation_type = attributes.get('observation_type')
    integration_id = attributes.get('integration_id')
    outbound_config_id = attributes.get('outbound_config_id')

    dispatch_transformed_observation(observation_type, outbound_config_id, integration_id, transformed_observation)

@app.agent(observations_unprocessed_topic)
async def process_observations(streaming_data):
    async for key, message in streaming_data.items():
        try:
            await process_observation(key, message)
        # we want to catch all exceptions and repost to a topic to avoid data loss
        except Exception as e:
            logger.exception(f'Exception {e} occurred processing {message}')
            # TODO: determine what we want to do with failed observations


@app.agent(observations_transformed_topic)
async def process_transformed_observations(streaming_transformed_data):
    async for key, transformed_message in streaming_transformed_data.items():
        try:
            await process_transformed_observation(key, transformed_message)
        # we want to catch all exceptions and repost to a topic to avoid data loss
        except Exception as e:
            logger.exception(f'Exception {e} occurred processing {transformed_message}')
            # TODO: determine what we want to do with failed observations


@app.timer(interval=120.0)
async def log_metrics(app):
    m = app.monitor
    metrics_dict = {
        'rebalances': m.rebalances,
        'rebalance_return_avg': m.rebalance_return_avg,
        # 'messages_received_by_topic': m.messages_received_by_topic,
    }
    logger.info(f"Metrics heartbeat for Consumer: {metrics_dict}")


# @app.on_rebalance_start()
# async def on_rebalance_start():
#     pass



if __name__ == '__main__':
    logger.info("Application getting started")
    app.main()
