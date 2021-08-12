import logging
from enum import Enum

import certifi
import faust
from aiokafka.helpers import create_ssl_context
from cdip_connector.core import schemas
from app.core.local_logging import LocalLogging

from app import settings
from app.core.utils import get_redis_db
from app.subscribers.services import extract_fields_from_message, convert_observation_to_cdip_schema, \
    create_transformed_message, get_key_for_transformed_observation, dispatch_transformed_observation
from app.transform_service.services import get_all_outbound_configs_for_id

LocalLogging()
logger = logging.getLogger(__name__)

TOPIC_PREFIX = 'sintegrate'
APP_ID = 'cdip-routing'


# Todo: refactor this class into module that both sensors and routing can reference
class TopicEnum(str, Enum):
    positions_unprocessed = f'{TOPIC_PREFIX}.positions.unprocessed'
    positions_transformed = f'{TOPIC_PREFIX}.positions.transformed'
    geoevent_unprocessed = f'{TOPIC_PREFIX}.geoevent.unprocessed'
    geoevent_transformed = f'{TOPIC_PREFIX}.geoevent.transformed'
    message_unprocessed = f'{TOPIC_PREFIX}.message.unprocessed'
    message_transformed = f'{TOPIC_PREFIX}.message.transformed'
    cameratrap_unprocessed = f'{TOPIC_PREFIX}.cameratrap.unprocessed'
    cameratrap_transformed = f'{TOPIC_PREFIX}.cameratrap.transformed'


cloud_enabled = settings.CONFLUENT_CLOUD_ENABLED
if cloud_enabled:
    cert_path = certifi.where()
    ssl_context = create_ssl_context(cafile=cert_path)

    ''' Currently there are limitations on the basic Confluent Cloud account. Automatic topic creation is restricted
        which requires the disabling of the leader topic. This may have repercussions regarding the durability of this
        process. Any topics that are specified in code and utilized in the flow must be created ahead of time in the
        cloud.
    '''
    app = faust.App(
            APP_ID,
            broker=f'{settings.KAFKA_BROKER}',
            broker_credentials=faust.SASLCredentials(
                username=settings.CONFLUENT_CLOUD_USERNAME,
                password=settings.CONFLUENT_CLOUD_PASSWORD,
                ssl_context=ssl_context
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

positions_unprocessed_topic = app.topic(TopicEnum.positions_unprocessed.value)
positions_transformed_topic = app.topic(TopicEnum.positions_transformed.value)

cameratrap_unprocessed_topic = app.topic(TopicEnum.cameratrap_unprocessed.value)
cameratrap_transformed_topic = app.topic(TopicEnum.cameratrap_transformed.value)


async def process_observation(key, message, schema: schemas, prefix: schemas.StreamPrefixEnum, transformed_topic):
    logger.info(f'received unprocessed observation with key: {key}')
    logger.debug(f'message received: {message}')
    raw_observation, attributes = extract_fields_from_message(message)
    logger.debug(f'observation: {raw_observation}')
    logger.debug(f'attributes: {attributes}')

    db = get_redis_db()

    observation = convert_observation_to_cdip_schema(raw_observation, schema)

    if observation:
        int_id = observation.integration_id
        destinations = get_all_outbound_configs_for_id(db, int_id)

        for destination in destinations:
            jsonified_data = create_transformed_message(observation, destination, prefix)

            if destination.id:
                key = get_key_for_transformed_observation(key, destination.id)
            await transformed_topic.send(key=key, value=jsonified_data)


async def process_transformed_observation(key, transformed_message, schema: schemas = None):
    logger.info(f'received transformed observation with key: {key}')
    logger.debug(f'message received: {transformed_message}')
    raw_observation, attributes = extract_fields_from_message(transformed_message)
    logger.debug(f'observation: {raw_observation}')
    logger.debug(f'attributes: {attributes}')

    # TODO: May need to create a different schema for the transformed schema since we only have string dict at this point
    # observation = convert_observation_to_cdip_schema(raw_observation, schema)

    if not raw_observation:
        logger.warning(f'No observation was obtained from {transformed_message}')
        return
    if not attributes:
        logger.warning(f'No attributes were obtained from {transformed_message}')
        return
    observation_type = attributes.get('observation_type')
    integration_id = attributes.get('integration_id')
    outbound_config_id = attributes.get('outbound_config_id')

    dispatch_transformed_observation(observation_type, outbound_config_id, integration_id, raw_observation)


@app.agent(positions_unprocessed_topic)
async def process_position(streaming_data):
    async for key, message in streaming_data.items():
        try:
            await process_observation(key, message, schemas.Position, schemas.StreamPrefixEnum.position, positions_transformed_topic)
        # we want to catch all exceptions and repost to a topic to avoid data loss
        except Exception as e:
            logger.exception(f'Exception {e} occurred processing {message}')
            # TODO: determine what we want to do with failed observations
            # await positions_unprocessed_topic.send(key=key, value=message)


@app.agent(positions_transformed_topic)
async def process_transformed_position(streaming_transformed_data):
    async for key, transformed_message in streaming_transformed_data.items():
        try:
            await process_transformed_observation(key, transformed_message, schemas.Position)

        # we want to catch all exceptions and repost to a topic to avoid data loss
        except Exception as e:
            logger.exception(f'Exception {e} occurred processing {transformed_message}')
            # TODO: determine what we want to do with failed observations
            # await positions_transformed_topic.send(key=key, value=transformed_message)


@app.agent(cameratrap_unprocessed_topic)
async def process_cameratrap(streaming_data):
    async for key, message in streaming_data.items():
        try:
            await process_observation(key, message, schemas.CameraTrap, schemas.StreamPrefixEnum.camera_trap, cameratrap_transformed_topic)
        # we want to catch all exceptions and repost to a topic to avoid data loss
        except Exception as e:
            logger.exception(f'Exception {e} occurred processing {message}')
            # TODO: determine what we want to do with failed observations


@app.agent(cameratrap_transformed_topic)
async def process_transformed_cameratrap(streaming_transformed_data):
    async for key, transformed_message in streaming_transformed_data.items():
        try:
            await process_transformed_observation(key, transformed_message, schemas.CameraTrap)
        # we want to catch all exceptions and repost to a topic to avoid data loss
        except Exception as e:
            logger.exception(f'Exception {e} occurred processing {transformed_message}')
            # TODO: determine what we want to do with failed observations
            # await positions_transformed_topic.send(key=key, value=transformed_message)

if __name__ == '__main__':
    logger.info("Application getting started")
    app.main()