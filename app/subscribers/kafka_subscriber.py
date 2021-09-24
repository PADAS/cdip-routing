import logging

import certifi
import faust
from aiokafka.helpers import create_ssl_context
from cdip_connector.core import schemas
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

positions_unprocessed_topic = app.topic(TopicEnum.positions_unprocessed.value)
positions_transformed_topic = app.topic(TopicEnum.positions_transformed.value)

cameratrap_unprocessed_topic = app.topic(TopicEnum.cameratrap_unprocessed.value)
cameratrap_transformed_topic = app.topic(TopicEnum.cameratrap_transformed.value)

geoevents_unprocessed_topic = app.topic(TopicEnum.geoevent_unprocessed.value)
geoevents_transformed_topic = app.topic(TopicEnum.geoevent_transformed.value)


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


@app.agent(geoevents_unprocessed_topic)
async def process_geoevents(streaming_data):
    async for key, message in streaming_data.items():
        try:
            await process_observation(key, message, schemas.GeoEvent, schemas.StreamPrefixEnum.geoevent, geoevents_transformed_topic)
        # we want to catch all exceptions and repost to a topic to avoid data loss
        except Exception as e:
            logger.exception(f'Exception {e} occurred processing {message}')
            # TODO: determine what we want to do with failed observations


@app.agent(geoevents_transformed_topic)
async def process_transformed_geoevents(streaming_transformed_data):
    async for key, transformed_message in streaming_transformed_data.items():
        try:
            await process_transformed_observation(key, transformed_message, schemas.GeoEvent)
        # we want to catch all exceptions and repost to a topic to avoid data loss
        except Exception as e:
            logger.exception(f'Exception {e} occurred processing {transformed_message}')
            # TODO: determine what we want to do with failed observations

if __name__ == '__main__':
    logger.info("Application getting started")
    app.main()
