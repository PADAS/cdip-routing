import json
import logging
import sys

import faust
from cdip_connector.core import schemas
from enum import Enum

from core.utils import get_redis_db
from subscribers.services import dispatch_transformed_observation
from transform_service.services import get_all_outbound_configs_for_id, transform_observation

logger = logging.getLogger(__name__)

APP_NAME = 'sintegrate'


# Todo: refactor this class into module that both sensors and routing can reference
class TopicEnum(str, Enum):
    positions_unprocessed = f'{APP_NAME}.positions.unprocessed'
    positions_transformed = f'{APP_NAME}.positions.transformed'
    geoevent_unprocessed = f'{APP_NAME}.geoevent.unprocessed'
    geoevent_transformed = f'{APP_NAME}.geoevent.transformed'
    message_unprocessed = f'{APP_NAME}.message.unprocessed'
    message_transformed = f'{APP_NAME}.message.transformed'
    cameratrap_unprocessed = f'{APP_NAME}.cameratrap.unprocessed'
    cameratrap_transformed = f'{APP_NAME}.cameratrap.transformed'


app = faust.App(
        'cdip-routing',
        broker='kafka://localhost:9092',
        value_serializer='raw',
    )

positions_unprocessed_topic = app.topic(TopicEnum.positions_unprocessed.value)
positions_transformed_topic = app.topic(TopicEnum.positions_transformed.value)


def convert_observation_to_position(position):
    positions = [position]
    positions, errors = schemas.get_validated_objects(positions, schemas.Position)
    if len(positions) > 0:
        return positions[0]
    else:
        logger.warning(f'unable to validate position: {position} errors: {errors}')
        return None


def create_message(attributes, observation):
    message = {'attributes': attributes,
               'data': observation}
    return message


def create_transformed_message(position, destination):
    transformed_position = transform_observation(schemas.StreamPrefixEnum.position, destination, position)
    logger.debug(f'Transformed observation: {transformed_position}')

    attributes = {'observation_type': schemas.StreamPrefixEnum.position.value,
                  'outbound_config_id': str(destination.id)}

    transformed_message = create_message(attributes, transformed_position)

    jsonified_data = json.dumps(transformed_message, default=str)
    return jsonified_data


def extract_fields_from_message(message):
    decoded_message = json.loads(message.decode('utf-8'))
    if decoded_message:
        observation = decoded_message.get('data')
        attributes = decoded_message.get('attributes')
    else:
        logger.warning(f'message: {message} contained no payload')
        return None, None
    return observation, attributes


@app.agent(positions_unprocessed_topic)
async def process_data(streaming_data):
    async for key, message in streaming_data.items():
        try:
            print(f'received unprocessed position with key: {key}')
            logger.debug(f'message received: {message}')
            observation, attributes = extract_fields_from_message(message)
            logger.debug(f'observation: {observation}')
            logger.debug(f'attributes: {attributes}')
            observation_type = attributes.get('observation_type')
            db = get_redis_db()

            if observation_type == schemas.StreamPrefixEnum.position.value:
                position = convert_observation_to_position(observation)

                if position:
                    int_id = position.integration_id
                    destinations = get_all_outbound_configs_for_id(db, int_id)

                    for destination in destinations:
                        jsonified_data = create_transformed_message(position, destination)
                        await positions_transformed_topic.send(key=key, value=jsonified_data)
        # we want to catch all exceptions and repost to a topic to avoid data loss
        except:
            e = sys.exc_info()[0]
            logger.exception(f'Exception {e} occurred processing {message}')
            await positions_unprocessed_topic.send(value=message)


@app.agent(positions_transformed_topic)
async def process_transformed_data(streaming_transformed_data):
    async for key, transformed_message in streaming_transformed_data.items():
        try:
            print(f'received transformed position with key: {key}')
            logger.debug(f'message received: {transformed_message}')
            observation, attributes = extract_fields_from_message(transformed_message)
            logger.debug(f'observation: {observation}')
            logger.debug(f'attributes: {attributes}')
            observation, attributes = extract_fields_from_message(transformed_message)
            if not observation:
                logger.warning(f'No observation was obtained from {transformed_message}')
                return
            if not attributes:
                logger.warning(f'No attributes were obtained from {transformed_message}')
                return
            observation_type = attributes.get('observation_type')
            outbound_config_id = attributes.get('outbound_config_id')
            dispatch_transformed_observation(observation_type, outbound_config_id, observation)
        # we want to catch all exceptions and repost to a topic to avoid data loss
        except:
            e = sys.exc_info()[0]
            logger.exception(f'Exception {e} occurred processing {transformed_message}')
            await positions_transformed_topic.send(value=transformed_message)

if __name__ == '__main__':
    logger.info("Application getting started")
    app.main()