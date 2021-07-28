import json
import logging
import sys
from app import settings

import faust
from cdip_connector.core import schemas
from enum import Enum
from uuid import UUID

from app.core.utils import get_redis_db
from app.subscribers.services import dispatch_transformed_observation
from app.transform_service.services import get_all_outbound_configs_for_id, transform_observation

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
        broker=f'{settings.KAFKA_BROKER}',
        value_serializer='raw',
    )

positions_unprocessed_topic = app.topic(TopicEnum.positions_unprocessed.value)
positions_transformed_topic = app.topic(TopicEnum.positions_transformed.value)

cameratrap_unprocessed_topic = app.topic(TopicEnum.cameratrap_unprocessed.value)
cameratrap_transformed_topic = app.topic(TopicEnum.cameratrap_transformed.value)


def convert_observation_to_position(observation):
    positions = [observation]
    positions, errors = schemas.get_validated_objects(positions, schemas.Position)
    if len(positions) > 0:
        return positions[0]
    else:
        logger.warning(f'unable to validate position: {observation} errors: {errors}')
        return None


def convert_observation_to_cameratrap(observation):
    payloads = [observation]
    cameratrap_payloads, errors = schemas.get_validated_objects(payloads, schemas.CameraTrap)
    if len(cameratrap_payloads) > 0:
        return cameratrap_payloads[0]
    else:
        logger.warning(f'unable to validate position: {observation} errors: {errors}')
        return None


def create_message(attributes, observation):
    message = {'attributes': attributes,
               'data': observation}
    return message


def create_transformed_message(observation, destination, schema: schemas.StreamPrefixEnum):
    transformed_observation = transform_observation(schema, destination, observation)
    logger.debug(f'Transformed observation: {transformed_observation}')

    attributes = {'observation_type': schemas.StreamPrefixEnum.position.value,
                  'outbound_config_id': str(destination.id)}

    transformed_message = create_message(attributes, transformed_observation)

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


def get_key_for_transformed_observation(current_key: bytes, destination_id: UUID):
    # caller must provide key and destination_id must be present in order to create for transformed observation
    if current_key is None or destination_id is None:
        return current_key
    else:
        new_key = f"{current_key.decode('utf-8')}.{str(destination_id)}"
        return new_key.encode('utf-8')


@app.agent(positions_unprocessed_topic)
async def process_position(streaming_data):
    async for key, message in streaming_data.items():
        try:
            logger.info(f'received unprocessed position with key: {key}')
            logger.debug(f'message received: {message}')
            observation, attributes = extract_fields_from_message(message)
            logger.debug(f'observation: {observation}')
            logger.debug(f'attributes: {attributes}')

            db = get_redis_db()

            position = convert_observation_to_position(observation)

            if position:
                int_id = position.integration_id
                destinations = get_all_outbound_configs_for_id(db, int_id)

                for destination in destinations:
                    jsonified_data = create_transformed_message(schemas.StreamPrefixEnum.position, position, destination)

                    if destination.id:
                        key = get_key_for_transformed_observation(key, destination.id)
                    await positions_transformed_topic.send(key=key, value=jsonified_data)
        # we want to catch all exceptions and repost to a topic to avoid data loss
        except Exception as e:
            logger.exception(f'Exception {e} occurred processing {message}')
            # TODO: determine what we want to do with failed observations
            # await positions_unprocessed_topic.send(key=key, value=message)


@app.agent(positions_transformed_topic)
async def process_transformed_position(streaming_transformed_data):
    async for key, transformed_message in streaming_transformed_data.items():
        try:
            logger.info(f'received transformed position with key: {key}')
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
        except Exception as e:
            logger.exception(f'Exception {e} occurred processing {transformed_message}')
            # TODO: determine what we want to do with failed observations
            # await positions_transformed_topic.send(key=key, value=transformed_message)


@app.agent(cameratrap_unprocessed_topic)
async def process_cameratrap(streaming_data):
    async for key, message in streaming_data.items():
        try:
            logger.info(f'received unprocessed position with key: {key}')
            logger.debug(f'message received: {message}')
            observation, attributes = extract_fields_from_message(message)
            logger.debug(f'observation: {observation}')
            logger.debug(f'attributes: {attributes}')

            db = get_redis_db()

            camera_trap_payload = convert_observation_to_cameratrap(observation)

            if camera_trap_payload:
                int_id = camera_trap_payload.integration_id
                destinations = get_all_outbound_configs_for_id(db, int_id)

                for destination in destinations:
                    jsonified_data = create_transformed_message(schemas.StreamPrefixEnum.camera_trap, camera_trap_payload, destination)

                    if destination.id:
                        key = get_key_for_transformed_observation(key, destination.id)
                    # await cameratrap_transformed_topic.send(key=key, value=jsonified_data)
        # we want to catch all exceptions and repost to a topic to avoid data loss
        except Exception as e:
            logger.exception(f'Exception {e} occurred processing {message}')
            # TODO: determine what we want to do with failed observations

if __name__ == '__main__':
    logger.info("Application getting started")
    app.main()