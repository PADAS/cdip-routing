import json

import faust
from cdip_connector.core import schemas

from core.utils import get_redis_db
from subscribers.services import dispatch_transformed_observation
from transform_service.services import get_all_outbound_configs_for_id, transform_observation


app = faust.App(
        'hello-world',
        broker='kafka://localhost:9092',
        value_serializer='raw',
    )

streaming_topic = app.topic('streaming-topic')
streaming_transformed_topic = app.topic('streaming-transformed-topic')


@app.agent(streaming_topic)
async def process_data(streaming_data):
    async for message in streaming_data:
        print(message)
        decoded_message = json.loads(message.decode('utf-8'))
        attributes = decoded_message['attributes']
        observation_type = attributes['observation_type']
        db = get_redis_db()
        if observation_type == schemas.StreamPrefixEnum.position.value:
            position = decoded_message['data']
            print(f'Received observation: {position}')
            # position_json = json.dumps(position)
            positions = [position]
            positions, errors = schemas.get_validated_objects(positions, schemas.Position)
            position = positions[0]

            int_id = position.integration_id
            destinations = get_all_outbound_configs_for_id(db, int_id)
            for destination in destinations:
                transformed_position = transform_observation(schemas.StreamPrefixEnum.position, destination, position)
                print(f'Transformed observation: {transformed_position}')
                extra = {'observation_type': schemas.StreamPrefixEnum.position.value,
                         'outbound_config_id': str(destination.id)}
                transformed_message = {'attributes': extra,
                           'data': transformed_position}
                jsonified_data = json.dumps(transformed_message, default=str)
                await streaming_transformed_topic.send(value=jsonified_data)


@app.agent(streaming_transformed_topic)
async def process_transformed_data(streaming_transformed_data):
    async for transformed_message in streaming_transformed_data:
        print(transformed_message)
        decoded_message = json.loads(transformed_message.decode('utf-8'))
        observation = decoded_message['data']
        attributes = decoded_message['attributes']
        observation_type = attributes['observation_type']
        outbound_config_id = attributes['outbound_config_id']
        dispatch_transformed_observation(observation_type, outbound_config_id, observation)

if __name__ == '__main__':
    print("Application getting started")
    app.main()