import json
import os

import uvicorn
import walrus
from cdip_connector.core import schemas
from fastapi import Depends
from fastapi import FastAPI
from google.cloud import pubsub_v1

import settings
from core.utils import get_redis_db
from transform_service.services import get_all_outbound_configs_for_id, transform_observation

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = settings.GOOGLE_APPLICATION_CREDENTIALS

project_id = settings.GOOGLE_PUB_SUB_PROJECT_ID

topic_id = settings.STREAMING_TRANSFORMED_TOPIC_NAME

topic_name = f"projects/{project_id}/topics/{topic_id}"

publisher = pubsub_v1.PublisherClient()


def publish(msg):
    attributes = msg.attributes
    future = publisher.publish(topic_name, msg.data.encode('utf8'), **attributes)
    # ensure the message publishes successfully
    future.result()


class Message:

    def __init__(self, data='', attributes={}):
        self.data = data
        self.attributes = attributes


app = FastAPI()


@app.post("/streaming/position", status_code=201)
async def transform_data(position: schemas.Position,
                         db: walrus.Database = Depends(get_redis_db)):

    int_id = position.integration_id
    destinations = get_all_outbound_configs_for_id(db, int_id)

    for destination in destinations:
        transformed_position = transform_observation(schemas.StreamPrefixEnum.position, destination, position)
        print(transformed_position)
        msg = Message(data=json.dumps(transformed_position),
                      attributes={'observation_type': schemas.StreamPrefixEnum.position.value,
                                  'outbound_config_id': str(destination.id)})
        publish(msg)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8200, reload=True)
