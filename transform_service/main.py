import json
import os

import uvicorn
import walrus
from cdip_connector.core import schemas
from fastapi import Depends
from fastapi import FastAPI
from google.cloud import pubsub_v1

import settings
from core.pubsub import Publisher, GooglePublisher
from core.utils import get_redis_db
from transform_service.services import get_all_outbound_configs_for_id, transform_observation

app = FastAPI()


def get_publisher():
    return GooglePublisher()


@app.post("/streaming/position", status_code=201)
async def transform_data(position: schemas.Position,
                         db: walrus.Database = Depends(get_redis_db),
                         publisher: Publisher = Depends(get_publisher)):

    int_id = position.integration_id
    destinations = get_all_outbound_configs_for_id(db, int_id)

    for destination in destinations:
        transformed_position = transform_observation(schemas.StreamPrefixEnum.position, destination, position)
        print(transformed_position)
        extra = {'observation_type': schemas.StreamPrefixEnum.position.value,
                 'outbound_config_id': str(destination.id)}
        publisher.publish(transformed_position, extra)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8200, reload=True)
