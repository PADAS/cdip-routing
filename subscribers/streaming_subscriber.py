import json
import logging
import os
import sys

import requests
from cdip_connector.core import schemas
from google.cloud import pubsub_v1

import settings

logger = logging.getLogger(__name__)


def callback(message):
    try:
        observation_type = message.attributes['observation_type']
        # send message to transform service and wait for reply
        observation = json.loads(message.data.decode('utf8'))
        print(f"Received observation: {observation}")
        if observation_type == schemas.StreamPrefixEnum.position:
            response = requests.post(settings.TRANSFORM_SERVICE_POSITIONS_ENDPOINT, json=observation)
        else:
            logger.warning(f'Observation: {observation} type: {observation_type} is not supported')
            # TODO how to handle unsupported observation types
            message.ack()
        if response.ok:
            print(f"message {message.message_id} ack'd")
            message.ack()
        else:
            # TODO how to handle bad Transform Service responses ?
            logger.error(f"Transform Service Error response: {response} "
                         f"while processing: {message.message_id} "
                         f"observation: {observation}")
            message.ack()
    except:
        # TODO how to handle exceptions ?
        e = sys.exc_info()[0]
        logger.exception(f"Exception: {e} "
                         f"while processing: {message.message_id} "
                         f"observation: {observation}")
        message.ack()


def execute():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = settings.GOOGLE_APPLICATION_CREDENTIALS

    project_id = settings.GOOGLE_PUB_SUB_PROJECT_ID
    sub_name = settings.STREAMING_SUBSCRIPTION_NAME

    subscriber = pubsub_v1.SubscriberClient()

    subscriber_path = subscriber.subscription_path(project_id, sub_name)

    future = subscriber.subscribe(subscriber_path, callback=callback)

    with subscriber:
        try:
            future.result()
        except TimeoutError or KeyboardInterrupt:
            future.cancel()
            print(f"subscribing cancelled")


if __name__ == "__main__":
    execute()
