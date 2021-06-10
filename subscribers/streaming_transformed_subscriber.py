import json
import logging
import os
import sys

from cdip_connector.core import schemas
from google.cloud import pubsub_v1

import settings
import utils

logger = logging.getLogger(__name__)


def callback(message):
    try:
        observation = json.loads(message.data.decode('utf8'))
        observation_type = schemas.StreamPrefixEnum(message.attributes['observation_type'])
        outbound_config_id = message.attributes['outbound_config_id']
        utils.dispatch_transformed_observation(observation_type, outbound_config_id, observation)
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
    sub_name = settings.STREAMING_TRANSFORMED_SUBSCRIPTION_NAME

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
