import json
import logging
import sys

from cdip_connector.core import schemas

import services
from app import settings
from app.core.pubsub import GoogleSubscriber

logger = logging.getLogger(__name__)


class StreamingTransformedGoogleSubscriber(GoogleSubscriber):

    def callback(self, message):
        try:
            observation = json.loads(message.data.decode('utf8'))
            observation_type = schemas.StreamPrefixEnum(message.attributes['observation_type'])
            outbound_config_id = message.attributes['outbound_config_id']
            services.dispatch_transformed_observation(observation_type, outbound_config_id, observation)
            message.ack()
        except:
            e = sys.exc_info()[0]
            logger.exception(f"Exception: {e} "
                             f"while processing: {message.message_id} "
                             f"observation: {observation}")
            message.nack()


def execute():
    subscriber = StreamingTransformedGoogleSubscriber(settings.STREAMING_TRANSFORMED_SUBSCRIPTION_NAME)
    subscriber.subscribe()


if __name__ == "__main__":
    execute()
