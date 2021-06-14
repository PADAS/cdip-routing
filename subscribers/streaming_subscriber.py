import json
import logging
import sys

import services
import settings
from core.pubsub import GoogleSubscriber

logger = logging.getLogger(__name__)


class StreamingGoogleSubscriber(GoogleSubscriber):

    def callback(self, message):
        try:
            observation_type = message.attributes['observation_type']
            observation = json.loads(message.data.decode('utf8'))
            services.post_message_to_transform_service(observation_type, observation, message.message_id)
            message.ack()
        except:
            e = sys.exc_info()[0]
            logger.exception(f"Exception: {e} "
                             f"while processing: {message.message_id} "
                             f"observation: {observation}")
            message.nack()


def execute():
    subscriber = StreamingGoogleSubscriber(settings.STREAMING_SUBSCRIPTION_NAME)
    subscriber.subscribe()


if __name__ == "__main__":
    sys.path.append('code')
    execute()
