from abc import ABC, abstractmethod
import settings
import os
from google.cloud import pubsub_v1
from cdip_connector.core import schemas
import json
from typing import Any
from enum import Enum


class Subscriber(ABC):

    @abstractmethod
    def subscribe(self):
        ...


class Publisher(ABC):

    @abstractmethod
    def publish(self, data: dict, extra: dict):
        ...


class GoogleSubscriber(Subscriber):
    def __init__(self, subscription_name):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = settings.GOOGLE_APPLICATION_CREDENTIALS
        project_id = settings.GOOGLE_PUB_SUB_PROJECT_ID
        sub_name = subscription_name
        self.subscriber = pubsub_v1.SubscriberClient()
        subscriber_path = self.subscriber.subscription_path(project_id, sub_name)
        self.future = self.subscriber.subscribe(subscriber_path, callback=self.callback)

    def subscribe(self):
        with self.subscriber:
            try:
                self.future.result()
            except TimeoutError or KeyboardInterrupt:
                self.future.cancel()
                print(f"subscribing cancelled")

    def callback(self, message):
        message.ack()
        # raise NotImplementedError


class GooglePublisher(Publisher):
    def publish(self, data: dict, extra: dict):
        attributes = extra
        future = self.publisher.publish(self.topic_name, json.dumps(data).encode('utf8'), **attributes)
        # ensure the message publishes successfully
        future.result()

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = settings.GOOGLE_APPLICATION_CREDENTIALS
    project_id = settings.GOOGLE_PUB_SUB_PROJECT_ID
    topic_id = settings.STREAMING_TRANSFORMED_TOPIC_NAME
    topic_name = f"projects/{project_id}/topics/{topic_id}"
    publisher = pubsub_v1.PublisherClient()
