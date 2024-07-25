import asyncio
import backoff
import aiohttp
import json
import logging
from opentelemetry.trace import SpanKind
from gcloud.aio import pubsub
from app.core import tracing, settings


logger = logging.getLogger(__name__)


@backoff.on_exception(
    backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=20
)
async def send_message_to_gcp_pubsub_dispatcher(
    message, attributes, destination, broker_config, ordering_key=""
):
    with tracing.tracer.start_as_current_span(  # Trace observations with Open Telemetry
        "routing_service.send_message_to_gcp_pubsub_dispatcher",
        kind=SpanKind.PRODUCER,
    ) as current_span:
        destination_id_str = str(destination.id)
        current_span.set_attribute("destination_id", destination_id_str)
        # Propagate OTel context in message attributes
        tracing_context = json.dumps(
            tracing.pubsub_instrumentation.build_context_headers(),
            default=str,
        )
        attributes["tracing_context"] = tracing_context
        timeout_settings = aiohttp.ClientTimeout(total=60.0)
        async with aiohttp.ClientSession(
            raise_for_status=True, timeout=timeout_settings
        ) as session:
            client = pubsub.PublisherClient(session=session)
            # Get the topic name from config or use a default naming convention
            topic_name = broker_config.get(
                "topic",
                f"destination-{destination_id_str}-{settings.GCP_ENVIRONMENT}",  # Try with a default name for older integrations
            ).strip()
            current_span.set_attribute("topic", topic_name)
            topic = client.topic_path(settings.GCP_PROJECT_ID, topic_name)
            # Serialize UUIDs or other complex types to string
            attributes_clean = json.loads(json.dumps(attributes, default=str))
            ordering_key_clean = str(ordering_key)
            # ToDo: enable ordering key once we update the infra to support it
            #messages = [pubsub.PubsubMessage(message, ordering_key=ordering_key_clean, **attributes_clean)]
            messages = [pubsub.PubsubMessage(message, **attributes_clean)]
            logger.info(f"Sending observation to PubSub topic {topic_name}..")
            try:
                response = await client.publish(
                    topic, messages, timeout=int(timeout_settings.total)
                )
            except Exception as e:
                error_msg = (
                    f"Error sending observation to PubSub topic {topic_name}: {e}."
                )
                logger.exception(error_msg)
                current_span.set_attribute("error", error_msg)
                raise e
            else:
                logger.info(f"Observation sent successfully.")
                logger.debug(f"GCP PubSub response: {response}")
        current_span.add_event(
            name="routing_service.transformed_observation_sent_to_dispatcher"
        )


async def send_observation_to_dead_letter_topic(transformed_observation, attributes):
    with tracing.tracer.start_as_current_span(
        "send_message_to_dead_letter_topic", kind=SpanKind.CLIENT
    ) as current_span:
        print(f"Forwarding observation to dead letter topic: {transformed_observation}")
        # Publish to another PubSub topic
        connect_timeout, read_timeout = settings.DEFAULT_REQUESTS_TIMEOUT
        timeout_settings = aiohttp.ClientTimeout(
            sock_connect=connect_timeout, sock_read=read_timeout
        )
        async with aiohttp.ClientSession(
            raise_for_status=True, timeout=timeout_settings
        ) as session:
            client = pubsub.PublisherClient(session=session)
            # Get the topic
            topic_name = settings.DEAD_LETTER_TOPIC
            current_span.set_attribute("topic", topic_name)
            topic = client.topic_path(settings.GCP_PROJECT_ID, topic_name)
            # Prepare the payload
            binary_payload = json.dumps(transformed_observation, default=str).encode(
                "utf-8"
            )
            messages = [pubsub.PubsubMessage(binary_payload, **attributes)]
            logger.info(f"Sending observation to PubSub topic {topic_name}..")
            try:  # Send to pubsub
                response = await client.publish(topic, messages)
            except Exception as e:
                logger.exception(
                    f"Error sending observation to dead letter topic {topic_name}: {e}. Please check if the topic exists or review settings."
                )
                raise e
            else:
                logger.info(f"Observation sent to the dead letter topic successfully.")
                logger.debug(f"GCP PubSub response: {response}")

        current_span.set_attribute("is_sent_to_dead_letter_queue", True)
        current_span.add_event(
            name="routing_service.observation_sent_to_dead_letter_queue"
        )
