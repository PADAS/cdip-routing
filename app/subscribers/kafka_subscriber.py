import logging
from datetime import datetime

import certifi
import faust
from aiokafka.helpers import create_ssl_context
from cdip_connector.core.routing import TopicEnum

from cdip_connector.core import cdip_settings
from google.cloud.trace_v2 import TraceServiceClient, AttributeValue, types
from opentelemetry.trace import Link

from app.core.local_logging import DEFAULT_LOGGING, ExtraKeys
from app.core.utils import ReferenceDataError, DispatcherException
from app.subscribers.services import (
    extract_fields_from_message,
    convert_observation_to_cdip_schema,
    create_transformed_message,
    get_key_for_transformed_observation,
    dispatch_transformed_observation,
    wait_until_retry_at,
    update_attributes_for_transformed_retry,
    create_retry_message,
    update_attributes_for_unprocessed_retry,
)
from app.transform_service.services import (
    get_all_outbound_configs_for_id,
    update_observation_with_device_configuration,
)

from opentelemetry.trace.span import SpanContext, NonRecordingSpan
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor
)
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
import app.settings as routing_settings

logger = logging.getLogger(__name__)

provider = TracerProvider()
trace.set_tracer_provider(provider)

# cloud_trace_client = TraceServiceClient(credentials=)
cloud_trace_exporter = CloudTraceSpanExporter(project_id=cdip_settings.GOOGLE_PUB_SUB_PROJECT_ID)
# TODO: use the opentelemetry.sdk.trace.export.BatchSpanProcessor for real production purposes to optimize performance
# processor = BatchSpanProcessor(ConsoleSpanExporter())
# provider.add_span_processor(processor)
# provider.add_span_processor(cloud_trace_exporter)
trace.get_tracer_provider().add_span_processor(
    SimpleSpanProcessor(cloud_trace_exporter)
)

tracer = trace.get_tracer(__name__)

APP_ID = "cdip-routing"

cloud_enabled = cdip_settings.CONFLUENT_CLOUD_ENABLED
if cloud_enabled:
    logger.debug(f"Entering Confluent Cloud Enabled Flow")
    cert_path = certifi.where()
    logger.debug(f"cert path: {cert_path}")
    ssl_context = create_ssl_context(cafile=cert_path)

    """ Currently there are limitations on the basic Confluent Cloud account. Automatic topic creation is restricted
        which requires the disabling of the leader topic. This may have repercussions regarding the durability of this
        process. Any topics that are specified in code and utilized in the flow must be created ahead of time in the
        cloud.
    """
    logger.debug(
        f"username: {cdip_settings.CONFLUENT_CLOUD_USERNAME}, pw: {cdip_settings.CONFLUENT_CLOUD_PASSWORD}"
    )

    app = faust.App(
        APP_ID,
        broker=f"{cdip_settings.KAFKA_BROKER}",
        broker_credentials=faust.SASLCredentials(
            username=cdip_settings.CONFLUENT_CLOUD_USERNAME,
            password=cdip_settings.CONFLUENT_CLOUD_PASSWORD,
            ssl_context=ssl_context,
            mechanism="PLAIN",
        ),
        value_serializer="raw",
        logging_config=DEFAULT_LOGGING,
        topic_disable_leader=True,
    )
else:
    app = faust.App(
        APP_ID,
        broker=f"{cdip_settings.KAFKA_BROKER}",
        value_serializer="raw",
        logging_config=DEFAULT_LOGGING,
    )

observations_unprocessed_topic = app.topic(TopicEnum.observations_unprocessed.value)
observations_unprocessed_retry_short_topic = app.topic(
    TopicEnum.observations_unprocessed_retry_short.value
)
observations_unprocessed_retry_long_topic = app.topic(
    TopicEnum.observations_unprocessed_retry_long.value
)
observations_unprocessed_deadletter = app.topic(
    TopicEnum.observations_unprocessed_deadletter.value
)
observations_transformed_topic = app.topic(TopicEnum.observations_transformed.value)
observations_transformed_retry_short_topic = app.topic(
    TopicEnum.observations_transformed_retry_short.value
)
observations_transformed_retry_long_topic = app.topic(
    TopicEnum.observations_transformed_retry_long.value
)
observations_transformed_deadletter = app.topic(
    TopicEnum.observations_transformed_deadletter.value
)

topics_dict = {
    TopicEnum.observations_unprocessed.value: observations_unprocessed_topic,
    TopicEnum.observations_unprocessed_retry_short: observations_unprocessed_retry_short_topic,
    TopicEnum.observations_unprocessed_retry_long: observations_unprocessed_retry_long_topic,
    TopicEnum.observations_unprocessed_deadletter: observations_unprocessed_deadletter,
    TopicEnum.observations_transformed: observations_transformed_topic,
    TopicEnum.observations_transformed_retry_short: observations_transformed_retry_short_topic,
    TopicEnum.observations_transformed_retry_long: observations_transformed_retry_long_topic,
    TopicEnum.observations_transformed_deadletter: observations_transformed_deadletter,
}


async def process_observation(key, message):
    try:
        logger.debug(f"message received: {message}")
        raw_observation, attributes = extract_fields_from_message(message)
        logger.debug(f"observation: {raw_observation}")
        logger.debug(f"attributes: {attributes}")

        observation = convert_observation_to_cdip_schema(raw_observation)
        extra = {
                ExtraKeys.DeviceId: observation.device_id,
                ExtraKeys.InboundIntId: str(observation.integration_id),
                ExtraKeys.StreamType: observation.observation_type,
            }
        logger.info(
            "received unprocessed observation",
            extra=extra,
        )
    except Exception as e:
        logger.exception(
            f"Exception occurred prior to processing observation",
            extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.Observation: message},
        )
        raise e
    try:
        prop = TraceContextTextMapPropagator()
        carrier = attributes.get(ExtraKeys.Carrier)
        parent_context = prop.extract(carrier=carrier) if carrier else None
        with tracer.start_as_current_span(name="unprocessed_observation_span",
                                          context=parent_context) as unprocessed_observation_span:
            # prop.inject(carrier=carrier)
            for extra_key, extra_value in extra.items():
                unprocessed_observation_span.set_attribute(extra_key, extra_value)
            if observation:
                unprocessed_observation_span.add_event(name="begin_update_observation_with_device_configuration")
                observation = await update_observation_with_device_configuration(
                    observation
                )
                unprocessed_observation_span.add_event(name="end_update_observation_with_device_configuration")
                int_id = observation.integration_id
                unprocessed_observation_span.add_event(name="begin_get_all_outbound_configs_for_id")
                destinations = get_all_outbound_configs_for_id(
                    int_id, observation.device_id
                )
                unprocessed_observation_span.add_event(name="end_get_all_outbound_configs_for_id")

                for destination in destinations:
                    jsonified_data = create_transformed_message(
                        observation=observation,
                        destination=destination,
                        prefix=observation.observation_type,
                        trace_carrier=carrier
                    )
                    if jsonified_data:
                        key = get_key_for_transformed_observation(key, destination.id)
                        await observations_transformed_topic.send(
                            key=key, value=jsonified_data
                        )
    except ReferenceDataError:
        logger.exception(
            f"External error occurred obtaining reference data for observation",
            extra={
                ExtraKeys.AttentionNeeded: True,
                ExtraKeys.DeviceId: observation.device_id,
                ExtraKeys.InboundIntId: observation.integration_id,
                ExtraKeys.StreamType: observation.observation_type,
            },
        )
        await process_failed_unprocessed_observation(key, message)

    except Exception:
        logger.exception(
            f"Unexpected internal exception occurred processing observation",
            extra={
                ExtraKeys.AttentionNeeded: True,
                ExtraKeys.DeadLetter: True,
                ExtraKeys.DeviceId: observation.device_id,
                ExtraKeys.InboundIntId: observation.integration_id,
                ExtraKeys.StreamType: observation.observation_type,
            },
        )
        # Unexpected internal errors will be redirected straight to deadletter
        await observations_unprocessed_deadletter.send(value=message)


async def process_transformed_observation(key, transformed_message):
    try:
        transformed_observation, attributes = extract_fields_from_message(
            transformed_message
        )


        observation_type = attributes.get(ExtraKeys.StreamType)
        device_id = attributes.get(ExtraKeys.DeviceId)
        integration_id = attributes.get(ExtraKeys.InboundIntId)
        outbound_config_id = attributes.get(ExtraKeys.OutboundIntId)
        retry_attempt: int = attributes.get(ExtraKeys.RetryAttempt) or 0

        extra = {
            ExtraKeys.DeviceId: device_id,
            ExtraKeys.InboundIntId: integration_id,
            ExtraKeys.OutboundIntId: outbound_config_id,
            ExtraKeys.StreamType: observation_type,
            ExtraKeys.RetryAttempt: retry_attempt,
        }

        carrier = attributes.get(ExtraKeys.Carrier)

        prop = TraceContextTextMapPropagator()
        parent_context = prop.extract(carrier=carrier) if carrier else None

        logger.debug(f"transformed_observation: {transformed_observation}")
        logger.info(
            "received transformed observation",
            extra=extra,
        )

        logger.debug("transformed_observation", extra=dict(transformed_observation=transformed_observation))

    except Exception as e:
        logger.exception(
            f"Exception occurred prior to dispatching transformed observation",
            extra={
                ExtraKeys.AttentionNeeded: True,
                ExtraKeys.Observation: transformed_message,
            },
        )
        raise e
    try:
        with tracer.start_as_current_span(name="transformed_observation_span",
                                          context=parent_context) as transformed_observation_span:
            for extra_key, extra_value in extra.items():
                transformed_observation_span.set_attribute(extra_key, extra_value)
            logger.info(
                "Dispatching for transformed observation.",
                extra={
                    ExtraKeys.InboundIntId: integration_id,
                    ExtraKeys.OutboundIntId: outbound_config_id,
                    ExtraKeys.StreamType: observation_type,
                },
            )

            dispatch_transformed_observation(
                stream_type=observation_type,
                outbound_config_id=outbound_config_id,
                inbound_int_id=integration_id,
                observation=transformed_observation,
                span=transformed_observation_span
            )
            transformed_observation_span.end()

    except (DispatcherException, ReferenceDataError):
        logger.exception(
            f"External error occurred processing transformed observation",
            extra={
                ExtraKeys.AttentionNeeded: True,
                ExtraKeys.DeviceId: device_id,
                ExtraKeys.InboundIntId: integration_id,
                ExtraKeys.OutboundIntId: outbound_config_id,
                ExtraKeys.StreamType: observation_type,
            },
        )
        await process_failed_transformed_observation(key, transformed_message)

    except Exception:
        logger.exception(
            f"Unexpected internal error occurred processing transformed observation",
            extra={
                ExtraKeys.AttentionNeeded: True,
                ExtraKeys.DeadLetter: True,
                ExtraKeys.DeviceId: device_id,
                ExtraKeys.InboundIntId: integration_id,
                ExtraKeys.OutboundIntId: outbound_config_id,
                ExtraKeys.StreamType: observation_type,
            },
        )
        # Unexpected internal errors will be redirected straight to deadletter
        await observations_transformed_deadletter.send(value=transformed_message)


async def process_failed_transformed_observation(key, transformed_message):
    try:
        transformed_observation, attributes = extract_fields_from_message(
            transformed_message
        )
        attributes = update_attributes_for_transformed_retry(attributes)
        observation_type = attributes.get("observation_type")
        device_id = attributes.get("device_id")
        integration_id = attributes.get("integration_id")
        outbound_config_id = attributes.get("outbound_config_id")
        retry_topic_str = attributes.get("retry_topic")
        retry_attempt = attributes.get("retry_attempt")
        retry_transformed_message = create_retry_message(
            transformed_observation, attributes
        )
        retry_topic: faust.Topic = topics_dict.get(retry_topic_str)
        extra_dict = {
            ExtraKeys.DeviceId: device_id,
            ExtraKeys.InboundIntId: integration_id,
            ExtraKeys.OutboundIntId: outbound_config_id,
            ExtraKeys.StreamType: observation_type,
            ExtraKeys.RetryTopic: retry_topic_str,
            ExtraKeys.RetryAttempt: retry_attempt,
            ExtraKeys.Observation: transformed_observation,
        }
        if retry_topic_str != TopicEnum.observations_transformed_deadletter.value:
            logger.info(
                "Putting failed transformed observation back on queue",
                extra=extra_dict,
            )
        else:
            logger.exception(
                "Retry attempts exceeded for transformed observation, sending to dead letter",
                extra={
                    **extra_dict,
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeadLetter: True,
                },
            )
        await retry_topic.send(value=retry_transformed_message)
    except Exception as e:
        logger.exception(
            "Unexpected Error occurred while preparing failed transformed observation for reprocessing",
            extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.DeadLetter: True},
        )
        # When all else fails post to dead letter
        await observations_transformed_deadletter.send(key=key, value=transformed_message)


async def process_failed_unprocessed_observation(key, message):
    try:
        raw_observation, attributes = extract_fields_from_message(message)
        attributes = update_attributes_for_unprocessed_retry(attributes)
        observation_type = attributes.get("observation_type")
        device_id = attributes.get("device_id")
        integration_id = attributes.get("integration_id")
        retry_topic_str = attributes.get("retry_topic")
        retry_attempt = attributes.get("retry_attempt")
        retry_unprocessed_message = create_retry_message(raw_observation, attributes)
        retry_topic: faust.Topic = topics_dict.get(retry_topic_str)
        logger.info(
            "Putting failed unprocessed observation back on queue",
            extra={
                ExtraKeys.DeviceId: device_id,
                ExtraKeys.InboundIntId: integration_id,
                ExtraKeys.StreamType: observation_type,
                ExtraKeys.RetryTopic: retry_topic_str,
                ExtraKeys.RetryAttempt: retry_attempt,
            },
        )
        await retry_topic.send(key=key, value=retry_unprocessed_message)
    except Exception as e:
        # When all else fails post to dead letter
        if retry_topic_str != TopicEnum.observations_transformed_deadletter.value:
            logger.info(
                "Putting failed unprocessed observation back on queue",
                extra=extra_dict,
            )
        else:
            logger.exception(
                "Retry attempts exceeded for unprocessed observation, sending to dead letter",
                extra={
                    **extra_dict,
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeadLetter: True,
                },
            )
        await retry_topic.send(value=retry_unprocessed_message)
    except Exception as e:
        # When all else fails post to dead letter
        logger.exception(
            "Unexpected Error occurred while preparing failed unprocessed observation for reprocessing",
            extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.DeadLetter: True},
        )
        await observations_unprocessed_deadletter.send(value=message)


async def process_transformed_retry_observation(key, transformed_message):
    try:
        transformed_observation, attributes = extract_fields_from_message(
            transformed_message
        )
        retry_at = datetime.fromisoformat(attributes.get("retry_at"))
        await wait_until_retry_at(retry_at)
        await process_transformed_observation(key, transformed_message)
    except Exception as e:
        logger.exception(
            "Unexpected Error occurred while attempting to process failed transformed observation",
            extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.DeadLetter: True},
        )
        # When all else fails post to dead letter
        await observations_transformed_deadletter.send(key=key, value=transformed_message)


async def process_retry_observation(key, message):
    try:
        raw_observation, attributes = extract_fields_from_message(message)
        retry_at = datetime.fromisoformat(attributes.get("retry_at"))
        await wait_until_retry_at(retry_at)
        await process_observation(key, message)
    except Exception as e:
        logger.exception(
            "Unexpected Error occurred while attempting to process failed unprocessed observation",
            extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.DeadLetter: True},
        )
        # When all else fails post to dead letter
        await observations_unprocessed_deadletter.send(value=message)


@app.agent(
    observations_unprocessed_topic,
    concurrency=routing_settings.ROUTING_CONCURRENCY_UNPROCESSED,
)
async def process_observations(streaming_data):
    async for key, message in streaming_data.items():
        try:
            await process_observation(key, message)
        except Exception as e:
            logger.exception(
                f"Unexpected error prior to processing observation",
                extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.DeadLetter: True},
            )
            # When all else fails post to dead letter
            await observations_unprocessed_deadletter.send(value=message)


@app.agent(
    observations_unprocessed_retry_short_topic,
    concurrency=routing_settings.ROUTING_CONCURRENCY_UNPROCESSED_RETRY_SHORT,
)
async def process_retry_short_observations(streaming_data):
    async for key, message in streaming_data.items():
        await process_retry_observation(key, message)


@app.agent(
    observations_unprocessed_retry_long_topic,
    concurrency=routing_settings.ROUTING_CONCURRENCY_UNPROCESSED_RETRY_LONG,
)
async def process_retry_long_observations(streaming_data):
    async for key, message in streaming_data.items():
        await process_retry_observation(key, message)


@app.agent(
    observations_transformed_topic,
    concurrency=routing_settings.ROUTING_CONCURRENCY_TRANSFORMED,
)
async def process_transformed_observations(streaming_transformed_data):
    async for key, transformed_message in streaming_transformed_data.items():
        try:
            await process_transformed_observation(key, transformed_message)
        except Exception as e:
            logger.exception(
                f"Unexpected error prior to processing transformed observation",
                extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.DeadLetter: True},
            )
            # When all else fails post to dead letter
            await observations_transformed_deadletter.send(value=transformed_message)


@app.agent(
    observations_transformed_retry_short_topic,
    concurrency=routing_settings.ROUTING_CONCURRENCY_TRANSFORMED_RETRY_SHORT,
)
async def process_transformed_retry_short_observations(streaming_transformed_data):
    async for key, transformed_message in streaming_transformed_data.items():
        await process_transformed_retry_observation(key, transformed_message)


@app.agent(
    observations_transformed_retry_long_topic,
    concurrency=routing_settings.ROUTING_CONCURRENCY_TRANSFORMED_RETRY_LONG,
)
async def process_transformed_retry_long_observations(streaming_transformed_data):
    async for key, transformed_message in streaming_transformed_data.items():
        await process_transformed_retry_observation(key, transformed_message)


@app.timer(interval=120.0)
async def log_metrics(app):
    m = app.monitor
    metrics_dict = {
        "messages_received_by_topic": m.messages_received_by_topic,
        "messages_sent_by_topic": m.messages_sent_by_topic,
        "messages_active": m.messages_active,
        "assignment_latency": m.assignment_latency,
        "send_errors": m.send_errors,
        "rebalances": m.rebalances,
        "rebalance_return_avg": m.rebalance_return_avg,
    }
    logger.info(f"Metrics heartbeat for Consumer", extra=metrics_dict)


# @app.on_rebalance_start()
# async def on_rebalance_start():
#     pass


if __name__ == "__main__":
    logger.info("Application getting started")
    app.main()
