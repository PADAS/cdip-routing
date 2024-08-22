import logging
from gundi_core.events import ObservationReceived, EventReceived, EventUpdateReceived, AttachmentReceived
from gundi_core.events.transformers import (
    EventTransformedER,
    EventUpdateTransformedER,
    AttachmentTransformedER,
    ObservationTransformedER,
    EventTransformedSMART,
    EventUpdateTransformedSMART,
    EventTransformedWPSWatch,
    AttachmentTransformedWPSWatch
)
from opentelemetry.trace import SpanKind
from app.core import tracing
from app.core.errors import ReferenceDataError
from app.core.gundi import get_connection, get_route, get_integration
from app.core.local_logging import ExtraKeys
from app.core.utils import Broker
from app.core.utils import get_provider_key
from app.core.pubsub import send_message_to_gcp_pubsub_dispatcher
from app.services.transformers import (
    build_transformed_message_attributes,
    build_gcp_pubsub_message,
    get_source_id,
    get_data_provider_id,
    transform_observation_v2
)


logger = logging.getLogger(__name__)


transformer_events_by_data_type = {
    "EREvent": EventTransformedER,
    "EREventUpdate": EventUpdateTransformedER,
    "ERAttachment": AttachmentTransformedER,
    "ERObservation": ObservationTransformedER,
    "SMARTCompositeRequest": EventTransformedSMART,
    "SMARTUpdateRequest": EventUpdateTransformedSMART,
    "WPSWatchImageMetadata": EventTransformedWPSWatch,
    "WPSWatchImage": AttachmentTransformedWPSWatch
}


def build_transformer_event(transformed_observation):
    Event = transformer_events_by_data_type[type(transformed_observation).__name__]
    return Event(
        payload=transformed_observation
    )


async def transform_and_route_observation(observation):
    with tracing.tracer.start_as_current_span(
            "routing_service.transform_and_route_observation", kind=SpanKind.CONSUMER
    ) as current_span:
        try:
            # ToDo: Implement a destination resolution algorithm considering all the routes and filters
            connection = await get_connection(connection_id=observation.data_provider_id)
            if not connection:
                error = f"Connection '{observation.data_provider_id}' not found."
                current_span.set_attribute("error", error)
                raise ReferenceDataError(error)
            provider = connection.provider
            destinations = connection.destinations
            default_route = await get_route(route_id=connection.default_route.id)
            if not default_route:
                error = f"Default route '{connection.default_route.id}', for provider '{observation.data_provider_id}' not found."
                current_span.set_attribute("error", error)
                raise ReferenceDataError(error)
            route_configuration = default_route.configuration
            provider_key = get_provider_key(provider)  # i.e. gundi_cellstop_abc1234..
            current_span.set_attribute("destinations_qty", len(destinations))
            current_span.set_attribute(
                "destinations", str([str(d.id) for d in destinations])
            )
            if len(destinations) < 1:
                current_span.add_event(
                    name="routing_service.observation_has_no_destinations"
                )
                logger.warning(
                    f"Connection {observation.data_provider_id} has no Destinations. This is a configuration error.",
                    extra={
                        ExtraKeys.DeviceId: observation.source_id,
                        ExtraKeys.InboundIntId: observation.data_provider_id,
                        ExtraKeys.StreamType: observation.observation_type,
                        ExtraKeys.GundiVersion: "v2",
                        ExtraKeys.GundiId: observation.gundi_id,
                        ExtraKeys.AttentionNeeded: True,
                    },
                )

            for destination in destinations:
                # Get additional configuration for the destination
                destination_integration = await get_integration(integration_id=destination.id)
                broker_config = destination_integration.additional

                # Transform the observation for the destination
                transformed_observation = await transform_observation_v2(
                    observation=observation,
                    destination=destination_integration,
                    provider=provider,
                    route_configuration=route_configuration,
                )

                if not transformed_observation:
                    logger.warning(
                        f"Observation {observation.gundi_id} could not be transformed for destination {destination.id}. Discarded.",
                    )
                    continue

                # Add metadata used to dispatch the observation
                attributes = build_transformed_message_attributes(
                    observation=observation,
                    destination=destination,
                    gundi_version="v2",
                    provider_key=provider_key,
                )
                logger.debug(
                    f"Transformed observation: {repr(transformed_observation)}, attributes: {attributes}"
                )

                if broker_type := broker_config.get("broker", Broker.GCP_PUBSUB.value).strip().lower() != Broker.GCP_PUBSUB.value:
                    current_span.set_attribute("broker", broker_type)
                    raise ReferenceDataError(
                        f"Broker '{broker_type}' is no longer supported. Please use `{Broker.GCP_PUBSUB.value}` instead."
                    )

                # Build system event for dispatchers
                transformer_event = build_transformer_event(transformed_observation)
                # Publish to a GCP PubSub topic
                pubsub_message = build_gcp_pubsub_message(
                    payload=transformer_event.dict(exclude_none=True)
                )
                await send_message_to_gcp_pubsub_dispatcher(
                    message=pubsub_message,
                    attributes=attributes,
                    destination=destination,
                    broker_config=broker_config,
                    ordering_key=observation.gundi_id
                )
                logger.info(
                    f"Observation {observation.gundi_id} transformed and sent to pubsub topic successfully.",
                    extra=attributes
                )
        except ReferenceDataError as e:
            error_msg = (
                f"External error occurred obtaining reference data for observation: {e}",
            )
            logger.exception(
                error_msg,
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeviceId: get_source_id(observation, "v2"),
                    ExtraKeys.InboundIntId: get_data_provider_id(observation, "v2"),
                    ExtraKeys.StreamType: observation.observation_type,
                },
            )
            current_span.set_attribute("error", error_msg)
            raise e  # Raise the exception so the message is retried later by GCP
        except Exception as e:
            error_msg = (
                f"Unexpected internal exception occurred processing observation: {e}"
            )
            logger.exception(
                error_msg,
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeviceId: get_source_id(observation, "v2"),
                    ExtraKeys.InboundIntId: get_data_provider_id(observation, "v2"),
                    ExtraKeys.StreamType: observation.observation_type,
                },
            )
            # Unexpected internal errors
            current_span.set_attribute("error", error_msg)
            raise e  # Raise the exception so the message is retried later by GCP


async def handle_observation_received(event: ObservationReceived):
    # Trace observations with Open Telemetry
    with tracing.tracer.start_as_current_span(
            "routing_service.handle_observation_received", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        await transform_and_route_observation(observation=event.payload)


async def handle_event_received(event: EventReceived):
    with tracing.tracer.start_as_current_span(
            "routing_service.handle_event_received", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        await transform_and_route_observation(observation=event.payload)


async def handle_event_update(event: EventUpdateReceived):
    with tracing.tracer.start_as_current_span(
            "routing_service.handle_event_update", kind=SpanKind.CONSUMER
    ) as current_span:
        event_update = event.payload
        current_span.set_attribute("payload", repr(event.payload))
        current_span.set_attribute("changes", str(event_update.changes))
        await transform_and_route_observation(observation=event_update)


async def handle_attachment_received(event: AttachmentReceived):
    with tracing.tracer.start_as_current_span(
            "routing_service.handle_attachment_received", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        await transform_and_route_observation(observation=event.payload)

event_handlers = {
    "ObservationReceived": handle_observation_received,
    "EventReceived": handle_event_received,
    "EventUpdateReceived": handle_event_update,
    "AttachmentReceived": handle_attachment_received,
}

event_schemas = {
    "ObservationReceived": ObservationReceived,
    "EventReceived": EventReceived,
    "EventUpdateReceived": EventUpdateReceived,
    "AttachmentReceived": AttachmentReceived,
}
