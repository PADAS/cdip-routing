import logging
from gundi_core.events import (
    FilteredObservation,
    ObservationFiltered,
    ObservationReceived,
    ObservationsBatchReceived,
    EventReceived,
    EventUpdateReceived,
    AttachmentReceived,
    TextMessageReceived,
    GundiDelivery,
    ProviderInfo,
    ERObservationsBatch,
    ObservationsBatchTransformedER,
    TransformedERObservationItem,
)
from gundi_core.schemas.v2 import StreamPrefixEnum, ERObservation
from gundi_core.events.transformers import (
    EventTransformedER,
    EventUpdateTransformedER,
    AttachmentTransformedER,
    ObservationTransformedER,
    EventTransformedSMART,
    EventUpdateTransformedSMART,
    EventTransformedWPSWatch,
    AttachmentTransformedWPSWatch,
    EventTransformedTrapTagger,
    AttachmentTransformedTrapTagger,
    MessageTransformedER,
    MessageTransformedInReach,
)
from opentelemetry.trace import SpanKind
from app.core import settings, tracing
from app.core.deduplication import is_destination_complete, set_destination_complete
from app.core.errors import ReferenceDataError
from app.core.gundi import get_connection, get_route, get_integration
from app.core.local_logging import ExtraKeys, mark_logged
from app.core.utils import Broker
from app.core.utils import get_provider_key
from gundi_core.events.integrations import ObservationFilterReason
from app.core.pubsub import (
    send_event_to_routing_events_topic,
    send_events_to_routing_events_topic,
    send_message_to_gcp_pubsub_dispatcher,
)
from app.services.activity_logger import log_missing_default_route, log_transform_error
from app.services.transformers import (
    build_transformed_message_attributes,
    build_gcp_pubsub_message,
    get_source_id,
    get_data_provider_id,
    transform_observation_v2,
)


logger = logging.getLogger(__name__)


def _uses_generic_model(destination_integration) -> bool:
    """Whether a destination uses the generic-model path (publish a
    GundiDelivery for its action runner to transform) instead of a legacy
    in-process Transformer.

    Decided by the destination's integration *type* (e.g. ``cmore``) via
    ``settings.GENERIC_MODEL_DESTINATION_TYPES`` so it requires no per-integration
    config. The legacy per-integration ``additional.generic_model`` flag is still
    honored as an override for one-off opt-in.
    """
    type_value = getattr(getattr(destination_integration, "type", None), "value", None)
    if type_value and type_value in settings.GENERIC_MODEL_DESTINATION_TYPES:
        return True
    return bool((destination_integration.additional or {}).get("generic_model"))


transformer_events_by_data_type = {
    "EREvent": EventTransformedER,
    "EREventUpdate": EventUpdateTransformedER,
    "ERAttachment": AttachmentTransformedER,
    "ERObservation": ObservationTransformedER,
    "SMARTCompositeRequest": EventTransformedSMART,
    "SMARTUpdateRequest": EventUpdateTransformedSMART,
    "WPSWatchImageMetadata": EventTransformedWPSWatch,
    "WPSWatchImage": AttachmentTransformedWPSWatch,
    "TrapTaggerImageMetadata": EventTransformedTrapTagger,
    "TrapTaggerImage": AttachmentTransformedTrapTagger,
    "ERMessage": MessageTransformedER,
    "InReachIPCMessage": MessageTransformedInReach,
}


def build_transformer_event(transformed_observation):
    Event = transformer_events_by_data_type[type(transformed_observation).__name__]
    return Event(payload=transformed_observation)


def _build_provider_info(provider) -> ProviderInfo:
    provider_type = ""
    if getattr(provider, "type", None) is not None:
        provider_type = getattr(provider.type, "value", "") or ""

    owner_id = ""
    owner_name = ""
    if getattr(provider, "owner", None) is not None:
        owner_id = str(getattr(provider.owner, "id", "") or "")
        owner_name = getattr(provider.owner, "name", "") or ""

    return ProviderInfo(
        provider_id=str(provider.id),
        provider_type=provider_type,
        provider_name=getattr(provider, "name", "") or "",
        owner_id=owner_id,
        owner_name=owner_name,
    )


def _build_gundi_delivery(
    *, observation, provider, route_configuration
) -> GundiDelivery:
    return GundiDelivery(
        payload=observation,
        route_configuration=route_configuration,
        provider=_build_provider_info(provider),
    )


async def _publish_gundi_delivery(
    *,
    observation,
    destination,
    destination_integration,
    provider,
    provider_key,
    route_configuration,
    broker_config,
    destination_str,
    provider_str,
    current_span,
):
    """Generic-model publish path: wrap the Gundi payload in a GundiDelivery
    envelope and publish to the destination's PubSub topic. The action runner
    is responsible for transformation."""

    # Validate broker — same restriction as the legacy path.
    broker_value = (
        (broker_config or {}).get("broker", Broker.GCP_PUBSUB.value).strip().lower()
    )
    if broker_value != Broker.GCP_PUBSUB.value:
        current_span.set_attribute("broker", broker_value)
        raise ReferenceDataError(
            f"Broker '{broker_value}' is no longer supported. Please use `{Broker.GCP_PUBSUB.value}` instead."
        )

    try:
        delivery = _build_gundi_delivery(
            observation=observation,
            provider=provider,
            route_configuration=route_configuration,
        )
    except Exception as e:
        error_msg = (
            f"Error building GundiDelivery for observation {observation.gundi_id} "
            f"from {provider_str} for destination {destination_str}: "
            f"{type(e).__name__}: {e}. Discarded."
        )
        logger.exception(error_msg)
        current_span.set_attribute("error", error_msg)
        current_span.set_attribute("is_discarded", True)
        current_span.add_event(
            name="routing_service.observation_discarded_on_generic_envelope_error"
        )
        return

    attributes = build_transformed_message_attributes(
        observation=observation,
        destination=destination,
        gundi_version="v2",
        provider_key=provider_key,
    )

    pubsub_message = build_gcp_pubsub_message(payload=delivery.dict(exclude_none=True))
    ordering_key = (
        str(observation.gundi_id)
        if observation.observation_type == StreamPrefixEnum.event_update.value
        else ""
    )
    await send_message_to_gcp_pubsub_dispatcher(
        message=pubsub_message,
        attributes=attributes,
        destination=destination,
        broker_config=broker_config,
        ordering_key=ordering_key,
    )
    logger.info(
        f"Observation {observation.gundi_id} published as GundiDelivery to {destination_str}.",
        extra=attributes,
    )


async def _discard_on_missing_default_route(
    *, connection, observations, observation_type, current_span
):
    """Log, trace and emit an activity log for a connection with no default route.

    This is a portal configuration error (the provider has no routing rule), so
    the observations cannot be routed. Callers discard them instead of raising,
    because raising makes PubSub retry a message that can never succeed.
    """
    provider = connection.provider
    provider_id = str(provider.id)
    owner_name = provider.owner.name if provider.owner else None
    gundi_ids = [str(o.gundi_id) for o in observations]
    destinations = [str(d.id) for d in (connection.destinations or [])]
    routing_rules = [str(r.id) for r in (connection.routing_rules or [])]
    error_msg = (
        f"Connection '{owner_name} - {provider.name}'({provider_id}) has no default route. "
        f"This is a configuration error. "
        f"{len(gundi_ids)} {observation_type} observation(s) discarded: {gundi_ids}. "
        f"destinations={destinations} routing_rules={routing_rules}"
    )
    logger.error(
        error_msg,
        extra={
            ExtraKeys.AttentionNeeded: True,
            ExtraKeys.InboundIntId: provider_id,
            ExtraKeys.Provider: provider.name,
            ExtraKeys.StreamType: observation_type,
            ExtraKeys.GundiVersion: "v2",
            ExtraKeys.GundiId: gundi_ids[0] if len(gundi_ids) == 1 else gundi_ids,
            "destinations": destinations,
            "routing_rules": routing_rules,
        },
    )
    current_span.set_attribute("error", error_msg)
    current_span.set_attribute("is_discarded", True)
    current_span.add_event(
        name="routing_service.observation_discarded_on_missing_default_route"
    )
    await log_missing_default_route(
        connection=connection,
        observation_type=observation_type,
        gundi_ids=gundi_ids,
    )


async def _report_transform_error(
    *,
    connection,
    destination,
    observations,
    observation_type,
    exception,
    will_retry,
    error_msg,
    current_span,
    span_event,
):
    # exc_info passed explicitly: the batch path reports after leaving the except block
    logger.error(
        error_msg,
        exc_info=exception,
        extra={
            ExtraKeys.AttentionNeeded: True,
            ExtraKeys.InboundIntId: str(connection.provider.id),
            ExtraKeys.OutboundIntId: str(destination.id),
            ExtraKeys.StreamType: observation_type,
            ExtraKeys.GundiVersion: "v2",
            ExtraKeys.GundiId: (
                str(observations[0].gundi_id)
                if len(observations) == 1
                else [str(o.gundi_id) for o in observations]
            ),
        },
    )
    mark_logged(exception)
    current_span.set_attribute("error", error_msg)
    if not will_retry:
        current_span.set_attribute("is_discarded", True)
    current_span.add_event(name=span_event)
    await log_transform_error(
        connection=connection,
        destination=destination,
        observation_type=observation_type,
        gundi_ids=[str(o.gundi_id) for o in observations],
        exception=exception,
        will_retry=will_retry,
    )


async def _is_destination_complete(
    *, event_id, destination, gundi_ids, provider_id, current_span
):
    if not event_id or not await is_destination_complete(event_id, str(destination.id)):
        return False
    logger.info(
        f"Event {event_id} already delivered to destination {destination.id}. Skipped.",
        extra={
            ExtraKeys.GundiId: gundi_ids[0] if len(gundi_ids) == 1 else gundi_ids,
            ExtraKeys.InboundIntId: provider_id,
            ExtraKeys.OutboundIntId: str(destination.id),
        },
    )
    current_span.add_event(name="routing_service.destination_skipped_already_complete")
    return True


async def _mark_destination_complete(event_id, destination_id):
    if event_id:
        await set_destination_complete(event_id, str(destination_id))


def _external_source_id_for_record(observation):
    # Only for the log line and the drop record. The v2 schemas default an
    # omitted external_source_id to the string "None"/"none", not null; an
    # omitted id must read as "no device" there, while a value the provider
    # actually sent - even one spelled "none" - is kept as sent. The rule
    # itself always gets the verbatim value: ids compare verbatim against
    # Source.external_id by gundi-core's contract.
    if "external_source_id" not in getattr(observation, "__fields_set__", ()):
        return None
    return getattr(observation, "external_source_id", None)


def _dropping_source_filter(route, destination_id, observation):
    """The device rule that drops this observation for this destination, or None
    to deliver. Fail-open throughout (GUNDI-5712): flag off, no filters block, no
    rule for the arrow, a rule that does not name this provider, a disabled rule
    or an unknown mode all answer None - a malformed configuration must never
    cause a silent drop. Only an explicit allows() == False drops.
    """
    if not settings.ROUTING_FILTERS_ENABLED:
        return None
    # An attachment has no device identity of its own; it follows its parent
    # event, which was already gated. One whose parent was dropped fails at the
    # dispatcher the same way today's orphaned attachments do.
    if observation.observation_type == StreamPrefixEnum.attachment.value:
        return None
    rule = route.filter_for(destination_id) if route is not None else None
    if rule is None:
        return None
    verdict = rule.allows(
        str(observation.data_provider_id),
        getattr(observation, "external_source_id", None),
    )
    return rule if verdict is False else None


async def _discard_filtered_observation(
    *,
    observation,
    destination,
    rule,
    current_span,
    span_event,
    set_discarded=False,
    sink=None,
):
    """Record a policy drop. With a `sink` list the ObservationFiltered event is
    collected there for the caller to publish in one request; without one it is
    published right away."""
    if set_discarded:
        current_span.set_attribute("is_discarded", True)
    current_span.add_event(name=span_event)
    # Never raises, structurally: in the batch path this runs outside the
    # per-item try, so an exception here would abort the batch and redelivery
    # would re-publish items already sent. The drop itself already happened
    # (the caller continues); this only records it.
    try:
        external_source_id = _external_source_id_for_record(observation)
        # INFO, like a delivery: until the ObservationFiltered consumer marks the
        # GundiTrace this line is the only visible record of the drop. Not
        # attention_needed - a working rule is not an operator problem.
        logger.info(
            f"Observation {observation.gundi_id} from device {external_source_id} dropped "
            f"by the {rule.mode} rule for destination {destination.id}.",
            extra={
                ExtraKeys.GundiId: str(observation.gundi_id),
                ExtraKeys.DeviceId: external_source_id,
                ExtraKeys.InboundIntId: str(observation.data_provider_id),
                ExtraKeys.OutboundIntId: str(destination.id),
                ExtraKeys.StreamType: observation.observation_type,
            },
        )
        event = ObservationFiltered(
            payload=FilteredObservation(
                gundi_id=str(observation.gundi_id),
                related_to=getattr(observation, "related_to", None),
                data_provider_id=str(observation.data_provider_id),
                destination_id=str(destination.id),
                external_source_id=external_source_id,
                observation_type=observation.observation_type,
                filtered_by=ObservationFilterReason.for_mode(rule.mode).value,
            )
        )
        if sink is not None:
            sink.append(event)
        else:
            await send_event_to_routing_events_topic(event)
    except Exception as e:
        logger.warning(
            f"Failed to record the filtered drop of {observation.gundi_id}: "
            f"{type(e).__name__}: {e}"
        )


async def transform_and_route_observation(observation, *, event_id=None):
    with tracing.tracer.start_as_current_span(
        "routing_service.transform_and_route_observation", kind=SpanKind.CONSUMER
    ) as current_span:
        retry_error = None
        try:
            # ToDo: Implement a destination resolution algorithm considering all the routes and filters
            connection = await get_connection(
                connection_id=observation.data_provider_id
            )
            if not connection:
                error = f"Connection '{observation.data_provider_id}' not found."
                current_span.set_attribute("error", error)
                raise ReferenceDataError(error)
            if not connection.default_route:
                await _discard_on_missing_default_route(
                    connection=connection,
                    observations=[observation],
                    observation_type=observation.observation_type,
                    current_span=current_span,
                )
                return
            provider = connection.provider
            destinations = connection.destinations
            default_route = await get_route(
                route_id=connection.default_route.id,
                data_provider_id=observation.data_provider_id,
            )
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

            provider_str = f"'{connection.provider.owner.name} - {connection.provider.name}'({connection.provider.id})"
            for destination in destinations:
                if await _is_destination_complete(
                    event_id=event_id,
                    destination=destination,
                    gundi_ids=[str(observation.gundi_id)],
                    provider_id=str(observation.data_provider_id),
                    current_span=current_span,
                ):
                    continue
                # Policy gate first: a dropped destination needs neither the
                # portal lookup below nor a transform.
                if rule := _dropping_source_filter(
                    default_route, destination.id, observation
                ):
                    await _discard_filtered_observation(
                        observation=observation,
                        destination=destination,
                        rule=rule,
                        current_span=current_span,
                        span_event="routing_service.observation_discarded_by_source_filter",
                        set_discarded=True,
                    )
                    await _mark_destination_complete(event_id, destination.id)
                    continue
                # Get additional configuration for the destination
                destination_integration = await get_integration(
                    integration_id=destination.id
                )
                if destination_integration is None:
                    # get_integration swallows portal errors (timeouts, 5xx) and
                    # returns None after emitting a portal-lookup activity log.
                    # Raise a ReferenceDataError so the message is retried once
                    # the portal recovers, instead of an AttributeError below.
                    error = (
                        f"Destination integration '{destination.id}' could not be "
                        f"loaded from the portal for provider '{observation.data_provider_id}'."
                    )
                    current_span.set_attribute("error", error)
                    raise ReferenceDataError(error)
                broker_config = destination_integration.additional
                destination_str = (
                    f"'{destination.owner.name} - {destination.name}'({destination.id})"
                )

                # Generic-model path: publish a GundiDelivery envelope and let
                # the action runner perform destination-specific transformation.
                if _uses_generic_model(destination_integration):
                    await _publish_gundi_delivery(
                        observation=observation,
                        destination=destination,
                        destination_integration=destination_integration,
                        provider=provider,
                        provider_key=provider_key,
                        route_configuration=route_configuration,
                        broker_config=broker_config,
                        destination_str=destination_str,
                        provider_str=provider_str,
                        current_span=current_span,
                    )
                    await _mark_destination_complete(event_id, destination.id)
                    continue

                # Transform the observation for the destination
                try:
                    transformed_observation = await transform_observation_v2(
                        observation=observation,
                        destination=destination_integration,
                        provider=provider,
                        route_configuration=route_configuration,
                    )
                except Exception as e:
                    will_retry = isinstance(e, ReferenceDataError)
                    outcome = "Will retry." if will_retry else "Discarded."
                    await _report_transform_error(
                        connection=connection,
                        destination=destination,
                        observations=[observation],
                        observation_type=observation.observation_type,
                        exception=e,
                        will_retry=will_retry,
                        error_msg=f"Error transforming observation {observation.gundi_id} from {provider_str} for destination {destination_str}: {type(e).__name__}: {e}. {outcome}",
                        current_span=current_span,
                        span_event=(
                            "routing_service.observation_retried_on_transformer_error"
                            if will_retry
                            else "routing_service.observation_discarded_on_transformer_error"
                        ),
                    )
                    # Raised only after the other destinations are attempted, so a
                    # broken destination doesn't block them; the ones completed
                    # here are marked and skipped on the retry.
                    if not will_retry:
                        await _mark_destination_complete(event_id, destination.id)
                    elif retry_error is None:
                        retry_error = e
                    continue

                if not transformed_observation:
                    logger.warning(
                        f"Observation {observation.gundi_id} from {provider_str} could not be transformed for destination {destination_str}. Discarded."
                    )
                    current_span.set_attribute("is_discarded", True)
                    current_span.add_event(
                        name="routing_service.observation_discarded_by_transformer"
                    )
                    await _mark_destination_complete(event_id, destination.id)
                    continue

                logger.debug(
                    f"Observation {observation.gundi_id} from {provider_str} transformed for destination {destination_str}."
                )
                # Add metadata used to dispatch the observation
                attributes = build_transformed_message_attributes(
                    observation=observation,
                    destination=destination,
                    gundi_version="v2",
                    provider_key=getattr(
                        transformed_observation, "provider_key", provider_key
                    ),  # Field mappings overrides take precedence
                )
                logger.debug(
                    f"Transformed observation: {repr(transformed_observation)}, attributes: {attributes}"
                )

                broker_type = (
                    broker_config.get("broker", Broker.GCP_PUBSUB.value).strip().lower()
                )
                if broker_type != Broker.GCP_PUBSUB.value:
                    current_span.set_attribute("broker", broker_type)
                    raise ReferenceDataError(
                        f"Broker '{broker_type}' is no longer supported. Please use `{Broker.GCP_PUBSUB.value}` instead."
                    )

                # Build message for dispatcher
                if isinstance(transformed_observation, dict):
                    # Pass the data as a raw dict for backward compatibility with older dispatchers (e.g. Movebank)
                    pubsub_message_payload = transformed_observation
                else:
                    # Build system event using pydantic models
                    pubsub_message_payload = build_transformer_event(
                        transformed_observation
                    ).dict(exclude_none=True)

                # Publish to a GCP PubSub topic
                pubsub_message = build_gcp_pubsub_message(
                    payload=pubsub_message_payload
                )
                # Set ordering key only for updates
                ordering_key = (
                    str(observation.gundi_id)
                    if observation.observation_type
                    == StreamPrefixEnum.event_update.value
                    else ""
                )
                await send_message_to_gcp_pubsub_dispatcher(
                    message=pubsub_message,
                    attributes=attributes,
                    destination=destination,
                    broker_config=broker_config,
                    ordering_key=ordering_key,
                )
                logger.info(
                    f"Observation {observation.gundi_id} transformed and sent to pubsub topic successfully.",
                    extra=attributes,
                )
                await _mark_destination_complete(event_id, destination.id)
        except ReferenceDataError as e:
            error_msg = (
                f"External error occurred obtaining reference data for observation: {e}"
            )
            logger.exception(
                error_msg,
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeviceId: get_source_id(observation, "v2"),
                    ExtraKeys.InboundIntId: get_data_provider_id(observation, "v2"),
                    ExtraKeys.StreamType: observation.observation_type,
                    ExtraKeys.GundiId: str(observation.gundi_id),
                },
            )
            current_span.set_attribute("error", error_msg)
            # Raise the exception so the message is retried later by GCP
            raise mark_logged(e)
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
                    ExtraKeys.GundiId: str(observation.gundi_id),
                },
            )
            # Unexpected internal errors
            current_span.set_attribute("error", error_msg)
            # Raise the exception so the message is retried later by GCP
            raise mark_logged(e)
        # Outside the try: it was already logged per destination.
        if retry_error is not None:
            raise retry_error


async def _publish_transformed_batch_group(
    *,
    batch,
    items,
    effective_provider_key,
    destination,
    broker_config,
):
    er_batch = ERObservationsBatch(
        batch_id=batch.batch_id,
        data_provider_id=batch.data_provider_id,
        destination_id=str(destination.id),
        provider_key=effective_provider_key,
        items=items,
    )
    envelope = ObservationsBatchTransformedER(payload=er_batch)
    attributes = {
        "gundi_version": "v2",
        "batch": "true",
        "batch_count": str(len(items)),
        "provider_key": effective_provider_key,
        "stream_type": batch.observation_type,
        "destination_id": str(destination.id),
        "data_provider_id": str(batch.data_provider_id),
    }
    pubsub_message = build_gcp_pubsub_message(payload=envelope.dict(exclude_none=True))
    await send_message_to_gcp_pubsub_dispatcher(
        message=pubsub_message,
        attributes=attributes,
        destination=destination,
        broker_config=broker_config,
        ordering_key="",
    )
    logger.info(
        f"Batch {batch.batch_id}: {len(items)} observations transformed and sent to destination {destination.id}.",
        extra=attributes,
    )


async def transform_and_route_observations_batch(batch, *, event_id=None):
    with tracing.tracer.start_as_current_span(
        "routing_service.transform_and_route_observations_batch", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("batch_id", str(batch.batch_id))
        current_span.set_attribute("batch_count", len(batch.observations))
        if not batch.observations:
            return
        retry_error = None
        try:
            data_provider_id = str(batch.data_provider_id)
            # ONE connection/route lookup for the whole batch — every item
            # shares the provider by the envelope invariant.
            connection = await get_connection(connection_id=data_provider_id)
            if not connection:
                error = f"Connection '{data_provider_id}' not found."
                current_span.set_attribute("error", error)
                raise ReferenceDataError(error)
            if not connection.default_route:
                await _discard_on_missing_default_route(
                    connection=connection,
                    observations=batch.observations,
                    observation_type=batch.observation_type,
                    current_span=current_span,
                )
                return
            provider = connection.provider
            default_route = await get_route(
                route_id=connection.default_route.id,
                data_provider_id=data_provider_id,
            )
            if not default_route:
                error = f"Default route '{connection.default_route.id}', for provider '{data_provider_id}' not found."
                current_span.set_attribute("error", error)
                raise ReferenceDataError(error)
            route_configuration = default_route.configuration
            provider_key = get_provider_key(provider)
            destinations = connection.destinations
            current_span.set_attribute("destinations_qty", len(destinations))

            provider_str = f"'{connection.provider.owner.name} - {connection.provider.name}'({connection.provider.id})"
            for destination in destinations:
                if await _is_destination_complete(
                    event_id=event_id,
                    destination=destination,
                    gundi_ids=[str(o.gundi_id) for o in batch.observations],
                    provider_id=data_provider_id,
                    current_span=current_span,
                ):
                    continue
                # Policy first, before this destination's portal lookup and
                # broker check: a destination that drops every item must not
                # be able to retry the envelope - and redeliver the others -
                # by failing its setup. Partial drops are published after the
                # deliveries so a setup failure does not record them twice.
                allowed, filtered_events = [], []
                for observation in batch.observations:
                    if rule := _dropping_source_filter(
                        default_route, destination.id, observation
                    ):
                        await _discard_filtered_observation(
                            observation=observation,
                            destination=destination,
                            rule=rule,
                            current_span=current_span,
                            span_event="routing_service.batch_item_discarded_by_source_filter",
                            sink=filtered_events,
                        )
                    else:
                        allowed.append(observation)
                if not allowed:
                    await send_events_to_routing_events_topic(filtered_events)
                    await _mark_destination_complete(event_id, destination.id)
                    continue

                destination_integration = await get_integration(
                    integration_id=destination.id
                )
                if destination_integration is None:
                    # get_integration swallows portal errors (timeouts, 5xx) and
                    # returns None after emitting a portal-lookup activity log.
                    # Raise a ReferenceDataError so the message is retried once
                    # the portal recovers, instead of an AttributeError below.
                    error = (
                        f"Destination integration '{destination.id}' could not be "
                        f"loaded from the portal for provider '{data_provider_id}'."
                    )
                    current_span.set_attribute("error", error)
                    raise ReferenceDataError(error)
                broker_config = destination_integration.additional
                destination_str = (
                    f"'{destination.owner.name} - {destination.name}'({destination.id})"
                )

                # Validate the broker once per destination, before any
                # transform/publish work — same restriction the single-item
                # path (transform_and_route_observation) and the generic-model
                # publish path (_publish_gundi_delivery) already enforce per
                # item. Hoisted here so a batch can't slip an unsupported
                # broker past this check the way per-item publishing would
                # have caught it.
                # str() + `or` guard: `additional.broker` can be present but
                # null in portal data; .strip() on None would fail the whole
                # batch before the unsupported-broker check even runs.
                broker_value = (
                    str((broker_config or {}).get("broker") or Broker.GCP_PUBSUB.value)
                    .strip()
                    .lower()
                )
                if broker_value != Broker.GCP_PUBSUB.value:
                    current_span.set_attribute("broker", broker_value)
                    raise ReferenceDataError(
                        f"Broker '{broker_value}' is no longer supported. Please use `{Broker.GCP_PUBSUB.value}` instead."
                    )

                # Generic-model destinations keep the per-item GundiDelivery
                # path (splitting the batch is allowed; merging never is).
                # A destination's drops go out in one publish after its
                # deliveries: one publish per dropped item, each with its own
                # session and timeout, can outlast the push ack deadline on a
                # large batch and get the allowed items redelivered.
                if _uses_generic_model(destination_integration):
                    for observation in allowed:
                        await _publish_gundi_delivery(
                            observation=observation,
                            destination=destination,
                            destination_integration=destination_integration,
                            provider=provider,
                            provider_key=provider_key,
                            route_configuration=route_configuration,
                            broker_config=broker_config,
                            destination_str=destination_str,
                            provider_str=provider_str,
                            current_span=current_span,
                        )
                    await send_events_to_routing_events_topic(filtered_events)
                    await _mark_destination_complete(event_id, destination.id)
                    continue

                # Transform per item; group per effective provider_key because
                # field mappings may override it per item and one ER bulk post
                # allows exactly one provider_key in its URL path.
                groups = {}
                discarded, discard_error = [], None
                destination_retry_error = None
                for observation in allowed:
                    try:
                        transformed = await transform_observation_v2(
                            observation=observation,
                            destination=destination_integration,
                            provider=provider,
                            route_configuration=route_configuration,
                        )
                    except ReferenceDataError as e:
                        # The whole envelope will be retried, so stop this
                        # destination here: publishing its other items now would
                        # only duplicate them on redelivery.
                        destination_retry_error = e
                        break
                    except Exception as e:
                        # Shrink the batch, never abort it
                        logger.exception(
                            f"Error transforming observation {observation.gundi_id} in batch {batch.batch_id} "
                            f"from {provider_str} for destination {destination_str}: {type(e).__name__}: {e}. Discarded.",
                            extra={
                                ExtraKeys.AttentionNeeded: True,
                                ExtraKeys.InboundIntId: data_provider_id,
                                ExtraKeys.OutboundIntId: str(destination.id),
                                ExtraKeys.StreamType: batch.observation_type,
                                ExtraKeys.GundiVersion: "v2",
                                ExtraKeys.GundiId: str(observation.gundi_id),
                            },
                        )
                        current_span.add_event(
                            name="routing_service.batch_item_discarded_on_transformer_error"
                        )
                        discarded.append(observation)
                        discard_error = discard_error or e
                        continue
                    if not transformed:
                        current_span.add_event(
                            name="routing_service.batch_item_discarded_by_transformer"
                        )
                        continue
                    if not isinstance(transformed, ERObservation):
                        # Non-ER destination in the same connection (e.g. a raw-dict
                        # transformer): no batch envelope exists for it yet, so this
                        # item publishes individually exactly as the single path does.
                        pubsub_message_payload = (
                            transformed
                            if isinstance(transformed, dict)
                            else build_transformer_event(transformed).dict(
                                exclude_none=True
                            )
                        )
                        attributes = build_transformed_message_attributes(
                            observation=observation,
                            destination=destination,
                            gundi_version="v2",
                            provider_key=getattr(
                                transformed, "provider_key", provider_key
                            ),
                        )
                        await send_message_to_gcp_pubsub_dispatcher(
                            message=build_gcp_pubsub_message(
                                payload=pubsub_message_payload
                            ),
                            attributes=attributes,
                            destination=destination,
                            broker_config=broker_config,
                            ordering_key="",
                        )
                        continue
                    effective_key = (
                        getattr(transformed, "provider_key", None) or provider_key
                    )
                    groups.setdefault(effective_key, []).append(
                        TransformedERObservationItem(
                            gundi_id=observation.gundi_id,
                            observation=transformed,
                        )
                    )

                if destination_retry_error is not None:
                    # Filtered drops are skipped too: the retry records them.
                    await _report_transform_error(
                        connection=connection,
                        destination=destination,
                        observations=allowed,
                        observation_type=batch.observation_type,
                        exception=destination_retry_error,
                        will_retry=True,
                        error_msg=(
                            f"Error transforming batch {batch.batch_id} from {provider_str} "
                            f"for destination {destination_str}: "
                            f"{type(destination_retry_error).__name__}: {destination_retry_error}. Will retry."
                        ),
                        current_span=current_span,
                        span_event="routing_service.batch_retried_on_transformer_error",
                    )
                    if retry_error is None:
                        retry_error = destination_retry_error
                    continue

                for effective_provider_key, items in groups.items():
                    await _publish_transformed_batch_group(
                        batch=batch,
                        items=items,
                        effective_provider_key=effective_provider_key,
                        destination=destination,
                        broker_config=broker_config,
                    )
                await send_events_to_routing_events_topic(filtered_events)
                if discarded:
                    await log_transform_error(
                        connection=connection,
                        destination=destination,
                        observation_type=batch.observation_type,
                        gundi_ids=[str(o.gundi_id) for o in discarded],
                        exception=discard_error,
                        will_retry=False,
                    )
                # Per destination, not per group: a crash between this
                # destination's group publishes re-publishes the earlier groups
                # on retry.
                await _mark_destination_complete(event_id, destination.id)
        except ReferenceDataError as e:
            logger.exception(
                f"External error occurred obtaining reference data for batch {batch.batch_id}: {e}",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.InboundIntId: str(batch.data_provider_id),
                },
            )
            current_span.set_attribute("error", str(e))
            raise mark_logged(e)  # Raise so the whole envelope is retried later by GCP
        except Exception as e:
            logger.exception(
                f"Unexpected internal exception occurred processing batch {batch.batch_id}: {e}",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.InboundIntId: str(batch.data_provider_id),
                },
            )
            current_span.set_attribute("error", str(e))
            raise mark_logged(e)
        # Raised only after the other destinations are published, so a broken
        # destination doesn't block them; the ones completed here are marked
        # and skipped on the retry. Outside the try: it was already logged for
        # its destination.
        if retry_error is not None:
            raise retry_error


async def handle_observations_batch_received(event: ObservationsBatchReceived):
    with tracing.tracer.start_as_current_span(
        "routing_service.handle_observations_batch_received", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("batch_count", len(event.payload.observations))
        await transform_and_route_observations_batch(
            batch=event.payload, event_id=str(event.event_id)
        )


async def handle_observation_received(event: ObservationReceived):
    # Trace observations with Open Telemetry
    with tracing.tracer.start_as_current_span(
        "routing_service.handle_observation_received", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        await transform_and_route_observation(
            observation=event.payload, event_id=str(event.event_id)
        )


async def handle_event_received(event: EventReceived):
    with tracing.tracer.start_as_current_span(
        "routing_service.handle_event_received", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        await transform_and_route_observation(
            observation=event.payload, event_id=str(event.event_id)
        )


async def handle_event_update(event: EventUpdateReceived):
    with tracing.tracer.start_as_current_span(
        "routing_service.handle_event_update", kind=SpanKind.CONSUMER
    ) as current_span:
        event_update = event.payload
        current_span.set_attribute("payload", repr(event.payload))
        current_span.set_attribute("changes", str(event_update.changes))
        await transform_and_route_observation(
            observation=event_update, event_id=str(event.event_id)
        )


async def handle_attachment_received(event: AttachmentReceived):
    with tracing.tracer.start_as_current_span(
        "routing_service.handle_attachment_received", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        await transform_and_route_observation(
            observation=event.payload, event_id=str(event.event_id)
        )


async def handle_text_message_received(event: TextMessageReceived):
    with tracing.tracer.start_as_current_span(
        "routing_service.handle_text_message_received", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        await transform_and_route_observation(
            observation=event.payload, event_id=str(event.event_id)
        )


event_handlers = {
    "ObservationReceived": handle_observation_received,
    "ObservationsBatchReceived": handle_observations_batch_received,
    "EventReceived": handle_event_received,
    "EventUpdateReceived": handle_event_update,
    "AttachmentReceived": handle_attachment_received,
    "TextMessageReceived": handle_text_message_received,
}

event_schemas = {
    "ObservationReceived": ObservationReceived,
    "ObservationsBatchReceived": ObservationsBatchReceived,
    "EventReceived": EventReceived,
    "EventUpdateReceived": EventUpdateReceived,
    "AttachmentReceived": AttachmentReceived,
    "TextMessageReceived": TextMessageReceived,
}
