import faust
from opentelemetry import propagate, context
from opentelemetry.instrumentation.confluent_kafka.utils import (
    _kafka_getter,
    _kafka_setter,
)
from functools import wraps


def load_context(func):
    """
    This decorator loads the tracing context from faust event headers.
    This allows to make spans created in the decorated function to be attached to
    traces created previously by another kafka publisher/producer.
    """

    @wraps(func)
    async def wrapper(*args):
        faust_event_headers = faust.streams.current_event().headers
        ctx = propagate.extract(
            carrier=faust_event_headers.items(), getter=_kafka_getter
        )
        context.attach(ctx)
        await func(*args)

    return wrapper


def build_context_headers():
    headers = []
    propagate.inject(
        headers,
        setter=_kafka_setter,
    )
    return headers
