# Open telemetry metrics (Distributed Tracing)
from opentelemetry import propagate, trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.propagators.cloud_trace_propagator import (
    CloudTraceFormatPropagator,
)
from opentelemetry.propagate import set_global_textmap
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor
from opentelemetry.trace import Link
from opentelemetry.instrumentation.confluent_kafka.utils import _kafka_getter, _kafka_setter


def configure_tracer(name: str, version: str = ""):
    resource = Resource.create(
        {
            "service.name": name,
            "service.version": version,
        }
    )
    tracer_provider = TracerProvider(resource=resource)
    cloud_trace_exporter = CloudTraceSpanExporter()
    tracer_provider.add_span_processor(
        # BatchSpanProcessor buffers spans and sends them in batches in a
        # background thread. The default parameters are sensible, but can be
        # tweaked to optimize your performance
        BatchSpanProcessor(cloud_trace_exporter)
    )
    trace.set_tracer_provider(tracer_provider)
    return trace.get_tracer(name, version)


def instrument_kafka_consumer(consumer):
    return ConfluentKafkaInstrumentor.instrument_consumer(consumer)


def instrument_kafka_producer(producer):
    return ConfluentKafkaInstrumentor.instrument_producer(producer)


# Capture requests (sync and async)
RequestsInstrumentor().instrument()
AioHttpClientInstrumentor().instrument()
# Using the X-Cloud-Trace-Context header
set_global_textmap(CloudTraceFormatPropagator())
tracer = configure_tracer(name="cdip-routing", version="1.0.8")


def get_tracing_context_from_kafka(headers):
    #######################################################################
    # Open Telemetry Metrics Test
    #######################################################################
    # ToDo: Implement an instrumentor for faust
    ctx = propagate.extract(carrier=headers, getter=_kafka_getter)
    links = []
    if ctx:
        for item in ctx.values():
            if hasattr(item, "get_span_context"):
                links.append(Link(context=item.get_span_context()))
    return ctx, links


def inject_tracing_context_into_kafka(headers):
    propagate.inject(
        headers,
        setter=_kafka_setter,
    )
