import faust
from opentelemetry import propagate, context
from functools import wraps


def load_context_from_attributes(attributes):
    ctx = propagate.extract(carrier=attributes.get("tracing_context", []))
    context.attach(ctx)


def build_context_headers():
    headers = []
    propagate.inject(headers)
    return headers
