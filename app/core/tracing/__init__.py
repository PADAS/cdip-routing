# Imports required for instrumentation
import requests
import httpx
import aiohttp
from app.core import settings
from opentelemetry.propagators.cloud_trace_propagator import (
    CloudTraceFormatPropagator,
)
from opentelemetry.propagate import set_global_textmap
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from . import config
from . import pubsub_instrumentation

# Using the X-Cloud-Trace-Context header
set_global_textmap(CloudTraceFormatPropagator())
tracer = config.configure_tracer(name="cdip-routing", version="2.0.0")

# Capture requests (sync and async)
if settings.TRACING_ENABLED:
    RequestsInstrumentor().instrument()
    HTTPXClientInstrumentor().instrument()
    AioHttpClientInstrumentor().instrument()
else:
    RequestsInstrumentor().uninstrument()
    HTTPXClientInstrumentor().uninstrument()
    AioHttpClientInstrumentor().uninstrument()
