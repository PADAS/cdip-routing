from opentelemetry.propagators.cloud_trace_propagator import (
    CloudTraceFormatPropagator,
)
from opentelemetry.propagate import set_global_textmap
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
from . import config
from . import faust_instrumentation

# Capture requests (sync and async)
RequestsInstrumentor().instrument()
AioHttpClientInstrumentor().instrument()
# Using the X-Cloud-Trace-Context header
set_global_textmap(CloudTraceFormatPropagator())
tracer = config.configure_tracer(name="cdip-routing", version="1.0.8")
