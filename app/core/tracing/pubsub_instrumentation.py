from opentelemetry import propagate


def build_context_headers():
    headers = {}
    propagate.inject(headers)
    return headers
