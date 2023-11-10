"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import os
from opentelemetry import trace
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor, BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider

resource = Resource(attributes={
    "service.name": "arcticdb"
})

trace.set_tracer_provider(TracerProvider(resource=resource))

if os.getenv("ARCTICDB_OT_PRINT_TO_CONSOLE", "0") == "1":
    console_exporter = ConsoleSpanExporter()
    trace.get_tracer_provider().add_span_processor(SimpleSpanProcessor(console_exporter))

oltp_exporter_endpoint = os.getenv("ARCTICDB_OLTP_ENDPOINT")
if oltp_exporter_endpoint:
    otlp_exporter = OTLPSpanExporter(endpoint=oltp_exporter_endpoint, insecure=True)
    trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(otlp_exporter))

tracer = trace.get_tracer(__name__)

from functools import wraps
from inspect import signature

def telemetry_trace(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with tracer.start_as_current_span(func.__qualname__):
            current_span = trace.get_current_span()
            sig = signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            if 'symbol' in bound.arguments:
                current_span.set_attribute(f"{func.__qualname__}.symbol", bound.arguments['symbol'])
            return func(*args, **kwargs)
    return wrapper
