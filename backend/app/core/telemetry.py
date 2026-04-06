"""
OpenTelemetry instrumentation for eDeltaShared.

Provides distributed tracing across:
- FastAPI backend (HTTP requests)
- CDC Agent (shadow table reads, Kafka produces)
- Consumers (Kafka consume, target writes)

Trace context propagates through Kafka message headers so a single
trace spans: HANA trigger → CDC Agent → Kafka → Consumer → Target.
"""

import logging
import os

logger = logging.getLogger("edeltashared.telemetry")

OTEL_ENABLED = os.getenv("OTEL_ENABLED", "false").lower() == "true"
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "edeltashared")


def init_telemetry(service_name: str | None = None) -> None:
    """Initialize OpenTelemetry tracing.

    Call once at startup. No-op if OTEL_ENABLED is not true.
    """
    if not OTEL_ENABLED:
        logger.info("OpenTelemetry disabled (set OTEL_ENABLED=true to enable)")
        return

    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource.create({
            "service.name": service_name or OTEL_SERVICE_NAME,
            "service.version": "0.1.0",
        })

        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)

        logger.info(f"OpenTelemetry initialized: {service_name or OTEL_SERVICE_NAME} → {OTEL_ENDPOINT}")

    except ImportError:
        logger.warning("OpenTelemetry packages not installed — tracing disabled")


def instrument_fastapi(app) -> None:
    """Instrument FastAPI with OpenTelemetry."""
    if not OTEL_ENABLED:
        return

    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        FastAPIInstrumentor.instrument_app(app)
        logger.info("FastAPI instrumented with OpenTelemetry")
    except ImportError:
        logger.warning("opentelemetry-instrumentation-fastapi not installed")


def get_tracer(name: str = "edeltashared"):
    """Get an OpenTelemetry tracer instance."""
    if not OTEL_ENABLED:
        return _NoOpTracer()

    from opentelemetry import trace
    return trace.get_tracer(name)


class _NoOpTracer:
    """Stub tracer that does nothing when OTEL is disabled."""

    def start_as_current_span(self, name, **kwargs):
        return _NoOpContextManager()

    def start_span(self, name, **kwargs):
        return _NoOpSpan()


class _NoOpContextManager:
    def __enter__(self):
        return _NoOpSpan()

    def __exit__(self, *args):
        pass


class _NoOpSpan:
    def set_attribute(self, key, value):
        pass

    def add_event(self, name, attributes=None):
        pass

    def set_status(self, status):
        pass

    def end(self):
        pass


def inject_trace_context(headers: dict) -> dict:
    """Inject current trace context into Kafka message headers."""
    if not OTEL_ENABLED:
        return headers

    try:
        from opentelemetry.context import get_current
        from opentelemetry.propagators import inject

        carrier = dict(headers) if headers else {}
        inject(carrier)
        return carrier
    except ImportError:
        return headers


def extract_trace_context(headers: dict):
    """Extract trace context from Kafka message headers."""
    if not OTEL_ENABLED:
        return None

    try:
        from opentelemetry.propagators import extract
        return extract(headers)
    except ImportError:
        return None
