"""Structured JSON logging, Sentry setup, and Prometheus metric definitions."""

import sys
import json
import logging
import sentry_sdk
from datetime import UTC, datetime
from sentry_sdk.integrations.logging import LoggingIntegration
from prometheus_client import Counter, Histogram, CollectorRegistry

CUSTOM_REGISTRY = CollectorRegistry(auto_describe=True)
LOGGER_NAME = 'main'


class JsonFormatter(logging.Formatter):
    """Formats each log record as one JSON line with time, level, name,
    message, and (if present) exception, stack and hostname."""

    def format(self, record):
        log_record = {
            "time": datetime.fromtimestamp(
                record.created, UTC
            ).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        if record.stack_info:
            log_record["stack"] = self.formatStack(record.stack_info)

        if hasattr(record, "hostname"):
            log_record["hostname"] = record.hostname

        return json.dumps(log_record, ensure_ascii=False)



def setup_logging(log_level: str):
    """Configure the ``main`` logger to write JSON lines to stdout at ``log_level``."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(log_level.upper())
    logger.addHandler(handler)


def setup_sentry(dsn: str, environment: str = "local"):
    """Initialize the Sentry SDK for ``environment``. Does nothing if ``dsn`` is empty."""
    if not dsn:
        return
    sentry_logging = LoggingIntegration(
        level=logging.DEBUG,
        event_level=logging.ERROR
    )
    sentry_sdk.init(
        dsn=dsn,
        integrations=[sentry_logging],
        traces_sample_rate=0.1,
        environment=environment,
    )


instance_messages_total = Counter(
    name="instance_messages_received_total",
    documentation="Total number of received instance messages",
    registry=CUSTOM_REGISTRY,
)
instance_message_exceptions_total = Counter(
    name="instance_messages_exceptions_total",
    documentation="Total number of exceptions during processing of instance messages",
    registry=CUSTOM_REGISTRY,
)

instance_messages_create = Counter(
    name="instance_messages_create_processed_total",
    documentation="Total number of processed instance create messages",
    registry=CUSTOM_REGISTRY,
)
instance_messages_resize = Counter(
    name="instance_messages_resize_processed_total",
    documentation="Total number of processed instance resize messages",
    registry=CUSTOM_REGISTRY,
)
instance_messages_delete = Counter(
    name="instance_messages_delete_processed_total",
    documentation="Total number of processed instance delete messages",
    registry=CUSTOM_REGISTRY,
)

instance_messages_attach = Counter(
    name="instance_messages_attach_processed_total",
    documentation="Total number of processed instance attach messages",
    registry=CUSTOM_REGISTRY,
)

instance_messages_detach = Counter(
    name="instance_messages_detach_processed_total",
    documentation="Total number of processed instance detach messages",
    registry=CUSTOM_REGISTRY,
)

instance_create_processing_duration = Histogram(
    name="instance_create_processing_duration_seconds",
    documentation="Time spent processing create messages",
    registry=CUSTOM_REGISTRY,
)
instance_delete_processing_duration = Histogram(
    name="instance_delete_processing_duration_seconds",
    documentation="Time spent processing delete messages",
    registry=CUSTOM_REGISTRY,
)
instance_resize_processing_duration = Histogram(
    name="instance_resize_processing_duration_seconds",
    documentation="Time spent processing resize messages",
    registry=CUSTOM_REGISTRY,
)

instance_attach_processing_duration = Histogram(
    name="instance_attach_processing_duration_seconds",
    documentation="Time spent processing attach messages",
    registry=CUSTOM_REGISTRY,
)

instance_detach_processing_duration = Histogram(
    name="instance_detach_processing_duration_seconds",
    documentation="Time spent processing detach messages",
    registry=CUSTOM_REGISTRY,
)
