"""Handler configuration, loaded from environment variables."""

import logging
import os
from dataclasses import dataclass

logger = logging.getLogger('main.config')

def str_to_bool(value):
    """``True`` for ``'true'``, ``'1'``, ``'yes'`` or ``'on'`` (case-insensitive); ``False`` otherwise."""
    return str(value).lower() in ('true', '1', 'yes', 'on')


def env_str(name, default=None):
    """Read the string environment variable ``name``, or raise if unset and no ``default`` is given."""
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f'{name} is required but is not set')
    return value


def env_int(name, default=None, minimum=None):
    """Read the integer environment variable ``name``.

    Falls back to ``default`` — with a warning — if the value is missing, not
    an integer, or below ``minimum``. Raises if there is no ``default`` to
    fall back to.
    """
    raw = os.getenv(name)
    if raw is None:
        if default is None:
            raise RuntimeError(f'{name} is required but is not set')
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        if default is None:
            raise RuntimeError(f'{name}={raw!r} is not an integer')
        logger.warning(f'{name}={raw!r} is not an integer; falling back to {default}')
        return default
    if minimum is not None and value < minimum:
        if default is None:
            raise RuntimeError(f'{name}={value} is below the minimum of {minimum}')
        logger.warning(
            f'{name}={value} is below the minimum of {minimum}; '
            f'falling back to {default}'
        )
        return default
    return value


@dataclass
class Config:
    """Handler configuration. Build with ``from_env``."""

    cmdb_url: str
    cmdb_username: str
    cmdb_password: str

    amqp_host: str
    amqp_port: int
    amqp_username: str
    amqp_password: str
    amqp_vhost: str
    amqp_queue: str

    sentry_dsn: str
    k8s_pod_name: str
    k8s_namespace: str
    k8s_node_name: str
    amqp_ssl: bool = True
    max_message_process_retries: int = 3
    min_message_process_retry_delay: int = 2
    amqp_prefetch_count: int = 10
    #: Handler-level attempts per CMDB operation — attempts, not retries.
    cmdb_transport_attempts: int = 5
    sentry_env: str = 'local'
    log_level: str = 'INFO'

    @classmethod
    def from_env(cls) -> 'Config':
        """Build a ``Config`` by reading every setting from its environment variable."""
        return cls(
            cmdb_url=env_str('CMDB_URL'),
            cmdb_username=env_str('CMDB_USERNAME'),
            cmdb_password=env_str('CMDB_PASSWORD'),

            amqp_host=env_str('AMQP_HOST'),
            amqp_port=env_int('AMQP_PORT', minimum=1),
            amqp_ssl=str_to_bool(os.getenv('AMQP_SSL', str(cls.amqp_ssl))),
            amqp_username=env_str('AMQP_USERNAME'),
            amqp_password=env_str('AMQP_PASSWORD'),
            amqp_vhost=env_str('AMQP_VHOST'),
            amqp_queue=env_str('AMQP_QUEUE'),
            amqp_prefetch_count=env_int(
                'AMQP_PREFETCH_COUNT', cls.amqp_prefetch_count, minimum=1
            ),

            max_message_process_retries=env_int(
                'MAX_MESSAGE_PROCESS_RETRIES', cls.max_message_process_retries, minimum=1
            ),
            min_message_process_retry_delay=env_int(
                'MIN_MESSAGE_PROCESS_RETRY_DELAY', cls.min_message_process_retry_delay,
                minimum=0
            ),
            # Optional: an empty dsn turns Sentry off in ``setup_sentry``, and
            # the k8s names only decorate the amqp connection's client name.
            sentry_dsn=env_str('SENTRY_DSN', ''),
            k8s_pod_name=env_str('K8S_POD_NAME', ''),
            k8s_namespace=env_str('K8S_NAMESPACE', ''),
            k8s_node_name=env_str('K8S_NODE_NAME', ''),

            cmdb_transport_attempts=env_int(
                'CMDB_TRANSPORT_ATTEMPTS', cls.cmdb_transport_attempts, minimum=1
            ),

            sentry_env=env_str('SENTRY_ENV', cls.sentry_env),
            log_level=env_str('LOG_LEVEL', cls.log_level),
        )
