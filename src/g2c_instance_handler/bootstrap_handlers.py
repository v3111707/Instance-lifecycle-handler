"""Wires the AMQP consumer to ``InstanceHandler``: retries, ack/nack, task
tracking, and the Prometheus metrics HTTP server."""

import asyncio
import logging
import json
from json import JSONDecodeError
from aio_pika.abc import AbstractIncomingMessage
from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from g2c_instance_handler.config import Config
from g2c_instance_handler.handler_models import PermanentError

from g2c_instance_handler.handler import InstanceHandler as MessageHandler
from g2c_instance_handler.observability import CUSTOM_REGISTRY, instance_message_exceptions_total, instance_messages_total

logger = logging.getLogger("main")


class MessageHandlerService:
    """Runs ``handler.process_message`` per message, with retries, and
    ack/nacks it via the broker, tracking in-flight tasks."""

    def __init__(
            self,
            handler: MessageHandler,
            max_retries: int,
            retry_delay: int = 2,
            ack_retries: int = 3,
    ):
        self.handler = handler
        self.active_tasks: set[asyncio.Task] = set()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.ack_retries = ack_retries


    async def _confirm(self, action, label):
        """Run ``action`` (ack or nack), retrying up to ``ack_retries`` times on failure.

        If it never succeeds, logs and gives up — the broker will requeue
        once it notices the dropped connection.
        """
        for attempt in range(1, self.ack_retries + 1):
            try:
                await action()
                return
            except Exception as e:
                logger.warning(f"{label} attempt {attempt}/{self.ack_retries} failed: {e}")
                if attempt < self.ack_retries:
                    await asyncio.sleep(self.retry_delay)
        logger.error(f"{label} failed after {self.ack_retries} attempts; "
                     f"relying on the broker to requeue once it drops the connection")


    def done_callback(self, task: asyncio.Task):
        """Untrack a finished task, logging whether it was cancelled, raised, or completed normally."""
        self.active_tasks.discard(task)
        if task.cancelled():
            logger.warning("Task cancelled: %s", task)
        elif task.exception():
            logger.error("Task error: %s", task.exception())
        else:
            logger.debug("Task finished: %s", task)

    async def handle_message(self, message: AbstractIncomingMessage):
        """Process one message with retries, then ack it, or nack it to the
        dead-letter queue on failure.

        ``PermanentError``, ``JSONDecodeError`` and ``KeyError`` abort
        immediately without retry; any other exception is retried up to
        ``max_retries`` times with a growing delay before also going to the
        dead-letter queue.
        """
        instance_messages_total.inc()
        retry_delay = self.retry_delay
        try:
            for attempt in range(1, self.max_retries + 1):
                try:
                    await self.handler.process_message(json.loads(message.body))
                    break
                except (PermanentError, JSONDecodeError, KeyError):
                    raise
                except Exception as e:
                    logger.warning(f"Attempt {attempt} failed: {e}")
                    if attempt < self.max_retries:
                        await asyncio.sleep(retry_delay)
                        retry_delay += 5
                    else:
                        raise

        except Exception as e:
            instance_message_exceptions_total.inc()
            logger.exception("Permanent message exception: %s", e)
            logger.info("Message is sending to the x-dead-letter queue. message.nack(requeue=False).")
            await self._confirm(lambda: message.nack(requeue=False), "nack")
            return

        await self._confirm(message.ack, "ack")


    def get_wrapper(self):
        """Return a queue consumer callback that runs ``handle_message`` as a tracked background task."""
        async def wrapper(message: AbstractIncomingMessage):
            task = asyncio.create_task(self.handle_message(message))
            task.add_done_callback(self.done_callback)
            self.active_tasks.add(task)
        return wrapper

def build_handler_service(max_workers: int = 10) -> MessageHandlerService:
    """Build a ``MessageHandlerService`` wired to an ``InstanceHandler``, both configured from the environment."""
    config = Config.from_env()
    message_handler = MessageHandler(
        cmdb_url=config.cmdb_url,
        cmdb_username=config.cmdb_username,
        cmdb_password=config.cmdb_password,
        max_workers=max_workers,
        cmdb_transport_attempts=config.cmdb_transport_attempts
    )

    return MessageHandlerService(
        handler=message_handler,
        max_retries=config.max_message_process_retries,
        retry_delay=config.min_message_process_retry_delay,
    )

def render_metrics():
    """Render all registered Prometheus metrics in text exposition format."""
    return generate_latest(CUSTOM_REGISTRY)

async def metrics_handler(request):
    """HTTP handler returning the current Prometheus metrics."""
    data = render_metrics()
    return web.Response(
        body=data,
        headers={ "Content-Type": CONTENT_TYPE_LATEST }
    )

async def start_metrics_server(
        port: int = 8000,
        host: str = "0.0.0.0",
        path: str = '/metrics'
):
    """Start an HTTP server exposing Prometheus metrics at ``path``."""
    app = web.Application()
    app.router.add_get(path=path, handler=metrics_handler)
    runner = web.AppRunner(app=app)
    await runner.setup()
    logger.info(f'Prometheus metrics server started on http://{host}:{port}{path}')
    site = web.TCPSite(runner=runner, host=host, port=port)
    await site.start()
