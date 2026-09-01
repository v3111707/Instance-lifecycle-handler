"""Production entry point: connect to AMQP and consume ``config.amqp_queue`` until a shutdown signal."""

import logging
import signal
import asyncio
from aio_pika import connect_robust
from aio_pika.exceptions import ChannelNotFoundEntity
from aio_pika.abc import FieldValue

from g2c_instance_handler.config import Config
from g2c_instance_handler.observability import setup_logging, setup_sentry
from g2c_instance_handler.bootstrap_handlers import build_handler_service, start_metrics_server

LOGGER_NAME = 'main'
logger = logging.getLogger(LOGGER_NAME)

shutdown_event = asyncio.Event()


def handle_shutdown():
    """Signal handler: set ``shutdown_event`` so the consume loop can shut down gracefully."""
    logger.warning("Shutdown signal received")
    shutdown_event.set()


async def main() -> None:
    """Connect to the broker, consume ``config.amqp_queue``, and run until a shutdown signal.

    Declares the queue passively first; if it does not exist, falls back to
    declaring it durable. Waits for in-flight tasks to finish before closing
    the connection.
    """
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown)

    config = Config.from_env()
    setup_logging(
        log_level=config.log_level
    )
    setup_sentry(
        dsn=config.sentry_dsn,
        environment=config.sentry_env
    )

    logger.debug('Starting...')

    prometheus_port = 8000
    await start_metrics_server(prometheus_port)

    handler_service = build_handler_service(max_workers=10)


    client_properties: dict[str, FieldValue] = {
        'name': f'{config.k8s_node_name}:{config.k8s_namespace}/{config.k8s_pod_name}'
    }
    async with await connect_robust(
            host=config.amqp_host,
            port=config.amqp_port,
            virtualhost=config.amqp_vhost,
            login=config.amqp_username,
            password=config.amqp_password,
            ssl=config.amqp_ssl,
            client_properties=client_properties
    ) as connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=config.amqp_prefetch_count)

        try:
            queue = await channel.declare_queue(name=config.amqp_queue, passive=True)
        except ChannelNotFoundEntity:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=config.amqp_prefetch_count)
            queue = await channel.declare_queue(name=config.amqp_queue, durable=True)


        consumer_tag = await queue.consume(handler_service.get_wrapper())
        broker = f'{config.amqp_host}/{config.amqp_vhost}'
        logger.info(f'Consuming messages from {broker!r}, queue: {config.amqp_queue!r}')

        await shutdown_event.wait()

        logger.info("Shutting down consumer...")
        await queue.cancel(consumer_tag)

        logger.info("Waiting for in-flight tasks to complete...")
        if handler_service.active_tasks:
            await asyncio.gather(*handler_service.active_tasks)

    logger.info("Connection closed")


if __name__ == "__main__":
    asyncio.run(main())
