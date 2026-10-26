"""Settings and helpers shared by the producer and the worker."""
import logging
import os
import sys
import time

import pika

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
QUEUE_NAME = os.environ.get("ORDER_QUEUE", "orders")


def setup_logging(name):
    """Log to stdout with millisecond timestamps, so logs from all processes can be merged in order."""
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s.%(msecs)03d [{name} pid={os.getpid()}] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    logging.getLogger("pika").setLevel(logging.WARNING)
    return logging.getLogger(name)


def connect(log, connection_name, attempts=10):
    """Open a connection and channel, retrying while the broker is still starting up."""
    params = pika.URLParameters(RABBITMQ_URL)
    params.client_properties = {"connection_name": connection_name}
    for attempt in range(1, attempts + 1):
        try:
            connection = pika.BlockingConnection(params)
            break
        except pika.exceptions.AMQPConnectionError:
            if attempt == attempts:
                raise
            log.info("RabbitMQ not reachable yet (attempt %d/%d), retrying in 2s", attempt, attempts)
            time.sleep(2)
    channel = connection.channel()
    # Durable: the queue survives a broker restart. Declaring is idempotent, so the
    # producer and every worker declare it and whichever starts first creates it.
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    return connection, channel
