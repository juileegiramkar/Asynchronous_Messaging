"""Order Producer: publishes order messages to the RabbitMQ 'orders' queue.

    python producer.py --count 6
    python producer.py --count 3 --duplicate    # then re-sends the first event with the same event_id
"""
import argparse
import json
import random
import time
import uuid

import pika

from common import QUEUE_NAME, connect, setup_logging

ITEMS = ["ITEM-KEYBOARD", "ITEM-MOUSE", "ITEM-MONITOR", "ITEM-HEADSET", "ITEM-WEBCAM"]


def parse_args():
    parser = argparse.ArgumentParser(description="Publish order messages to RabbitMQ.")
    parser.add_argument("--count", type=int, default=5, help="number of orders to publish (default 5)")
    parser.add_argument("--start-order", type=int, default=1001, help="first order number, e.g. 1001 -> ORD-1001")
    parser.add_argument("--delay", type=float, default=0.0, help="seconds to wait between messages")
    parser.add_argument(
        "--duplicate",
        action="store_true",
        help="after the batch, publish the first order again with the same event_id (a simulated producer retry)",
    )
    parser.add_argument("--retry-after", type=float, default=2.0, help="seconds before the --duplicate re-send")
    return parser.parse_args()


def publish(channel, order):
    channel.basic_publish(
        exchange="",  # the default exchange routes straight to the queue named by routing_key
        routing_key=QUEUE_NAME,
        body=json.dumps(order),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,  # written to disk, survives a broker restart
            message_id=order["event_id"],
        ),
        mandatory=True,  # raise if the queue does not exist instead of silently dropping the message
    )


def main():
    args = parse_args()
    log = setup_logging("producer")
    connection, channel = connect(log, "producer")
    # Publisher confirms: basic_publish blocks until the broker has taken the message,
    # and raises if it was rejected, so "PUBLISHED" below means the broker has it.
    channel.confirm_delivery()

    orders = []
    for n in range(args.count):
        order = {
            "event_id": str(uuid.uuid4()),
            "order_id": f"ORD-{args.start_order + n}",
            "item_id": random.choice(ITEMS),
            "quantity": random.randint(1, 5),
        }
        publish(channel, order)
        orders.append(order)
        log.info(
            "PUBLISHED event_id=%s order_id=%s item_id=%s quantity=%s",
            order["event_id"], order["order_id"], order["item_id"], order["quantity"],
        )
        if args.delay:
            time.sleep(args.delay)

    if args.duplicate and orders:
        time.sleep(args.retry_after)
        first = orders[0]
        publish(channel, first)
        log.info(
            "PUBLISHED AGAIN event_id=%s order_id=%s (simulated producer retry: same event_id -> duplicate)",
            first["event_id"], first["order_id"],
        )

    connection.close()
    total = len(orders) + (1 if args.duplicate and orders else 0)
    log.info("Done: %d message(s) published to queue '%s'", total, QUEUE_NAME)


if __name__ == "__main__":
    main()
