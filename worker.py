"""Order Worker: consumes order messages from the 'orders' queue and processes each event once.

Run two at the same time to get competing consumers on one queue:
    python worker.py --name worker-1
    python worker.py --name worker-2

Simulate a worker that dies before sending the ACK:
    python worker.py --name worker-1 --crash-on-message 1
"""
import argparse
import json
import os
import time
from datetime import datetime

from common import QUEUE_NAME, connect, setup_logging
from idempotency import ProcessedEventStore

REQUIRED_FIELDS = ("event_id", "order_id", "item_id", "quantity")
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))


def parse_args():
    parser = argparse.ArgumentParser(description="Consume and process order messages from RabbitMQ.")
    parser.add_argument("--name", default=f"worker-{os.getpid()}", help="worker name used in logs and as the consumer tag")
    parser.add_argument("--work-seconds", type=float, default=1.0, help="simulated processing time per order (default 1.0)")
    parser.add_argument(
        "--crash-on-message",
        type=int,
        metavar="N",
        help="exit abruptly, without sending the ACK, while handling the Nth message this worker receives",
    )
    parser.add_argument(
        "--crash-stage",
        choices=("before-commit", "after-commit"),
        default="before-commit",
        help="before-commit: die mid-processing, before the order is applied; "
        "after-commit: die after the order is applied but before the ACK",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    log = setup_logging(args.name)
    store = ProcessedEventStore(DATA_DIR)
    ledger_path = os.path.join(DATA_DIR, "inventory_reservations.log")
    received = 0

    def reserve_inventory(order):
        """The business action: record the stock reservation for this order."""
        with open(ledger_path, "a", encoding="utf-8") as ledger:
            ledger.write(
                f"{datetime.now().isoformat(timespec='milliseconds')} reserved {order['quantity']} x "
                f"{order['item_id']} for {order['order_id']} event_id={order['event_id']} by={args.name}\n"
            )

    def crash_if_requested(stage):
        if received == args.crash_on_message and args.crash_stage == stage:
            log.warning("SIMULATED CRASH (%s): process exits now, ACK never sent", stage)
            os._exit(1)  # hard exit, like a killed process: no ACK and no clean channel close

    def on_message(channel, method, properties, body):
        nonlocal received
        received += 1
        try:
            order = json.loads(body)
            missing = [field for field in REQUIRED_FIELDS if field not in order]
            if missing:
                raise ValueError(f"missing fields {missing}")
        except ValueError as exc:
            log.error("REJECTED malformed message (%s): %r", exc, body[:200])
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        event_id, order_id = order["event_id"], order["order_id"]
        log.info(
            "RECEIVED event_id=%s order_id=%s item_id=%s quantity=%s redelivered=%s",
            event_id, order_id, order["item_id"], order["quantity"], method.redelivered,
        )

        if store.contains(event_id):
            channel.basic_ack(delivery_tag=method.delivery_tag)
            log.info("DUPLICATE event_id=%s (%s) was already processed -> business action skipped, ACK sent", event_id, order_id)
            return

        log.info("PROCESSING %s: reserving %s x %s", order_id, order["quantity"], order["item_id"])
        time.sleep(args.work_seconds)
        crash_if_requested("before-commit")

        # Check again under the lock: another worker may have finished a copy of this
        # event while we were working.
        with store.lock():
            duplicate = store.contains(event_id)
            if not duplicate:
                reserve_inventory(order)
                store.add(event_id)
        if duplicate:
            channel.basic_ack(delivery_tag=method.delivery_tag)
            log.info("DUPLICATE event_id=%s (%s) was processed by another delivery meanwhile -> business action skipped, ACK sent", event_id, order_id)
            return

        crash_if_requested("after-commit")
        channel.basic_ack(delivery_tag=method.delivery_tag)
        log.info("DONE %s processed -> ACK sent", order_id)

    connection, channel = connect(log, args.name)
    # prefetch=1: the broker hands this worker one unacknowledged message at a time,
    # so work is shared between competing workers instead of piling up on one.
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False, consumer_tag=args.name)
    crash_note = f", will crash on message {args.crash_on_message} ({args.crash_stage})" if args.crash_on_message else ""
    log.info("Connected; consuming from queue '%s' (prefetch=1, manual ACK)%s", QUEUE_NAME, crash_note)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log.info("Stopping (Ctrl+C)")
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
