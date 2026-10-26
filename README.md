# Asynchronous Messaging with RabbitMQ: Order Producer and Order Workers

Module 5 assignment. A small asynchronous order pipeline in **Python** using **RabbitMQ**:

- **Order Producer** (`producer.py`) publishes order messages (`event_id`, `order_id`, `item_id`, `quantity`) to the `orders` queue.
- **Order Worker** (`worker.py`) consumes orders, logs each one, "reserves inventory" (the business action), and sends the **ACK only after processing finishes**.

The producer and each worker are independent processes. Two workers on the same queue act as **competing consumers**, a worker that dies before its ACK has its message **redelivered** to the other worker, and every worker **skips duplicate `event_id`s** using a shared processed-events file.

The logs below come from one run of [`scripts/demo.sh`](scripts/demo.sh) on 2026-10-26, and the raw logs are in [`evidence/`](evidence/). The RabbitMQ dashboard [screenshots](#screenshots-rabbitmq-management-ui) were taken during a separate live run the same day.

## Contents

1. [Architecture](#1-architecture)
2. [Repository layout](#2-repository-layout)
3. [How to run](#3-how-to-run)
4. [Evidence: successful message processing](#4-evidence-successful-message-processing)
5. [Evidence: two competing consumers](#5-evidence-two-competing-consumers)
6. [Evidence: failure and redelivery](#6-evidence-failure-and-redelivery)
7. [Evidence: duplicate handling](#7-evidence-duplicate-handling)
8. [Reflection](#8-reflection)
9. [Requirements checklist](#9-requirements-checklist)

## 1. Architecture

```mermaid
flowchart TB
    P["Order Producer<br/>producer.py"]
    subgraph MQ["RabbitMQ 4.1.8 (Docker)"]
        X(("default<br/>exchange"))
        Q[["queue: orders<br/>durable"]]
        X -- "routing key = orders" --> Q
    end
    W1["Order Worker<br/>worker-1"]
    W2["Order Worker<br/>worker-2"]
    S[("data/processed_events.txt<br/>data/inventory_reservations.log<br/>shared by both workers")]

    P -- "publish JSON order<br/>persistent, publisher confirms" --> X
    Q -- "deliver, prefetch=1" --> W1
    Q -- "deliver, prefetch=1" --> W2
    W1 -. "ACK after processing" .-> Q
    W2 -. "ACK after processing" .-> Q
    W1 -- "check / record event_id" --> S
    W2 -- "check / record event_id" --> S
```

What each worker does with a message:

```mermaid
flowchart TD
    A["message received<br/>log RECEIVED"] --> B{"event_id in<br/>processed_events.txt?"}
    B -- yes --> D["log DUPLICATE<br/>skip business action<br/>ACK"]
    B -- no --> C["log PROCESSING<br/>simulated work, 1s"]
    C --> E{"under file lock:<br/>event_id recorded now?"}
    E -- "yes, another copy won" --> D
    E -- no --> F["reserve inventory (ledger)<br/>record event_id"]
    F --> G["ACK<br/>log DONE"]
```

| Design choice | Why |
|---|---|
| Durable queue + persistent messages | Orders survive a RabbitMQ restart. |
| Publisher confirms (`confirm_delivery`) + `mandatory=True` | The producer logs `PUBLISHED` only after the broker has accepted the message. |
| Manual ACK (`auto_ack=False`), ACK sent last | If a worker dies mid-processing, the unacknowledged message goes back to the queue instead of being lost. |
| `prefetch_count=1` | Each worker holds one unacknowledged message at a time, so the two workers share the load. A crash also only affects that one message. |
| Consumer tag = worker name | `rabbitmqctl list_consumers` shows `worker-1` / `worker-2` by name. |
| Shared `processed_events.txt` with an OS file lock | A redelivery can land on a *different* worker, so a per-process in-memory set would miss it. The lock makes "check, apply, record" atomic across both workers. |

## 2. Repository layout

```
.
├── docker-compose.yml        RabbitMQ 4.1.8 with the management UI
├── requirements.txt          pika (RabbitMQ client)
├── common.py                 connection settings, queue declaration, logging format
├── producer.py               Order Producer
├── worker.py                 Order Worker (competing consumer, manual ACK, crash simulation)
├── idempotency.py            shared processed-event_id store with a cross-process file lock
├── scripts/demo.sh           runs every scenario and regenerates evidence/
├── evidence/                 logs and broker output from the run quoted below
├── screenshots/              RabbitMQ management UI with both workers connected
└── data/                     created at runtime: processed_events.txt, inventory_reservations.log
```

## 3. How to run

**Prerequisites:** Docker Desktop (or any Docker engine with Compose) and Python 3.10+ (tested with Python 3.13 on Windows 11).

### 3.1 Start RabbitMQ

```bash
docker compose up -d --wait
```

This starts `rabbitmq:4.1.8-management` with AMQP on `localhost:5672` and the management UI on <http://localhost:15672> (login `guest` / `guest`).

### 3.2 Install the Python dependency

```bash
python -m venv .venv
# Windows (PowerShell):    .venv\Scripts\Activate.ps1
# Windows (Git Bash):      source .venv/Scripts/activate
# macOS / Linux:           source .venv/bin/activate
pip install -r requirements.txt
```

### 3.3 Start two workers, each in its own terminal

```bash
python worker.py --name worker-1
```

```bash
python worker.py --name worker-2
```

Each worker prints `Connected; consuming from queue 'orders' (prefetch=1, manual ACK)` and waits for messages.

### 3.4 Publish orders, from a third terminal

```bash
python producer.py --count 6
```

The orders are split between the two terminals. Each worker logs `RECEIVED`, `PROCESSING` and `DONE ... -> ACK sent`.

### 3.5 Try the failure and duplicate scenarios by hand

| Scenario | Commands |
|---|---|
| Worker dies before ACK | Restart worker-1 as `python worker.py --name worker-1 --crash-on-message 1`, keep worker-2 running, then `python producer.py --count 4 --start-order 2001`. worker-1 exits on its first message, and worker-2 receives it with `redelivered=True`. |
| Producer re-sends an event | `python producer.py --count 3 --start-order 3001 --duplicate` publishes ORD-3001 a second time with the same `event_id`, and a worker logs `DUPLICATE`. |
| Worker dies after processing, before ACK | `python worker.py --name worker-1 --crash-on-message 1 --crash-stage after-commit`, then `python producer.py --count 2 --start-order 4001`. The redelivered message is detected as a duplicate. |

`Ctrl+C` stops a worker.

### 3.6 Or run everything automatically

```bash
bash scripts/demo.sh
```

On Windows, run it from Git Bash. The script starts RabbitMQ, resets the queue and `data/`, and runs the four scenarios with two background workers. It captures `rabbitmqctl` output and writes every log to `evidence/`. Each worker and producer log line starts with a millisecond timestamp, so each scenario also gets a merged `timeline.log`.

### 3.7 Stop

```bash
docker compose down
```

### Command-line options

| `producer.py` | Default | |
|---|---|---|
| `--count N` | 5 | number of orders to publish |
| `--start-order N` | 1001 | first order number (`ORD-1001`) |
| `--delay S` | 0 | seconds between messages |
| `--duplicate` | off | re-publish the first order with the same `event_id` (simulated producer retry) |
| `--retry-after S` | 2 | seconds before the duplicate is sent |

| `worker.py` | Default | |
|---|---|---|
| `--name NAME` | `worker-<pid>` | name in logs and the RabbitMQ consumer tag |
| `--work-seconds S` | 1.0 | simulated processing time per order |
| `--crash-on-message N` | off | hard-exit (`os._exit`) with no ACK while handling the Nth message this worker receives |
| `--crash-stage` | `before-commit` | `before-commit` dies mid-processing; `after-commit` dies after the reservation is recorded but before the ACK |

Environment variables: `RABBITMQ_URL` (default `amqp://guest:guest@localhost:5672/%2F`), `ORDER_QUEUE` (default `orders`), `DATA_DIR` (default `./data`).

## 4. Evidence: successful message processing

One order's full path through the system: published, received by worker-1, processed, then acknowledged. From [`evidence/01-competing-consumers/timeline.log`](evidence/01-competing-consumers/timeline.log):

```
2026-10-26 12:58:49.799 [producer pid=20384] PUBLISHED event_id=3272fd05-98de-4f74-bac5-73b6c643bb88 order_id=ORD-1001 item_id=ITEM-MONITOR quantity=1
2026-10-26 12:58:49.799 [worker-1 pid=13308] RECEIVED event_id=3272fd05-98de-4f74-bac5-73b6c643bb88 order_id=ORD-1001 item_id=ITEM-MONITOR quantity=1 redelivered=False
2026-10-26 12:58:49.799 [worker-1 pid=13308] PROCESSING ORD-1001: reserving 1 x ITEM-MONITOR
2026-10-26 12:58:50.808 [worker-1 pid=13308] DONE ORD-1001 processed -> ACK sent
```

The business action it performed, from [`evidence/05-final-check/inventory_reservations.log`](evidence/05-final-check/inventory_reservations.log):

```
2026-10-26T12:58:50.803 reserved 1 x ITEM-MONITOR for ORD-1001 event_id=3272fd05-98de-4f74-bac5-73b6c643bb88 by=worker-1
```

The message body is plain JSON, for example `{"event_id": "3272fd05-98de-4f74-bac5-73b6c643bb88", "order_id": "ORD-1001", "item_id": "ITEM-MONITOR", "quantity": 1}`.

> In the merged timelines a worker's `RECEIVED` line can appear a millisecond *before* the producer's `PUBLISHED` line. The producer logs only after the broker's publisher confirm comes back, and by then the broker has already pushed the message to an idle worker.

## 5. Evidence: two competing consumers

Both workers are registered on the same `orders` queue, each with manual ACK and prefetch 1. From [`evidence/01-competing-consumers/queue-state.txt`](evidence/01-competing-consumers/queue-state.txt), captured before publishing:

```
## two workers connected, before publishing
$ rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers
name	messages_ready	messages_unacknowledged	consumers
orders	0	0	2
$ rabbitmqctl list_consumers queue_name consumer_tag ack_required prefetch_count
queue_name	consumer_tag	ack_required	prefetch_count
orders	worker-2	true	1
orders	worker-1	true	1
```

The producer published 6 orders. RabbitMQ handed them out round-robin, and **each message was handled by exactly one worker**. Every `event_id` appears in exactly one worker log. [`evidence/01-competing-consumers/summary.txt`](evidence/01-competing-consumers/summary.txt):

```
worker-1 processed: ORD-1001 ORD-1003 ORD-1005 
worker-2 processed: ORD-1002 ORD-1004 ORD-1006 
```

The merged timeline shows the two workers processing in parallel: ORD-1001 and ORD-1002 run at the same time, then 1003/1004, then 1005/1006. From [`evidence/01-competing-consumers/timeline.log`](evidence/01-competing-consumers/timeline.log):

```
2026-10-26 12:58:46.449 [worker-1 pid=13308] Connected; consuming from queue 'orders' (prefetch=1, manual ACK)
2026-10-26 12:58:46.926 [worker-2 pid=37568] Connected; consuming from queue 'orders' (prefetch=1, manual ACK)
2026-10-26 12:58:49.799 [producer pid=20384] PUBLISHED event_id=3272fd05-98de-4f74-bac5-73b6c643bb88 order_id=ORD-1001 item_id=ITEM-MONITOR quantity=1
2026-10-26 12:58:49.799 [worker-1 pid=13308] RECEIVED event_id=3272fd05-98de-4f74-bac5-73b6c643bb88 order_id=ORD-1001 item_id=ITEM-MONITOR quantity=1 redelivered=False
2026-10-26 12:58:49.799 [worker-1 pid=13308] PROCESSING ORD-1001: reserving 1 x ITEM-MONITOR
2026-10-26 12:58:49.842 [producer pid=20384] PUBLISHED event_id=986062a9-f7c7-4f62-9ecf-f569fa122dfb order_id=ORD-1002 item_id=ITEM-HEADSET quantity=4
2026-10-26 12:58:49.842 [worker-2 pid=37568] RECEIVED event_id=986062a9-f7c7-4f62-9ecf-f569fa122dfb order_id=ORD-1002 item_id=ITEM-HEADSET quantity=4 redelivered=False
2026-10-26 12:58:49.842 [worker-2 pid=37568] PROCESSING ORD-1002: reserving 4 x ITEM-HEADSET
2026-10-26 12:58:49.886 [producer pid=20384] PUBLISHED event_id=78fdabfa-c488-4445-aca2-c0536005ab8a order_id=ORD-1003 item_id=ITEM-WEBCAM quantity=1
2026-10-26 12:58:49.930 [producer pid=20384] PUBLISHED event_id=c468ee30-f436-44b0-8495-8f82adec1429 order_id=ORD-1004 item_id=ITEM-KEYBOARD quantity=5
2026-10-26 12:58:49.975 [producer pid=20384] PUBLISHED event_id=44627d29-e65d-4529-a0c5-95d4a8df6999 order_id=ORD-1005 item_id=ITEM-MOUSE quantity=1
2026-10-26 12:58:50.019 [producer pid=20384] PUBLISHED event_id=f21cb0a0-e2cc-4fcd-8247-7a4c49d815e8 order_id=ORD-1006 item_id=ITEM-KEYBOARD quantity=1
2026-10-26 12:58:50.021 [producer pid=20384] Done: 6 message(s) published to queue 'orders'
2026-10-26 12:58:50.808 [worker-1 pid=13308] DONE ORD-1001 processed -> ACK sent
2026-10-26 12:58:50.811 [worker-1 pid=13308] RECEIVED event_id=78fdabfa-c488-4445-aca2-c0536005ab8a order_id=ORD-1003 item_id=ITEM-WEBCAM quantity=1 redelivered=False
2026-10-26 12:58:50.817 [worker-1 pid=13308] PROCESSING ORD-1003: reserving 1 x ITEM-WEBCAM
2026-10-26 12:58:50.849 [worker-2 pid=37568] DONE ORD-1002 processed -> ACK sent
2026-10-26 12:58:50.852 [worker-2 pid=37568] RECEIVED event_id=c468ee30-f436-44b0-8495-8f82adec1429 order_id=ORD-1004 item_id=ITEM-KEYBOARD quantity=5 redelivered=False
2026-10-26 12:58:50.861 [worker-2 pid=37568] PROCESSING ORD-1004: reserving 5 x ITEM-KEYBOARD
2026-10-26 12:58:51.821 [worker-1 pid=13308] DONE ORD-1003 processed -> ACK sent
2026-10-26 12:58:51.822 [worker-1 pid=13308] RECEIVED event_id=44627d29-e65d-4529-a0c5-95d4a8df6999 order_id=ORD-1005 item_id=ITEM-MOUSE quantity=1 redelivered=False
2026-10-26 12:58:51.830 [worker-1 pid=13308] PROCESSING ORD-1005: reserving 1 x ITEM-MOUSE
2026-10-26 12:58:51.867 [worker-2 pid=37568] DONE ORD-1004 processed -> ACK sent
2026-10-26 12:58:51.868 [worker-2 pid=37568] RECEIVED event_id=f21cb0a0-e2cc-4fcd-8247-7a4c49d815e8 order_id=ORD-1006 item_id=ITEM-KEYBOARD quantity=1 redelivered=False
2026-10-26 12:58:51.874 [worker-2 pid=37568] PROCESSING ORD-1006: reserving 1 x ITEM-KEYBOARD
2026-10-26 12:58:52.839 [worker-1 pid=13308] DONE ORD-1005 processed -> ACK sent
2026-10-26 12:58:52.902 [worker-2 pid=37568] DONE ORD-1006 processed -> ACK sent
```

### Screenshots: RabbitMQ management UI

These were taken during a separate live run the same day. `worker-1` and `worker-2` were running while `python producer.py --count 400 --start-order 5001 --delay 0.75` published about 1.3 orders per second. The page is <http://localhost:15672/#/queues/%2F/orders> (login `guest` / `guest`).

The `orders` queue page shows about 1.2 messages/s published and delivered with manual ACK, with consumer ACKs keeping pace. **Ready is 0**, so nothing piles up. **Unacked is 2**, one message in progress per worker because of prefetch 1. Under Details, **Consumers: 2**.

![RabbitMQ queue "orders": live message rates, Unacked 2, Consumers 2](screenshots/rabbitmq-queue-orders-overview.png)

Further down the same page, the **Consumers (2)** table lists both workers by consumer tag, each on its own channel with *Ack required* and *Prefetch count 1*:

![RabbitMQ queue "orders": Consumers table listing worker-1 and worker-2](screenshots/rabbitmq-queue-orders-consumers.png)

## 6. Evidence: failure and redelivery

worker-1 was started with `--crash-on-message 1 --crash-stage before-commit`. It receives a message, starts processing, and then hard-exits (`os._exit(1)`) **without sending the ACK**, just like a killed process. RabbitMQ sees the connection drop, puts the unacknowledged message back on the queue, and delivers it to worker-2 with `redelivered=True`. From [`evidence/02-failure-redelivery/timeline.log`](evidence/02-failure-redelivery/timeline.log):

```
2026-10-26 12:58:56.272 [worker-1 pid=39412] Connected; consuming from queue 'orders' (prefetch=1, manual ACK), will crash on message 1 (before-commit)
2026-10-26 12:58:56.801 [worker-2 pid=30372] Connected; consuming from queue 'orders' (prefetch=1, manual ACK)
2026-10-26 12:58:57.379 [worker-1 pid=39412] RECEIVED event_id=a32933dd-f92a-4919-99e4-83efa457d3d1 order_id=ORD-2001 item_id=ITEM-HEADSET quantity=2 redelivered=False
2026-10-26 12:58:57.380 [producer pid=32756] PUBLISHED event_id=a32933dd-f92a-4919-99e4-83efa457d3d1 order_id=ORD-2001 item_id=ITEM-HEADSET quantity=2
2026-10-26 12:58:57.386 [worker-1 pid=39412] PROCESSING ORD-2001: reserving 2 x ITEM-HEADSET
2026-10-26 12:58:57.426 [producer pid=32756] PUBLISHED event_id=80fcbffa-e411-4c8e-8c25-5d2e12099cea order_id=ORD-2002 item_id=ITEM-KEYBOARD quantity=4
2026-10-26 12:58:57.426 [worker-2 pid=30372] RECEIVED event_id=80fcbffa-e411-4c8e-8c25-5d2e12099cea order_id=ORD-2002 item_id=ITEM-KEYBOARD quantity=4 redelivered=False
2026-10-26 12:58:57.426 [worker-2 pid=30372] PROCESSING ORD-2002: reserving 4 x ITEM-KEYBOARD
2026-10-26 12:58:57.470 [producer pid=32756] PUBLISHED event_id=e1cd99c5-3cc8-45be-a29f-7f3db2147094 order_id=ORD-2003 item_id=ITEM-WEBCAM quantity=3
2026-10-26 12:58:57.514 [producer pid=32756] PUBLISHED event_id=1b4a9138-9a93-4254-9195-44b57d56a323 order_id=ORD-2004 item_id=ITEM-WEBCAM quantity=4
2026-10-26 12:58:57.516 [producer pid=32756] Done: 4 message(s) published to queue 'orders'
2026-10-26 12:58:58.386 [worker-1 pid=39412] SIMULATED CRASH (before-commit): process exits now, ACK never sent
2026-10-26 12:58:58.430 [worker-2 pid=30372] DONE ORD-2002 processed -> ACK sent
2026-10-26 12:58:58.432 [worker-2 pid=30372] RECEIVED event_id=a32933dd-f92a-4919-99e4-83efa457d3d1 order_id=ORD-2001 item_id=ITEM-HEADSET quantity=2 redelivered=True
2026-10-26 12:58:58.438 [worker-2 pid=30372] PROCESSING ORD-2001: reserving 2 x ITEM-HEADSET
2026-10-26 12:58:59.442 [worker-2 pid=30372] DONE ORD-2001 processed -> ACK sent
2026-10-26 12:58:59.443 [worker-2 pid=30372] RECEIVED event_id=e1cd99c5-3cc8-45be-a29f-7f3db2147094 order_id=ORD-2003 item_id=ITEM-WEBCAM quantity=3 redelivered=False
2026-10-26 12:58:59.450 [worker-2 pid=30372] PROCESSING ORD-2003: reserving 3 x ITEM-WEBCAM
2026-10-26 12:59:00.454 [worker-2 pid=30372] DONE ORD-2003 processed -> ACK sent
2026-10-26 12:59:00.456 [worker-2 pid=30372] RECEIVED event_id=1b4a9138-9a93-4254-9195-44b57d56a323 order_id=ORD-2004 item_id=ITEM-WEBCAM quantity=4 redelivered=False
2026-10-26 12:59:00.462 [worker-2 pid=30372] PROCESSING ORD-2004: reserving 4 x ITEM-WEBCAM
2026-10-26 12:59:01.466 [worker-2 pid=30372] DONE ORD-2004 processed -> ACK sent
2026-10-26 12:59:04.035 [demo] --- restarting worker-1, then publishing 2 more orders ---
2026-10-26 12:59:04.351 [worker-1 pid=33744] Connected; consuming from queue 'orders' (prefetch=1, manual ACK)
2026-10-26 12:59:04.881 [worker-2 pid=30372] RECEIVED event_id=0e65d0a6-94c1-47e6-96c7-49b695758526 order_id=ORD-2005 item_id=ITEM-KEYBOARD quantity=2 redelivered=False
2026-10-26 12:59:04.883 [producer pid=31564] PUBLISHED event_id=0e65d0a6-94c1-47e6-96c7-49b695758526 order_id=ORD-2005 item_id=ITEM-KEYBOARD quantity=2
2026-10-26 12:59:04.890 [worker-2 pid=30372] PROCESSING ORD-2005: reserving 2 x ITEM-KEYBOARD
2026-10-26 12:59:04.929 [producer pid=31564] PUBLISHED event_id=844912b4-be48-4911-94ea-580da95511b8 order_id=ORD-2006 item_id=ITEM-WEBCAM quantity=4
2026-10-26 12:59:04.929 [worker-1 pid=33744] RECEIVED event_id=844912b4-be48-4911-94ea-580da95511b8 order_id=ORD-2006 item_id=ITEM-WEBCAM quantity=4 redelivered=False
2026-10-26 12:59:04.929 [worker-1 pid=33744] PROCESSING ORD-2006: reserving 4 x ITEM-WEBCAM
2026-10-26 12:59:04.931 [producer pid=31564] Done: 2 message(s) published to queue 'orders'
2026-10-26 12:59:05.899 [worker-2 pid=30372] DONE ORD-2005 processed -> ACK sent
2026-10-26 12:59:05.946 [worker-1 pid=33744] DONE ORD-2006 processed -> ACK sent
```

What this shows:

1. **The first worker receives the message:** worker-1 logs `RECEIVED ... order_id=ORD-2001 ... redelivered=False` and starts processing.
2. **The message is not acknowledged:** worker-1 logs `SIMULATED CRASH (before-commit): process exits now, ACK never sent`.
3. **Another worker receives it again:** 46 ms later worker-2 logs `RECEIVED ... order_id=ORD-2001 ... redelivered=True`, processes it and ACKs it.
4. **The system keeps running:** worker-2 carries on with ORD-2003 and ORD-2004. worker-1 is then restarted and both workers share ORD-2005 and ORD-2006, with no broker or producer restart.

The broker's view after the crash and after the restart, from [`evidence/02-failure-redelivery/queue-state.txt`](evidence/02-failure-redelivery/queue-state.txt):

```
## after worker-1 crashed: worker-2 is still consuming
$ rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers
name	messages_ready	messages_unacknowledged	consumers
orders	0	0	1
$ rabbitmqctl list_consumers queue_name consumer_tag ack_required prefetch_count
queue_name	consumer_tag	ack_required	prefetch_count
orders	worker-2	true	1

## after worker-1 restarted and 2 more orders were processed
$ rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers
name	messages_ready	messages_unacknowledged	consumers
orders	0	0	2
$ rabbitmqctl list_consumers queue_name consumer_tag ack_required prefetch_count
queue_name	consumer_tag	ack_required	prefetch_count
orders	worker-1	true	1
orders	worker-2	true	1
```

No messages were lost: 0 ready and 0 unacknowledged once worker-2 finished. The consumer count went 2 → 1 → 2.

## 7. Evidence: duplicate handling

Every worker checks `data/processed_events.txt` before applying an order, and records the `event_id` right after applying it. Both steps run under a file lock shared by all workers. Two kinds of duplicates were tested.

### 7.1 Producer re-sends the same event

`producer.py --duplicate` publishes ORD-3001 again 2 seconds later with the **same `event_id`**, as a producer would after a lost publisher confirm. The worker recognises the `event_id` and skips the business action. From [`evidence/03-duplicate-event/timeline.log`](evidence/03-duplicate-event/timeline.log):

```
2026-10-26 12:59:08.668 [worker-1 pid=22440] Connected; consuming from queue 'orders' (prefetch=1, manual ACK)
2026-10-26 12:59:09.196 [worker-2 pid=10404] Connected; consuming from queue 'orders' (prefetch=1, manual ACK)
2026-10-26 12:59:09.533 [worker-1 pid=22440] RECEIVED event_id=76cbf08f-5adf-4a44-8cc7-7ce63f369dbb order_id=ORD-3001 item_id=ITEM-MONITOR quantity=2 redelivered=False
2026-10-26 12:59:09.534 [producer pid=33668] PUBLISHED event_id=76cbf08f-5adf-4a44-8cc7-7ce63f369dbb order_id=ORD-3001 item_id=ITEM-MONITOR quantity=2
2026-10-26 12:59:09.540 [worker-1 pid=22440] PROCESSING ORD-3001: reserving 2 x ITEM-MONITOR
2026-10-26 12:59:09.580 [worker-2 pid=10404] RECEIVED event_id=ad00c555-9c54-4cc5-ad3f-a2b92618b541 order_id=ORD-3002 item_id=ITEM-MOUSE quantity=4 redelivered=False
2026-10-26 12:59:09.581 [producer pid=33668] PUBLISHED event_id=ad00c555-9c54-4cc5-ad3f-a2b92618b541 order_id=ORD-3002 item_id=ITEM-MOUSE quantity=4
2026-10-26 12:59:09.581 [worker-2 pid=10404] PROCESSING ORD-3002: reserving 4 x ITEM-MOUSE
2026-10-26 12:59:09.628 [producer pid=33668] PUBLISHED event_id=09d584ac-3ad2-4da7-8a3c-0c4196c11702 order_id=ORD-3003 item_id=ITEM-MOUSE quantity=4
2026-10-26 12:59:10.549 [worker-1 pid=22440] DONE ORD-3001 processed -> ACK sent
2026-10-26 12:59:10.551 [worker-1 pid=22440] RECEIVED event_id=09d584ac-3ad2-4da7-8a3c-0c4196c11702 order_id=ORD-3003 item_id=ITEM-MOUSE quantity=4 redelivered=False
2026-10-26 12:59:10.558 [worker-1 pid=22440] PROCESSING ORD-3003: reserving 4 x ITEM-MOUSE
2026-10-26 12:59:10.585 [worker-2 pid=10404] DONE ORD-3002 processed -> ACK sent
2026-10-26 12:59:11.583 [worker-1 pid=22440] DONE ORD-3003 processed -> ACK sent
2026-10-26 12:59:11.674 [producer pid=33668] PUBLISHED AGAIN event_id=76cbf08f-5adf-4a44-8cc7-7ce63f369dbb order_id=ORD-3001 (simulated producer retry: same event_id -> duplicate)
2026-10-26 12:59:11.674 [worker-1 pid=22440] RECEIVED event_id=76cbf08f-5adf-4a44-8cc7-7ce63f369dbb order_id=ORD-3001 item_id=ITEM-MONITOR quantity=2 redelivered=False
2026-10-26 12:59:11.678 [producer pid=33668] Done: 4 message(s) published to queue 'orders'
2026-10-26 12:59:11.684 [worker-1 pid=22440] DUPLICATE event_id=76cbf08f-5adf-4a44-8cc7-7ce63f369dbb (ORD-3001) was already processed -> business action skipped, ACK sent
```

### 7.2 Worker crashes after processing but before the ACK

This is the case that makes idempotency necessary. worker-1 runs with `--crash-stage after-commit`: it **reserves the stock for ORD-4001 and records the event_id, then dies before the ACK**. RabbitMQ cannot tell that the work was done, so it redelivers the message to worker-2. Without the processed-events check, worker-2 would reserve the stock a second time. From [`evidence/04-crash-after-processing/timeline.log`](evidence/04-crash-after-processing/timeline.log):

```
2026-10-26 12:59:12.248 [worker-1 pid=22460] Connected; consuming from queue 'orders' (prefetch=1, manual ACK), will crash on message 1 (after-commit)
2026-10-26 12:59:12.887 [worker-2 pid=17320] Connected; consuming from queue 'orders' (prefetch=1, manual ACK)
2026-10-26 12:59:13.331 [worker-1 pid=22460] RECEIVED event_id=a7b4b862-6af8-4422-864d-40b5f6e4e5e8 order_id=ORD-4001 item_id=ITEM-MOUSE quantity=5 redelivered=False
2026-10-26 12:59:13.332 [producer pid=35628] PUBLISHED event_id=a7b4b862-6af8-4422-864d-40b5f6e4e5e8 order_id=ORD-4001 item_id=ITEM-MOUSE quantity=5
2026-10-26 12:59:13.332 [worker-1 pid=22460] PROCESSING ORD-4001: reserving 5 x ITEM-MOUSE
2026-10-26 12:59:13.380 [worker-2 pid=17320] RECEIVED event_id=e8031968-7f36-46bd-b3f7-4da1f13911e5 order_id=ORD-4002 item_id=ITEM-KEYBOARD quantity=5 redelivered=False
2026-10-26 12:59:13.381 [producer pid=35628] PUBLISHED event_id=e8031968-7f36-46bd-b3f7-4da1f13911e5 order_id=ORD-4002 item_id=ITEM-KEYBOARD quantity=5
2026-10-26 12:59:13.381 [worker-2 pid=17320] PROCESSING ORD-4002: reserving 5 x ITEM-KEYBOARD
2026-10-26 12:59:13.387 [producer pid=35628] Done: 2 message(s) published to queue 'orders'
2026-10-26 12:59:14.336 [worker-1 pid=22460] SIMULATED CRASH (after-commit): process exits now, ACK never sent
2026-10-26 12:59:14.397 [worker-2 pid=17320] DONE ORD-4002 processed -> ACK sent
2026-10-26 12:59:14.398 [worker-2 pid=17320] RECEIVED event_id=a7b4b862-6af8-4422-864d-40b5f6e4e5e8 order_id=ORD-4001 item_id=ITEM-MOUSE quantity=5 redelivered=True
2026-10-26 12:59:14.404 [worker-2 pid=17320] DUPLICATE event_id=a7b4b862-6af8-4422-864d-40b5f6e4e5e8 (ORD-4001) was already processed -> business action skipped, ACK sent
```

### 7.3 Final check: every order applied exactly once

Across all four scenarios, 17 distinct orders were published as 18 messages (one was re-sent). The broker made 20 deliveries, including 2 redeliveries. The inventory ledger still holds exactly one reservation per order. From [`evidence/05-final-check/summary.txt`](evidence/05-final-check/summary.txt), and from [`queue-stats.txt`](evidence/05-final-check/queue-stats.txt), which was read from the RabbitMQ management API:

```
Orders published (distinct event_ids): 17
Messages published incl. the re-sent duplicate: 18
Reservations in the ledger: 17
Distinct event_ids in the ledger: 17
Event_ids recorded as processed: 17
Duplicates detected and skipped: 2
Redeliveries received (redelivered=True): 2
```

```
Queue orders (from the management API)
  publish    18
  deliver    20
  redeliver  2
  ack        18
  messages still in queue: 0
```

`deliver` is 20 but `ack` is 18. The two missing ACKs are the two crashed deliveries, and each of those messages was delivered and ACKed a second time. The full ledger is in [`evidence/05-final-check/inventory_reservations.log`](evidence/05-final-check/inventory_reservations.log), and the processed `event_id` set is in [`evidence/05-final-check/processed_events.txt`](evidence/05-final-check/processed_events.txt).

## 8. Reflection

**Why can a message be delivered more than once, and how does idempotent handling help?**

RabbitMQ with manual ACKs gives *at-least-once* delivery: a message stays on the queue until a consumer acknowledges it, so if a worker crashes, loses its connection, or finishes the work but dies before its ACK reaches the broker, the message is requeued and delivered again. Duplicates can also start at the producer, which may re-publish an event when it is unsure the first publish arrived. Because the broker cannot tell "never processed" apart from "processed but not acknowledged", the consumer has to make processing idempotent. Here each worker records the `event_id` of every applied order, and when it sees an `event_id` again it ACKs the message without applying it, so the redelivered ORD-4001 in scenario 4 did not reserve stock twice and at-least-once delivery still gave an exactly-once business result.

## 9. Requirements checklist

| Requirement | Where |
|---|---|
| Producer publishes `event_id`, `order_id`, `item_id`, `quantity`; multiple messages | [`producer.py`](producer.py), [§4](#4-evidence-successful-message-processing) |
| Worker receives, logs the order, ACKs after processing | [`worker.py`](worker.py), [§4](#4-evidence-successful-message-processing) |
| Producer and workers are independent processes | separate `python` processes (different pids in every log line) |
| Two workers on the same queue as competing consumers | [§5](#5-evidence-two-competing-consumers) |
| Worker stops before ACK → another worker receives the message again; system keeps running | [§6](#6-evidence-failure-and-redelivery) |
| Track processed `event_id`s; skip the business action and log the duplicate | [`idempotency.py`](idempotency.py), [§7](#7-evidence-duplicate-handling) |
| Run instructions, architecture diagram, screenshots and logs as evidence, reflection | [§3](#3-how-to-run), [§1](#1-architecture), [screenshots](#screenshots-rabbitmq-management-ui), [`evidence/`](evidence/), [§8](#8-reflection) |

**Limitations (fine for a lab, not for production):** the ledger write and the `event_id` record are two appends under one lock. A real service would put both in a single database transaction with a unique constraint on `event_id`, and would expire old IDs instead of keeping the file forever.
