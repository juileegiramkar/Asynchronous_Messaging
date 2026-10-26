#!/usr/bin/env bash
# Runs every scenario end to end and writes the logs quoted in the README to evidence/.
# Usage, from the repo root (Git Bash on Windows, or any POSIX shell):  bash scripts/demo.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -x .venv/Scripts/python.exe ]; then PY=.venv/Scripts/python.exe
elif [ -x .venv/bin/python ]; then PY=.venv/bin/python
else PY=python; fi

rmq() { docker compose exec -T rabbitmq "$@"; }

WORKER_PIDS=()
stop_workers() {
  for pid in "${WORKER_PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${WORKER_PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
  WORKER_PIDS=()
}
trap stop_workers EXIT

scenario() {  # scenario <dir> <title>
  DIR=evidence/$1
  mkdir -p "$DIR"
  echo
  echo "=== $1: $2 ==="
}

start_worker() {  # start_worker <name> [worker args...]; logs to $DIR/<name>.log
  local name=$1; shift
  "$PY" -u worker.py --name "$name" "$@" >>"$DIR/$name.log" 2>&1 &
  WORKER_PIDS+=($!)
  wait_for "$DIR/$name.log" "Connected; consuming" "$(($(count "Connected; consuming" "$DIR/$name.log") + 1))"
}

note() {  # note <file> <text>; a timestamped line, so it sorts into the timeline
  echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') [demo] $2" | tee -a "$1"
}

produce() {  # produce [producer args...]; logs to $DIR/producer.log
  "$PY" -u producer.py "$@" >>"$DIR/producer.log" 2>&1
}

count() {  # count <pattern> <files...>
  local pattern=$1; shift
  cat "$@" 2>/dev/null | grep -c -- "$pattern" || true
}

wait_for() {  # wait_for <file-or-glob> <pattern> <count>
  local files=$1 pattern=$2 want=$3 i
  for ((i = 0; i < 300; i++)); do
    # shellcheck disable=SC2086
    [ "$(count "$pattern" $files)" -ge "$want" ] && return 0
    sleep 0.1
  done
  echo "timed out waiting for $want x '$pattern' in $files" >&2
  return 1
}

queue_state() {  # queue_state <label>; appends live queue state to $DIR/queue-state.txt
  {
    echo "## $1"
    echo "\$ rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers"
    rmq rabbitmqctl list_queues -q name messages_ready messages_unacknowledged consumers
    echo "\$ rabbitmqctl list_consumers queue_name consumer_tag ack_required prefetch_count"
    rmq rabbitmqctl list_consumers -q queue_name consumer_tag ack_required prefetch_count
    echo
  } >>"$DIR/queue-state.txt"
}

finish_scenario() {
  stop_workers
  # All processes log "YYYY-MM-DD HH:MM:SS.mmm [...]", so a stable sort merges them into one timeline.
  sort -s -k1,2 "$DIR"/producer.log "$DIR"/worker-*.log >"$DIR/timeline.log"
  cat "$DIR/timeline.log"
}

echo "Starting RabbitMQ (docker compose up -d --wait)..."
docker compose up -d --wait
rmq rabbitmqctl delete_queue -q orders >/dev/null 2>&1 || true  # fresh queue and fresh stats
rm -rf evidence data
mkdir -p evidence

scenario 01-competing-consumers "successful processing by two competing workers"
start_worker worker-1
start_worker worker-2
queue_state "two workers connected, before publishing"
produce --count 6 --start-order 1001
wait_for "$DIR/worker-*.log" "ACK sent" 6
queue_state "after all 6 orders were processed"
finish_scenario
for w in worker-1 worker-2; do
  echo "$w processed: $(grep -o 'DONE ORD-[0-9]*' "$DIR/$w.log" | cut -d' ' -f2 | tr '\n' ' ')"
done | tee "$DIR/summary.txt"

scenario 02-failure-redelivery "worker-1 dies before ACK, worker-2 gets the message again"
start_worker worker-1 --crash-on-message 1 --crash-stage before-commit
start_worker worker-2
produce --count 4 --start-order 2001
wait_for "$DIR/worker-1.log" "SIMULATED CRASH" 1
wait_for "$DIR/worker-2.log" "redelivered=True" 1
wait_for "$DIR/worker-2.log" "ACK sent" 4
queue_state "after worker-1 crashed: worker-2 is still consuming"
note "$DIR/worker-1.log" "--- restarting worker-1, then publishing 2 more orders ---"
start_worker worker-1
produce --count 2 --start-order 2005
wait_for "$DIR/worker-*.log" "ACK sent" 6
queue_state "after worker-1 restarted and 2 more orders were processed"
finish_scenario

scenario 03-duplicate-event "producer re-sends an event with the same event_id"
start_worker worker-1
start_worker worker-2
produce --count 3 --start-order 3001 --duplicate
wait_for "$DIR/worker-*.log" "DUPLICATE" 1
wait_for "$DIR/worker-*.log" "DONE" 3
finish_scenario

scenario 04-crash-after-processing "worker-1 applies the order then dies before ACK; the redelivery is a duplicate"
start_worker worker-1 --crash-on-message 1 --crash-stage after-commit
start_worker worker-2
produce --count 2 --start-order 4001
wait_for "$DIR/worker-1.log" "SIMULATED CRASH" 1
wait_for "$DIR/worker-2.log" "DUPLICATE" 1
wait_for "$DIR/worker-2.log" "DONE" 1
finish_scenario

scenario 05-final-check "every order was applied exactly once"
cp data/processed_events.txt data/inventory_reservations.log "$DIR/"
sleep 6  # management stats refresh every 5 seconds
curl -s -u guest:guest "http://localhost:15672/api/queues/%2F/orders" | "$PY" -c "
import json, sys
q = json.load(sys.stdin)
s = q.get('message_stats', {})
print('Queue', q['name'], '(from the management API)')
for key in ('publish', 'deliver', 'redeliver', 'ack'):
    print(f'  {key:<10} {s.get(key, 0)}')
print('  messages still in queue:', q['messages'])
" >"$DIR/queue-stats.txt"
{
  echo "Orders published (distinct event_ids): $(cat evidence/0*/producer.log | grep -o 'PUBLISHED event_id=[^ ]*' | sort -u | wc -l)"
  echo "Messages published incl. the re-sent duplicate: $(count 'PUBLISHED' evidence/0*/producer.log)"
  echo "Reservations in the ledger: $(wc -l <data/inventory_reservations.log)"
  echo "Distinct event_ids in the ledger: $(grep -o 'event_id=[^ ]*' data/inventory_reservations.log | sort -u | wc -l)"
  echo "Event_ids recorded as processed: $(wc -l <data/processed_events.txt)"
  echo "Duplicates detected and skipped: $(count 'DUPLICATE' evidence/0*/worker-*.log)"
  echo "Redeliveries received (redelivered=True): $(count 'redelivered=True' evidence/0*/worker-*.log)"
} >"$DIR/summary.txt"
cat "$DIR/queue-stats.txt" "$DIR/summary.txt"
