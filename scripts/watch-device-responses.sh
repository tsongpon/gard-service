#!/usr/bin/env bash
# Tails device response messages from the Kafka response topic (Ctrl+C to stop).
#
# Usage:
#   ./scripts/watch-device-responses.sh                    # stream new messages as they arrive
#   ./scripts/watch-device-responses.sh --from-beginning   # also replay messages already on the topic
#   ./scripts/watch-device-responses.sh --max-messages 3   # exit after 3 messages
#
# Requires the docker compose stack to be running (./scripts/up.sh).
# Environment overrides: KAFKA_CONTAINER, BOOTSTRAP_SERVER, TOPIC.

set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-gard-kafka}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:9092}"
TOPIC="${TOPIC:-device-responses}"
CONSOLE_CONSUMER="/opt/kafka/bin/kafka-console-consumer.sh"

if ! docker ps --format '{{.Names}}' | grep -qx "$KAFKA_CONTAINER"; then
  echo "Kafka container '$KAFKA_CONTAINER' is not running. Start the stack first: ./scripts/up.sh" >&2
  exit 1
fi

TTY_FLAG=""
if [[ -t 0 ]]; then
  TTY_FLAG="-t"
fi

echo "Consuming topic '$TOPIC' (Ctrl+C to stop)..."
exec docker exec -i $TTY_FLAG "$KAFKA_CONTAINER" "$CONSOLE_CONSUMER" \
  --bootstrap-server "$BOOTSTRAP_SERVER" --topic "$TOPIC" "$@"