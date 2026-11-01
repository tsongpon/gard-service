#!/usr/bin/env bash
# Sends a device-register command to the Kafka topic consumed by gard-service.
#
# Usage:
#   ./scripts/send-device-command.sh --imei 356938035643809
#   ./scripts/send-device-command.sh --imei 356938035643809 --reference my-app-ref-123
#
# The appReferenceId is the idempotency key: re-sending the same reference is treated
# as a replay and skipped. Messages are keyed by imei so commands for the same imei
# serialize on one partition.
#
# Requires the docker compose stack to be running (docker compose up -d).
# Override target via env vars: KAFKA_CONTAINER, BOOTSTRAP_SERVER, TOPIC.

set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-gard-kafka}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:9092}"
TOPIC="${TOPIC:-device-commands}"
CONSOLE_PRODUCER="/opt/kafka/bin/kafka-console-producer.sh"

usage() {
  cat <<EOF
Sends a device-register command to Kafka topic '$TOPIC'.

Usage:
  $(basename "$0") --imei <imei> [--reference <appReferenceId>]

Options:
  -i, --imei      Device IMEI (required)
  -r, --reference App reference id, the idempotency key (default: generated UUID)
  -h, --help      Show this help

Environment overrides:
  KAFKA_CONTAINER  Kafka container name (default: $KAFKA_CONTAINER)
  BOOTSTRAP_SERVER Kafka bootstrap server (default: $BOOTSTRAP_SERVER)
  TOPIC            Target topic (default: $TOPIC)
EOF
  exit "${1:-0}"
}

IMEI="" REFERENCE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -i|--imei)       IMEI="$2"; shift 2 ;;
    -r|--reference)  REFERENCE="$2"; shift 2 ;;
    -h|--help)       usage 0 ;;
    *)               echo "Unknown option: $1" >&2; usage 1 ;;
  esac
done

[[ -n "$IMEI" ]] || usage 1
if [[ -z "$REFERENCE" ]]; then
  REFERENCE="$(uuidgen | tr 'A-Z' 'a-z')"
fi

if ! docker ps --format '{{.Names}}' | grep -qx "$KAFKA_CONTAINER"; then
  echo "Kafka container '$KAFKA_CONTAINER' is not running. Start the stack first: docker compose up -d" >&2
  exit 1
fi

escape_json() {
  local s=$1
  s=${s//\\/\\\\}
  s=${s//\"/\\\"}
  printf '%s' "$s"
}

PAYLOAD="{\"imei\":\"$(escape_json "$IMEI")\",\"appReferenceId\":\"$(escape_json "$REFERENCE")\"}"

docker exec -i "$KAFKA_CONTAINER" "$CONSOLE_PRODUCER" \
  --bootstrap-server "$BOOTSTRAP_SERVER" --topic "$TOPIC" \
  --property parse.key=true --property key.separator='|' <<< "$IMEI|$PAYLOAD"

echo "Sent device-register command to topic '$TOPIC' (key: $IMEI): $PAYLOAD"