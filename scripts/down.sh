#!/usr/bin/env bash
# Stops the stack and removes its containers and network (keeps DB data).
#
# Usage:
#   ./scripts/down.sh            # stop and remove containers
#   ./scripts/down.sh -v         # also remove volumes (wipes MariaDB data)
#   ./scripts/down.sh <args>     # extra args are passed to 'docker compose down'

set -euo pipefail

cd "$(dirname "$0")/.."

exec docker compose down "$@"