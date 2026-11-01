#!/usr/bin/env bash
# Starts the full stack, always rebuilding the app image first so the latest code is deployed.
#
# Usage:
#   ./scripts/up.sh            # build app image + start db, kafka, app
#   ./scripts/up.sh <args>     # extra args are passed to 'docker compose up'
#                             # e.g. ./scripts/up.sh app

set -euo pipefail

cd "$(dirname "$0")/.."

exec docker compose up --build "$@"