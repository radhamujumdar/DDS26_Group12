#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
MODE="${TX_MODE:-${1:-2pc}}"

TX_MODE="$MODE" docker compose \
  --env-file "$ROOT_DIR/.env.medium" \
  -f "$ROOT_DIR/docker-compose.yml" \
  up -d --build --remove-orphans \
  --scale order-service=4 \
  --scale payment-service=3 \
  --scale stock-service=3
