#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
MODE="${TX_MODE:-${1:-2pc}}"

TX_MODE="$MODE" docker compose \
  --env-file "$ROOT_DIR/.env.small" \
  -f "$ROOT_DIR/docker-compose.yml" \
  up -d --build --remove-orphans \
  --scale order-service=1 \
  --scale payment-service=1 \
  --scale stock-service=1
