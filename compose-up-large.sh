#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
MODE="${TX_MODE:-${1:-2pc}}"

TX_MODE="$MODE" docker compose \
  -f "$ROOT_DIR/docker-compose.large.yml" \
  up -d --build --remove-orphans
