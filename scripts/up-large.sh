#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker compose \
  --project-name dds-large \
  -f "$ROOT_DIR/docker-compose.large.yml" \
  up -d --build \
  --scale order-checkout-worker=4 \
  --scale stock-activity-worker=3 \
  --scale payment-activity-worker=3
