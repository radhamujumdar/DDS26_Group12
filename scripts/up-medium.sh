#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker compose \
  --project-name dds-medium \
  -f "$ROOT_DIR/docker-compose.medium.yml" \
  up -d --build \
  --scale order-service=2 \
  --scale stock-service=2 \
  --scale payment-service=2 \
  --scale fluxi-server=2 \
  --scale fluxi-scheduler=2 \
  --scale order-checkout-worker=3 \
  --scale stock-activity-worker=2 \
  --scale payment-activity-worker=2
