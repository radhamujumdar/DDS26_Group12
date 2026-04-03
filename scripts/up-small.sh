#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker compose \
  --project-name dds-small \
  -f "$ROOT_DIR/docker-compose.small.yml" \
  up -d --build
