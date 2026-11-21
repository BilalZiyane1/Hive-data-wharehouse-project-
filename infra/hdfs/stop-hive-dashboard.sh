#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/docker-compose.yml"

podman compose --profile hive --profile dashboard -f "$COMPOSE_FILE" stop \
  hiveserver2 hive-metastore streamlit 2>/dev/null || true
