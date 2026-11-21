#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/docker-compose.yml"

podman compose --profile dashboard -f "$COMPOSE_FILE" up -d --build streamlit

echo "Streamlit dashboard: http://localhost:8501"
