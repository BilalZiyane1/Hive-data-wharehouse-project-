#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/docker-compose.yml"

podman compose -f "$COMPOSE_FILE" up -d --build \
  postgres broker namenode datanode1 spark-master spark-worker-a

echo "Core services started."
echo "NameNode: http://localhost:9870"
echo "Spark:    http://localhost:8086"
