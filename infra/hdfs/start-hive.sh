#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/docker-compose.yml"

podman compose --profile hive -f "$COMPOSE_FILE" up -d hive-metastore hiveserver2

health="starting"
for _ in $(seq 1 60); do
  health="$(podman inspect --format '{{.State.Health.Status}}' hdfs_hiveserver2_1 2>/dev/null || true)"
  [[ "$health" == "healthy" ]] && break
  sleep 2
done

if [[ "$health" != "healthy" ]]; then
  podman compose --profile hive -f "$COMPOSE_FILE" logs --tail 80 hiveserver2 hive-metastore
  echo "HiveServer2 did not become healthy." >&2
  exit 1
fi

podman compose --profile hive -f "$COMPOSE_FILE" exec -T hiveserver2 \
  /opt/hive/bin/beeline \
  -u 'jdbc:hive2://localhost:10000/default' \
  -f /tmp/warehouse_tables.sql

if podman compose -f "$COMPOSE_FILE" exec -T namenode \
    hdfs dfs -test -d /warehouse/bronze/transactions \
  && podman compose -f "$COMPOSE_FILE" exec -T namenode \
    hdfs dfs -test -d /warehouse/silver/transactions; then
  podman compose --profile hive -f "$COMPOSE_FILE" exec -T hiveserver2 \
    /opt/hive/bin/beeline \
    -u 'jdbc:hive2://localhost:10000/default' \
    -f /tmp/refresh_partitions.sql
fi

output="$(
  podman compose --profile hive -f "$COMPOSE_FILE" exec -T hiveserver2 \
    /opt/hive/bin/beeline \
    -u 'jdbc:hive2://localhost:10000/realtime_warehouse' \
    -e 'SHOW TABLES;' 2>/dev/null
)"

for table in \
  bronze_transactions \
  silver_transactions \
  silver_quarantine \
  gold_daily_transaction_metrics \
  gold_enriched_transactions
do
  if ! grep -q "$table" <<<"$output"; then
    echo "Hive validation failed: missing table $table" >&2
    exit 1
  fi
done

echo "Hive Metastore and HiveServer2 started."
echo "Hive UI:  http://localhost:10002"
