# PostgreSQL, Kafka, Spark, HDFS, HiveQL, and Superset

PostgreSQL is the operational source selected for this project instead of SQL Server. Spark incrementally loads transactions into HDFS Bronze, deduplicates Bronze into Silver, then calculates daily Gold metrics as Parquet. Kafka carries the live transaction stream to the Spark Structured Streaming enrichment job, which writes enriched events to Kafka and HDFS. Apache Hive provides the Metastore, HiveServer2, and HiveQL tables over those HDFS layers. Superset uses the Gold PostgreSQL table for dashboards.

## Start the lightweight platform (recommended for 8 GB RAM)

From the repository root:

```bash
podman compose -f infra/hdfs/docker-compose.yml up -d --build \
  postgres broker namenode datanode1 \
  spark-master spark-worker-a
```

The same core can be started with the convenience script:

```bash
bash infra/hdfs/start-lightweight.sh
```

The default profile uses one DataNode with replication factor 1 and one Spark worker. This is intentional for an 8 GB machine.
The development HDFS configuration also allows the single-node NameNode to leave safe mode without waiting for replicas from unavailable nodes.

Hive and the optional reporting UIs can be started separately:

```bash
podman compose --profile reporting -f infra/hdfs/docker-compose.yml up -d --build \
  hive-metastore hiveserver2 hive-warehouse-init kafka-ui superset
```

For the lightweight dashboard demonstration, start Streamlit instead of
Superset:

```bash
podman compose --profile dashboard -f infra/hdfs/docker-compose.yml \
  up -d --build streamlit
```

Open [http://localhost:8501](http://localhost:8501). The dashboard reads
`gold.daily_transaction_metrics` from PostgreSQL and displays transaction
counts, amounts, daily trends, currencies, statuses, and the Gold dataset.
Streamlit is the recommended dashboard for an 8 GB machine; Superset remains
available under the `reporting` profile when a full BI interface is required.

Hive Metastore listens on port `9083`; HiveServer2 accepts Beeline connections on `localhost:10000` and its web UI is at [http://localhost:10002](http://localhost:10002).

For the full teaching cluster with three DataNodes, YARN, HistoryServer and a second Spark worker, use `--profile full`; do not run it simultaneously with the lightweight profile on an 8 GB machine.

PostgreSQL initializes with local development credentials `warehouse` / `warehouse_dev`, database `realtime_warehouse`.

## Generate and load transactions

Start the optional demo producer and incremental loader:

```bash
podman compose --profile demo --profile streaming \
  -f infra/hdfs/docker-compose.yml up -d --build \
  kafka-init transaction-producer streaming-enricher
```

The producer writes generated transactions to PostgreSQL and Kafka. The loader polls PostgreSQL every minute, appends new rows to Bronze, and rebuilds Silver and Gold. Stop the producer and loader when you have enough sample data:

```bash
podman compose --profile demo -f infra/hdfs/docker-compose.yml stop \
  transaction-producer streaming-enricher
```

To run the PostgreSQL → Bronze/Silver/Gold batch loader on its own (recommended
on an 8 GB machine), use:

```bash
podman compose --profile batch -f infra/hdfs/docker-compose.yml up -d \
  warehouse-loader
```

Alternatively, run one Spark load manually after PostgreSQL has source rows:

```bash
podman compose -f infra/hdfs/docker-compose.yml exec spark-master \
  /opt/spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.5 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --master spark://spark-master:7077 \
  /app/postgres_to_hdfs.py
```

On first run Spark loads all source rows. Later runs use an HDFS timestamp and transaction ID watermark to append only newer rows. Silver deduplicates by transaction ID, and Silver/Gold are rewritten as compact Parquet snapshots.

## Register and query the HDFS data with HiveQL

The startup service `hive-warehouse-init` creates external Hive tables for the Bronze, Silver, and Gold Parquet paths. After Spark has written Bronze and Silver partitions, register those new partitions in the Metastore:

```bash
podman compose -f infra/hdfs/docker-compose.yml run --rm hive-partition-refresh
```

Connect to HiveServer2 with Beeline:

```bash
podman compose -f infra/hdfs/docker-compose.yml exec hiveserver2 \
  beeline -u 'jdbc:hive2://localhost:10000/realtime_warehouse'
```

  For the course demonstration, show the Metastore-backed external tables with:

  ```bash
  podman compose --profile hive -f infra/hdfs/docker-compose.yml \
    run --rm hive-partition-refresh
  podman compose --profile hive -f infra/hdfs/docker-compose.yml \
    exec hiveserver2 beeline \
    -u 'jdbc:hive2://localhost:10000/realtime_warehouse' \
    -e 'SHOW TABLES; SELECT * FROM gold_daily_transaction_metrics LIMIT 5;'
  ```

  The lightweight Hive profile uses local execution for demonstration queries.
  Queries requiring distributed MapReduce aggregation should be run with the
  `full` profile, which provides the YARN services.

Then run HiveQL, for example:

```sql
SHOW TABLES;

SELECT transaction_date, currency, SUM(transaction_count) AS transactions,
       ROUND(SUM(gross_amount), 2) AS gross_amount
FROM gold_daily_transaction_metrics
GROUP BY transaction_date, currency
ORDER BY transaction_date DESC, currency;

SELECT status, COUNT(*) AS transaction_count
FROM silver_transactions
GROUP BY status
ORDER BY transaction_count DESC;
```

The tables are external: Hive stores their schemas and partition metadata in the Metastore while the Parquet data remains in HDFS.

The streaming enrichment output is available at:

```text
hdfs://namenode:8020/warehouse/gold/enriched_transactions
```

Its checkpoint is stored at `/checkpoints/enriched_transactions_hdfs`.

| Hive table | HDFS location |
| --- | --- |
| `realtime_warehouse.bronze_transactions` | `/warehouse/bronze/transactions` |
| `realtime_warehouse.silver_transactions` | `/warehouse/silver/transactions` |
| `realtime_warehouse.gold_daily_transaction_metrics` | `/warehouse/gold/daily_transaction_metrics` |
| `realtime_warehouse.gold_enriched_transactions` | `/warehouse/gold/enriched_transactions` |

For a single batch run, execute the partition refresh after Spark finishes. For the continuous demo profile, rerun the refresh command after stopping the loader to register the latest partitions.

## Open Superset

Open [http://localhost:8089](http://localhost:8089) and sign in with `admin` / `admin`. Add a PostgreSQL database in **Settings → Database Connections → + Database** with this SQLAlchemy URI:

```text
postgresql+psycopg2://warehouse:warehouse_dev@postgres:5432/realtime_warehouse
```

Select the `gold` schema and register `daily_transaction_metrics` as a dataset. Create charts for transaction count and gross amount by `transaction_date`, then add them to a dashboard. Superset metadata persists in the `superset_home` volume.

## Useful checks

```bash
podman compose -f infra/hdfs/docker-compose.yml exec postgres \
  psql -U warehouse -d realtime_warehouse \
  -c 'SELECT * FROM gold.daily_transaction_metrics ORDER BY transaction_date DESC LIMIT 10;'

podman compose -f infra/hdfs/docker-compose.yml exec namenode \
  hdfs dfs -ls -R /warehouse

podman compose -f infra/hdfs/docker-compose.yml exec hiveserver2 \
  beeline -u 'jdbc:hive2://localhost:10000/realtime_warehouse' \
  -e 'SHOW TABLES; SELECT COUNT(*) FROM gold_daily_transaction_metrics;'
```

`podman compose down` stops the services but keeps PostgreSQL, HDFS, Hive Metastore, and Superset volumes. `podman compose down -v` deletes those volumes and all stored project data.

The stack uses pinned Spark, Hadoop, Hive, PostgreSQL, and Superset versions for reproducible course demonstrations. Credentials and the Superset secret are intentionally local-development values; replace them before any non-local deployment.
