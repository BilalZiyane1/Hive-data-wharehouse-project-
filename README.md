# Real-Time Transaction Warehouse

A course project that combines PostgreSQL, Apache Kafka, Apache Spark, HDFS,
Apache Hive/HiveQL, and Streamlit for batch and streaming transaction analytics.
PostgreSQL is the operational source in place of SQL Server in the reference
architecture.

## Architecture

```text
PostgreSQL ──┬── Spark batch ──> HDFS Bronze / Silver / Gold ──> Hive / HiveQL
             └── Kafka ──> Spark Structured Streaming ──> HDFS enriched data
      PostgreSQL Gold metrics ──> Streamlit dashboard
```

The batch pipeline reads PostgreSQL incrementally, writes Parquet datasets to
HDFS, deduplicates valid records, sends invalid records to a quarantine path,
and refreshes daily Gold metrics. The streaming pipeline reads Kafka events,
enriches them with Spark Structured Streaming, and writes enriched output to
HDFS. Hive external tables expose the warehouse paths for SQL analysis.

## Project structure

```text
infra/hdfs/
  docker-compose.yml       Container services and profiles
  Makefile                 Operations for the data platform
  POSTGRES.md              PostgreSQL setup and data notes
  app/                     Spark jobs and Kafka producer
  dashboard/               Streamlit dashboard
  hive/                    Hive table definitions and demo queries
  postgres/init.sql        PostgreSQL schema and sample data
  conf/, hadoop/conf/       Hadoop and Spark configuration
  tests/                   Pipeline contract tests
report/
  report.tex               French project report source
  figures/                 Report diagrams and captured screens
Realtime_Warehousing_Projec .gif  Reference architecture
```

## Requirements

- Linux
- Podman and Podman Compose
- Internet access on first build to download container images and packages

Check the installation:

```bash
podman --version
podman compose version
```

All commands below run from the project root unless otherwise stated.

## Start the core services

Start PostgreSQL, Kafka, HDFS, and the Spark master and worker:

```bash
make light-up
podman compose -f infra/hdfs/docker-compose.yml ps
```

The main web interfaces are:

| Service | Address |
| --- | --- |
| HDFS NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8086 |
| Spark Worker | http://localhost:8081 |

## PostgreSQL and batch pipeline

Open a PostgreSQL prompt:

```bash
make -C infra/hdfs postgres-shell
```

Example SQL:

```sql
SELECT COUNT(*) FROM transactions;
SELECT * FROM transactions LIMIT 10;
SELECT * FROM gold.daily_transaction_metrics
ORDER BY transaction_date DESC LIMIT 10;
```

Run the continuous batch loader and inspect the warehouse paths:

```bash
make -C infra/hdfs batch
make -C infra/hdfs hdfs-list
```

The loader populates these HDFS locations:

```text
/warehouse/bronze/transactions
/warehouse/silver/transactions
/warehouse/silver/quarantine
/warehouse/gold/daily_transaction_metrics
```

Stop the loader when the batch demonstration is complete:

```bash
podman compose --profile batch -f infra/hdfs/docker-compose.yml \
  stop warehouse-loader
```

## Kafka streaming

Start topic initialization and the Spark streaming job:

```bash
make streaming
```

To also generate demo transactions, start the producer:

```bash
podman compose --profile demo -f infra/hdfs/docker-compose.yml \
  up -d transaction-producer
```

View the streaming output and Kafka topics:

```bash
podman compose --profile streaming -f infra/hdfs/docker-compose.yml \
  logs -f streaming-enricher
podman compose -f infra/hdfs/docker-compose.yml exec broker \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:29092 --list
```

Enriched records are written to `/warehouse/gold/enriched_transactions`.

## Hive and HiveQL

Start Hive after the core services are running:

```bash
make hive
podman compose --profile hive -f infra/hdfs/docker-compose.yml ps
```

Connect with Beeline:

```bash
podman compose --profile hive -f infra/hdfs/docker-compose.yml exec \
  hiveserver2 beeline -u 'jdbc:hive2://localhost:10000/realtime_warehouse'
```

At the Beeline prompt, demonstrate HiveQL against the external tables:

```sql
SHOW TABLES;
SELECT COUNT(*) FROM gold_daily_transaction_metrics;
SELECT currency, SUM(transaction_count) AS transactions
FROM gold_daily_transaction_metrics
GROUP BY currency
ORDER BY transactions DESC;
```

The table definitions are in `infra/hdfs/hive/warehouse_tables.sql`; example
queries are in `infra/hdfs/hive/demo_queries.sql`. To run those queries from the
project root:

```bash
make hive-demo
```

## Streamlit dashboard

Start the dashboard and open http://localhost:8501:

```bash
make dashboard
```

The dashboard reads live transaction data and Gold aggregates from PostgreSQL.
Superset is also available as an optional Compose service under the
`reporting` profile (`superset` at http://localhost:8089); it uses local demo
credentials defined in `infra/hdfs/docker-compose.yml`.

## Presentation order

For a clear demonstration, start with the core and then show each workload:

1. `make light-up` — show PostgreSQL, HDFS, and Spark services.
2. `make -C infra/hdfs postgres-shell` — query source transactions.
3. `make -C infra/hdfs batch` — load and inspect Bronze, Silver, and Gold.
4. `make streaming` — show Kafka topics, streaming logs, and enriched HDFS data.
5. `make hive` — run `SHOW TABLES` and aggregate HiveQL queries in Beeline.
6. `make dashboard` — show live source metrics and Gold charts.

Stop optional services when finished:

```bash
make stop-optional
podman compose -f infra/hdfs/docker-compose.yml down
```

`down` preserves named volumes. `down -v` deletes the project's PostgreSQL,
HDFS, Hive Metastore, Kafka, and Superset data; use it only when you intend to
reset the demo data.

## Checks

The repository includes source-level tests and Compose configuration checks:

```bash
make check
```

## Report

The French LaTeX report source is `report/report.tex`. Build it with:

```bash
make -C report
```

The build creates `Rapport_Bilal_Ziyane_Entrepot_Temps_Reel.pdf` in the project
root. Report screenshots and diagrams are in `report/figures/`.

## Local demo credentials

The Compose file contains development-only credentials for local demonstration.
Change them before exposing services outside the local machine. Never commit
personal access tokens, private keys, or machine-specific environment files.
