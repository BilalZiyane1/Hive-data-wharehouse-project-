#!/usr/bin/env python3
"""Incremental PostgreSQL -> HDFS Bronze/Silver/Gold pipeline."""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum as spark_sum
from pyspark.sql.functions import to_date
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, when


POSTGRES_URL = os.getenv(
    "POSTGRES_JDBC_URL",
    "jdbc:postgresql://postgres:5432/realtime_warehouse",
)
POSTGRES_USER = os.getenv("POSTGRES_USER", "warehouse")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "warehouse_dev")
WAREHOUSE_ROOT = os.getenv("WAREHOUSE_ROOT", "hdfs://namenode:8020/warehouse")
BRONZE_PATH = f"{WAREHOUSE_ROOT}/bronze/transactions"
SILVER_PATH = f"{WAREHOUSE_ROOT}/silver/transactions"
GOLD_PATH = f"{WAREHOUSE_ROOT}/gold/daily_transaction_metrics"
QUARANTINE_PATH = f"{WAREHOUSE_ROOT}/silver/quarantine"
WATERMARK_PATH = f"{WAREHOUSE_ROOT}/control/postgres_transactions_watermark.txt"


spark = (
    SparkSession.builder
    .appName("PostgreSQLIncrementalWarehouseLoad")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def read_watermark():
    path = spark._jvm.org.apache.hadoop.fs.Path(WATERMARK_PATH)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return None
    stream = fs.open(path)
    try:
        reader = spark._jvm.java.io.BufferedReader(
            spark._jvm.java.io.InputStreamReader(stream)
        )
        timestamp = reader.readLine()
        transaction_id = reader.readLine()
        return (timestamp, transaction_id) if timestamp and transaction_id else None
    finally:
        stream.close()


def write_watermark(value):
    path = spark._jvm.org.apache.hadoop.fs.Path(WATERMARK_PATH)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    parent = path.getParent()
    fs.mkdirs(parent)
    temporary = spark._jvm.org.apache.hadoop.fs.Path(WATERMARK_PATH + ".tmp")
    stream = fs.create(temporary, True)
    try:
        stream.writeBytes("\n".join(value))
        stream.hflush()
    finally:
        stream.close()
    if fs.exists(path):
        fs.delete(path, False)
    if not fs.rename(temporary, path):
        raise RuntimeError(f"Could not move watermark into place: {path}")


previous_watermark = read_watermark()
predicate = (
    "TRUE"
    if previous_watermark is None
    else (
        "(created_at, transaction_id) > "
        f"(TIMESTAMPTZ '{previous_watermark[0]}', "
        f"'{previous_watermark[1]}'::uuid)"
    )
)
source_query = (
    "(SELECT * FROM public.transactions "
    f"WHERE {predicate}) AS incremental_transactions"
)

increment = (
    spark.read.format("jdbc")
    .option("url", POSTGRES_URL)
    .option("dbtable", source_query)
    .option("user", POSTGRES_USER)
    .option("password", POSTGRES_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
)

increment_count = increment.count()
if increment_count:
    max_row = (
        increment.orderBy(col("created_at").desc(), col("transaction_id").desc())
        .select("created_at", "transaction_id")
        .first()
    )
    max_watermark = (
        max_row["created_at"].strftime("%Y-%m-%d %H:%M:%S.%f"),
        str(max_row["transaction_id"]),
    )
    bronze_batch = increment.withColumn("created_date", to_date("created_at"))
    (
        bronze_batch.write
        .mode("append")
        .partitionBy("created_date")
        .parquet(BRONZE_PATH)
    )
    # Advance only after the Bronze write succeeds. Reprocessing after a crash is
    # safe because Silver removes duplicate transaction IDs.
    write_watermark(max_watermark)
    print(f"Appended {increment_count} new rows to Bronze")
else:
    print("No new PostgreSQL transactions to ingest")


bronze_path = spark._jvm.org.apache.hadoop.fs.Path(BRONZE_PATH)
bronze_fs = bronze_path.getFileSystem(spark._jsc.hadoopConfiguration())
if not bronze_fs.exists(bronze_path) and increment_count == 0:
    print("Bronze is empty; run the producer and retry the load")
    spark.stop()
    raise SystemExit(0)
bronze = spark.read.parquet(BRONZE_PATH)

dedupe_window = Window.partitionBy("transaction_id").orderBy(
    col("created_at").desc(),
    col("producer_timestamp_ms").desc(),
    col("transaction_id").desc(),
)
silver = (
    bronze.withColumn("_row_number", row_number().over(dedupe_window))
    .filter(col("_row_number") == 1)
    .drop("_row_number")
)

valid_statuses = ["completed", "pending", "failed", "cancelled", "processing"]
valid_types = [
    "purchase", "refund", "subscription", "withdrawal",
    "deposit", "transfer", "payment", "chargeback",
]
classified = silver.withColumn(
    "_rejection_reason",
    when(col("transaction_id").isNull() | col("customer_id").isNull()
         | col("payment_method_id").isNull(), "missing_required_field")
    .when(col("amount") < 0, "negative_amount")
    .when(~col("status").isin(valid_statuses), "invalid_status")
    .when(~col("transaction_type").isin(valid_types), "invalid_transaction_type")
    .when(col("producer_timestamp_ms") <= 0, "invalid_timestamp")
)

rejected = classified.filter(col("_rejection_reason").isNotNull())
valid = classified.filter(col("_rejection_reason").isNull())

rejection_report = {
    row["_rejection_reason"]: row["count"]
    for row in rejected.groupBy("_rejection_reason").count().collect()
}

valid.write.mode("overwrite").partitionBy("created_date").parquet(SILVER_PATH)
rejected.write.mode("overwrite").parquet(QUARANTINE_PATH)

silver = valid.drop("_rejection_reason")

gold = (
    silver.withColumn("transaction_date", to_date("created_at"))
    .groupBy("transaction_date", "currency", "status", "transaction_type")
    .agg(
        count("transaction_id").alias("transaction_count"),
        spark_sum("amount").alias("gross_amount"),
        avg("amount").alias("average_amount"),
    )
)
gold.write.mode("overwrite").parquet(GOLD_PATH)

(
    gold.write.format("jdbc")
    .option("url", POSTGRES_URL)
    .option("dbtable", "gold.daily_transaction_metrics")
    .option("user", POSTGRES_USER)
    .option("password", POSTGRES_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .option("truncate", "true")
    .mode("overwrite")
    .save()
)

print(f"Silver snapshot: {SILVER_PATH}")
print(f"Gold metrics: {GOLD_PATH} and PostgreSQL gold.daily_transaction_metrics")
print(f"Quarantine: {QUARANTINE_PATH}")
print(f"Rejection counts: {rejection_report}")
spark.stop()
