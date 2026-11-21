
#!/usr/bin/env python3
"""
Kafka -> Spark Structured Streaming -> Kafka
Real-time Transaction Enrichment Pipeline

Features:
- Kafka JSON ingestion
- Customer and payment method enrichment
- Stream-static joins
- Transaction risk scoring
- Processing latency calculation
- Kafka output
- Persistent HDFS checkpoint
- 5-second micro-batch processing

Compatible with Spark 4.x when the Kafka connector
matches the installed Spark and Scala versions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col,
    from_json,
    to_json,
    struct,
    lit,
    current_timestamp,
    unix_millis,
    when,
    round as spark_round,
    broadcast
)



# ==================================================
# 1. CONFIGURATION
# ==================================================

KAFKA_BOOTSTRAP_SERVERS = "broker:29092"

INPUT_TOPIC = "transactions_topic"
OUTPUT_TOPIC = "enriched_transactions_topic"

CUSTOMERS_FILE = "/data/customers.csv"
PAYMENT_METHODS_FILE = "/data/payment_methods.csv"

CHECKPOINT_LOCATION = (
    "hdfs://namenode:8020/checkpoints/transactions_enrichment_v2"
)

PROCESSING_INTERVAL = "5 seconds"


# ==================================================
# 2. SPARK SESSION
# ==================================================

spark = (
    SparkSession.builder
    .appName("TransactionEnrichmentPipeline")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 65)
print("TRANSACTION ENRICHMENT PIPELINE")
print("=" * 65)

print("Spark version:", spark.version)
print("Kafka input:", INPUT_TOPIC)
print("Kafka output:", OUTPUT_TOPIC)
print("Checkpoint:", CHECKPOINT_LOCATION)


# ==================================================
# 3. TRANSACTION JSON SCHEMA
# ==================================================

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("payment_method_id", StringType(), True),
    StructField("country_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("producer_timestamp_ms", LongType(), True),
    StructField("transaction_reference", StringType(), True)
])


# ==================================================
# 4. LOAD CUSTOMER LOOKUP TABLE
# ==================================================

print("\nLoading customer lookup table...")

customers_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(CUSTOMERS_FILE)
    .select(
        col("customer_id").cast("string"),
        col("customer_name").cast("string"),
        col("customer_email").cast("string"),
        col("customer_tier").cast("string"),
        col("customer_region").cast("string"),
        col("customer_registration_date").cast("string")
    )
)

customers_count = customers_df.count()

print("Customers loaded:", customers_count)


# ==================================================
# 5. LOAD PAYMENT METHOD LOOKUP TABLE
# ==================================================

print("\nLoading payment methods...")

payment_methods_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(PAYMENT_METHODS_FILE)
    .select(
        col("payment_method_id").cast("string"),
        col("payment_method_name").cast("string"),
        col("payment_method_type").cast("string"),
        col("payment_method_category").cast("string"),
        col("payment_processing_fee_percent").cast("double")
    )
)

payment_count = payment_methods_df.count()

print("Payment methods loaded:", payment_count)


# Broadcast static lookup tables

broadcast_customers = broadcast(customers_df)
broadcast_payments = broadcast(payment_methods_df)


# ==================================================
# 6. READ KAFKA STREAM
# ==================================================

print("\nConnecting to Kafka...")

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

print("Kafka source configured successfully")


# ==================================================
# 7. PARSE JSON TRANSACTIONS
# ==================================================

parsed_stream = (
    kafka_stream
    .select(
        from_json(
            col("value").cast("string"),
            transaction_schema
        ).alias("data")
    )
    .select("data.*")
)

# Ignore malformed JSON records and cancelled transactions

filtered_stream = (
    parsed_stream
    .filter(col("transaction_id").isNotNull())
    .filter(col("status").isNotNull())
    .filter(col("status") != "cancelled")
)


# ==================================================
# 8. JOIN CUSTOMER INFORMATION
# ==================================================

print("\nJoining customer information...")

enriched_with_customer = (
    filtered_stream
    .join(
        broadcast_customers,
        on="customer_id",
        how="left"
    )
)


# ==================================================
# 9. JOIN PAYMENT METHOD INFORMATION
# ==================================================

print("Joining payment method information...")

fully_enriched = (
    enriched_with_customer
    .join(
        broadcast_payments,
        on="payment_method_id",
        how="left"
    )
)


# ==================================================
# 10. BUSINESS TRANSFORMATIONS
# ==================================================

print("Applying business transformations...")

current_timestamp_ms = unix_millis(current_timestamp())

final_enriched = (
    fully_enriched

    # Processing timestamp
    .withColumn(
        "processing_timestamp_ms",
        current_timestamp_ms
    )

    # End-to-end latency
    .withColumn(
        "e2e_latency_ms",
        spark_round(
            col("processing_timestamp_ms")
            - col("producer_timestamp_ms"),
            2
        )
    )

    .withColumn(
        "processing_date",
        current_timestamp()
    )

    # Payment processing fee
    .withColumn(
        "processing_fee_amount",
        spark_round(
            col("amount")
            * col("payment_processing_fee_percent")
            / 100.0,
            2
        )
    )

    # High-value transaction
    .withColumn(
        "is_high_value",
        when(col("amount") >= 100.0, True)
        .otherwise(False)
    )

    # Premium customer
    .withColumn(
        "is_premium_customer",
        when(
            col("customer_tier").isin("premium", "gold"),
            True
        ).otherwise(False)
    )

    # Risk score
    .withColumn(
        "risk_score",
        when(
            (col("amount") > 500.0)
            & (col("customer_tier") == "free"),
            "HIGH"
        )
        .when(
            col("transaction_type") == "chargeback",
            "HIGH"
        )
        .when(
            col("amount") > 200.0,
            "MEDIUM"
        )
        .otherwise("LOW")
    )

    # Transaction category
    .withColumn(
        "transaction_category",
        when(
            col("transaction_type").isin(
                "purchase", "subscription"
            ),
            "revenue"
        )
        .when(
            col("transaction_type").isin(
                "refund", "chargeback"
            ),
            "loss"
        )
        .otherwise("neutral")
    )
)


# ==================================================
# 11. PREPARE KAFKA OUTPUT
# ==================================================

output_df = final_enriched.select(

    # Kafka message key
    col("transaction_id")
    .cast("string")
    .alias("key"),

    # Kafka JSON message value
    to_json(
        struct(

            col("transaction_id"),
            col("transaction_reference"),
            col("transaction_type"),
            col("transaction_category"),

            col("amount"),
            col("currency"),
            col("status"),

            col("country_id"),
            col("merchant_id"),
            col("device_type"),

            # Customer information
            col("customer_id"),
            col("customer_name"),
            col("customer_email"),
            col("customer_tier"),
            col("customer_region"),
            col("customer_registration_date"),
            col("is_premium_customer"),

            # Payment information
            col("payment_method_id"),
            col("payment_method_name"),
            col("payment_method_type"),
            col("payment_method_category"),
            col("payment_processing_fee_percent"),
            col("processing_fee_amount"),

            # Risk information
            col("is_high_value"),
            col("risk_score"),

            # Timestamps
            col("producer_timestamp_ms"),
            col("processing_timestamp_ms"),
            col("e2e_latency_ms"),
            col("processing_date")
        )
    ).alias("value")
)


# ==================================================
# 12. WRITE ENRICHED DATA TO KAFKA
# ==================================================

print("\nStarting Kafka output stream...")

query = (
    output_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        "hdfs://namenode:8020/data/rtm"
    )
    .option(
        "checkpointLocation",
        "hdfs://namenode:8020/checkpoints/rtm_parquet"
    )
    .trigger(processingTime=PROCESSING_INTERVAL)
    .start()
)


enriched_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("transaction_reference", StringType()),
    StructField("transaction_type", StringType()),
    StructField("transaction_category", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("status", StringType()),
    StructField("country_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("device_type", StringType()),
    StructField("customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("customer_email", StringType()),
    StructField("customer_tier", StringType()),
    StructField("customer_region", StringType()),
    StructField("is_premium_customer", BooleanType()),
    StructField("payment_method_id", StringType()),
    StructField("payment_method_name", StringType()),
    StructField("payment_method_type", StringType()),
    StructField("payment_method_category", StringType()),
    StructField("payment_processing_fee_percent", DoubleType()),
    StructField("processing_fee_amount", DoubleType()),
    StructField("is_high_value", BooleanType()),
    StructField("risk_score", StringType()),
    StructField("producer_timestamp_ms", LongType()),
    StructField("processing_timestamp_ms", LongType()),
    StructField("e2e_latency_ms", LongType()),
    StructField("processing_date", StringType())
])

# Convert JSON into individual DataFrame columns
display_df = (
    output_df
    .select(
        from_json(col("value").cast("string"), enriched_schema)
        .alias("transaction")
    )
    .select("transaction.*")
)

# Display as a table
query = (
    display_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 20)
    .option(
        "checkpointLocation",
        "hdfs://namenode:8020/checkpoints/transactions_console"
    )
    .trigger(processingTime=PROCESSING_INTERVAL)
    .start()
)


print("=" * 65)
print("STREAMING PIPELINE STARTED")
print("Input topic:", INPUT_TOPIC)
print("Output topic:", OUTPUT_TOPIC)
print("Processing interval:", PROCESSING_INTERVAL)
print("Checkpoint:", CHECKPOINT_LOCATION)
print("=" * 65)

try:
    query.awaitTermination()

except KeyboardInterrupt:
    print("\nStopping streaming pipeline...")

    query.stop()

    print("Streaming pipeline stopped.")

finally:
    spark.stop()
