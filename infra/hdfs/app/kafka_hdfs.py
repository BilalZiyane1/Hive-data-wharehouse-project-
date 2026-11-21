
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    sum,
    count,
    avg
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

# --------------------------------------------------
# Configuration
# --------------------------------------------------

HDFS_NAMENODE = "namenode"
HDFS_PORT = "8020"

HDFS_OUTPUT = f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}/data/sales/output"
HDFS_CHECKPOINT = f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}/checkpoints/sales"

KAFKA_BOOTSTRAP = "broker:29092"

# --------------------------------------------------
# Spark
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("KafkaStreaming-HDFS")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .config(
        "spark.hadoop.fs.defaultFS",
        f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}"
    ).master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# --------------------------------------------------
# Invoice schema
# --------------------------------------------------

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True)
])

# --------------------------------------------------
# Read invoices from Kafka
# --------------------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:29092")
    .option("subscribe", "invoices")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)


# --------------------------------------------------
# Kafka value -> JSON -> columns
# --------------------------------------------------

invoices = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json")
    .select(
        from_json(col("json"), schema).alias("data")
    )
    .select("data.*")
)

# --------------------------------------------------
# Aggregate by category
# --------------------------------------------------

result = (
    invoices
    .groupBy("category")
    .agg(
        sum("price").alias("total_amount"),
        count("*").alias("invoice_count"),
        avg("price").alias("average_price")
    )
)

# --------------------------------------------------
# Write each complete aggregation snapshot to HDFS
# --------------------------------------------------

def write_to_hdfs(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    print("-------------------------------------------")
    print(f"Batch: {batch_id}")
    print("-------------------------------------------")

    batch_df.show(truncate=False)

    # One HDFS directory per micro-batch
    batch_path = f"{HDFS_OUTPUT}/batch_id={batch_id}"

    (
        batch_df.write
        .format("parquet")
        .mode("overwrite")
        .save(batch_path)
    )

    print(f"Batch {batch_id} written to HDFS: {batch_path}")


# --------------------------------------------------
# Start streaming
# --------------------------------------------------

query = (
    result.writeStream
    .outputMode("complete")
    .foreachBatch(write_to_hdfs)
    .option(
        "checkpointLocation",
        HDFS_CHECKPOINT
    )
    .start()
)

# --------------------------------------------------
# Wait for streaming
# --------------------------------------------------

query.awaitTermination()
