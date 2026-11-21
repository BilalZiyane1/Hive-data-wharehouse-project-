from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TestKafkaConnection")
    .getOrCreate()
)

df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "invoices")
    .option("startingOffsets", "earliest")
    .load()
)

df.selectExpr(
    "CAST(key AS STRING)",
    "CAST(value AS STRING)",
    "topic",
    "partition",
    "offset"
).show(truncate=False)

spark.stop()