
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    regexp_replace,
    explode,
    split,
    count
)

# ------------------------------------------
# Spark session
# ------------------------------------------

spark = (
    SparkSession.builder
    .appName("SparkWordCount")
    .master("spark://spark-master:7077")
    .config(
        "spark.hadoop.fs.defaultFS",
        "hdfs://namenode:8020"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ------------------------------------------
# Read text from HDFS
# ------------------------------------------

input_path = "hdfs://namenode:8020/data/sales/data.txt"

df = spark.read.text(input_path)

# ------------------------------------------
# Clean and tokenize words
# ------------------------------------------

words = (
    df
    .select(
        explode(
            split(
                regexp_replace(
                    lower(col("value")),
                    r"[^a-z0-9\s]",
                    ""
                ),
                r"\s+"
            )
        ).alias("word")
    )
    .filter(col("word") != "")
)

# ------------------------------------------
# Count words
# ------------------------------------------

word_counts = (
    words
    .groupBy("word")
    .agg(count("*").alias("total"))
    .orderBy(col("total").desc(), col("word"))
)

# ------------------------------------------
# Display results
# ------------------------------------------

word_counts.show(truncate=False)

# ------------------------------------------
# Save results to HDFS
# ------------------------------------------

output_path = "hdfs://namenode:8020/data/sales/output"

(
    word_counts.write
    .mode("overwrite")
    .parquet(output_path)
)

print("WordCount successfully saved to HDFS!")

spark.stop()

