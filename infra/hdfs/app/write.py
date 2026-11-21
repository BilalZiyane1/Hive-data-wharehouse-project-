from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("HDFS-Test")
    .getOrCreate()
)

sc = spark.sparkContext

uri = "hdfs://namenode:8020"

# Test HDFS connection
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
    sc._jvm.java.net.URI(uri),
    sc._jsc.hadoopConfiguration()
)

print("HDFS connected:", fs.getUri())

# Create directory
path = sc._jvm.org.apache.hadoop.fs.Path(
    "/data/spark_test"
)

print("Directory exists:", fs.exists(path))

# Write test data
df = spark.createDataFrame(
    [(1, "Oracle"), (2, "Spark"), (3, "Hadoop")],
    ["id", "technology"]
)

df.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/data/spark_test"
)

print("SUCCESS: Data written to HDFS")

spark.stop()