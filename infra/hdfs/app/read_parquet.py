
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ReadParquetFromHDFS")
    .getOrCreate()
)

# HDFS Parquet location
HDFS_PATH = "hdfs://namenode:8020/data/sales/parquet"

# Read Parquet files
df = spark.read.parquet(HDFS_PATH)

print("========== SCHEMA ==========")
df.printSchema()

print("========== ALL DATA ==========")
df.show(truncate=False)

print("========== TOTAL RECORDS ==========")
print(df.count())

print("========== DATA ANALYSIS ==========")
df.groupBy("product").sum("total").show()

spark.stop()
