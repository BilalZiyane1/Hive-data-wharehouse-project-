from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TestParquetFile")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

path = (
    "hdfs://namenode:8020/data/rtm/"
    "part-00000-b720eaeb-b622-4bca-82f1-b73e19e2b412-c000.snappy.parquet"
)

df = spark.read.parquet(path)

print("========== SCHEMA ==========")
df.printSchema()

print("========== DATA ==========")
df.show(20, truncate=False)

print("========== RECORD COUNT ==========")
print(df.count())

spark.stop()