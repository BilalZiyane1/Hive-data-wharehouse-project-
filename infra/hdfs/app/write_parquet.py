
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg

# Spark session
spark = (
    SparkSession.builder
    .appName("SaveDataFrameToHDFS")
    .getOrCreate()
)

# Example DataFrame
data = [
    ("P001", "Laptop", 2, 750.0),
    ("P002", "Mouse", 5, 20.0),
    ("P003", "Keyboard", 3, 45.0),
    ("P004", "Laptop", 1, 750.0),
    ("P005", "Screen", 2, 200.0)
]

columns = ["product_id", "product", "quantity", "price"]

df = spark.createDataFrame(data, columns)

# Display original DataFrame
df.show()

# Calculate total amount
df = df.withColumn(
    "total",
    col("quantity") * col("price")
)

# Save as Parquet in HDFS
HDFS_PATH = "hdfs://namenode:8020/data/sales/parquet"

df.write \
    .mode("overwrite") \
    .parquet(HDFS_PATH)

print("DataFrame successfully saved to HDFS!")

spark.stop()
