from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .getOrCreate()

data = [("ORD-001", "delivered", 1000), ("ORD-002", "processing", 500)]
df = spark.createDataFrame(data, ["order_id", "status", "amount"])
df.show()

spark.stop()