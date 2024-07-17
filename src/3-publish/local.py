from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("publish") \
    .getOrCreate()

path = "output/2"

df = spark.read.parquet(path)
df.write.mode("overwrite").option("header", True).csv("output/3")
