from pyspark.sql import SparkSession

input_path = "data_lake/2_daily_summary"
output_path = "data_lake/3_daily_summary"

spark = SparkSession \
    .builder \
    .appName("publish") \
    .getOrCreate()

df = spark.read.parquet(input_path)
df.write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()