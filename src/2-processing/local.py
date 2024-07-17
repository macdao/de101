from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import IntegerType

spark = SparkSession \
    .builder \
    .appName("processing") \
    .getOrCreate()

path = "output/1"

df = spark.read.parquet(path)
result = df.groupBy(df.date).agg(sf.count("*").alias("count"), sf.sum(df.failure).cast(IntegerType()).alias("failures"))

result.write.mode("overwrite").parquet("output/2")