from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import IntegerType
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("input_path")
parser.add_argument("output_path")
args = parser.parse_args()
input_path = args.input_path
output_path = args.output_path

spark = SparkSession \
    .builder \
    .appName("processing") \
    .getOrCreate()

df = spark.read\
    .option("header", True)\
    .option("recursiveFileLookup", True)\
    .csv(input_path, pathGlobFilter="*.csv")

# in case of 6/17/19
date_column = sf.when(sf.to_date(df.date).isNull(), sf.to_date(df.date, "M/d/yy")).otherwise(df.date)
df = df.select(date_column.alias("date"), df.model, df.failure)

result = df.groupBy(df.date).agg(sf.count("*").alias("count"), sf.sum(df.failure).cast(IntegerType()).alias("failures"))

result.write.mode("overwrite").parquet(output_path)

spark.stop()