from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("input_path")
parser.add_argument("output_path")
args = parser.parse_args()
input_path = args.input_path
output_path = args.output_path

spark = SparkSession \
    .builder \
    .appName("publish") \
    .getOrCreate()

df = spark.read.parquet(input_path)
df.write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()