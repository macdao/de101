from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import IntegerType

input_path = "data_lake/1/**/*.csv"
output_path = "data_lake/2_yearly_summary"

model_to_brand = {
    'CT': "Crucial",
    'DELLBOSS': "Dell BOSS",
    'HGST': "HGST",
    "Seagate": "Seagate",
    "ST": "Seagate",
    "TOSHIBA": "Toshiba",
    "WDC": "Western Digital"
    }

def to_brand(value):
    column = None
    for prefix, brand in model_to_brand.items():
        if column is None:
            column = sf.when(value.startswith(prefix), brand)
        else:
            column = column.when(value.startswith(prefix), brand)
    return column.otherwise("Other")

spark = SparkSession \
    .builder \
    .appName("processing") \
    .getOrCreate()

df = spark.read.option("header", True).csv(input_path)

# in case of 6/17/19
date_column = sf.when(sf.to_date(df.date).isNull(), sf.to_date(df.date, "M/d/yy")).otherwise(df.date)
df = df.select(date_column.alias("date"), df.model, df.failure)

df.withColumn("year", sf.year(df.date))\
    .withColumn("brand", to_brand(df.model))\
    .groupBy(sf.col("year"), sf.col("brand"))\
    .agg(sf.sum(df.failure).cast(IntegerType()).alias("failures"))\
    .write.mode("overwrite").parquet(output_path)

spark.stop()