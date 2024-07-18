from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import IntegerType

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

path = "output/1"

df = spark.read.parquet(path)

df.withColumn("year", sf.year(df.date))\
    .withColumn("brand", to_brand(df.model))\
    .groupBy(sf.col("year"), sf.col("brand"))\
    .agg(sf.sum(df.failure).cast(IntegerType()).alias("failures"))\
    .write.mode("overwrite").parquet("output/2_yearly_summary")

spark.stop()