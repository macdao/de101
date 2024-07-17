from pyspark.sql import SparkSession
from zipfile import ZipFile
import urllib.request
from pathlib import Path

file_list2 = [
    "Q1_2019", "Q2_2019", "Q3_2019", "Q4_2019", 
    "Q1_2020", "Q2_2020", "Q3_2020", "Q4_2020", 
    "Q1_2021", "Q2_2021", "Q3_2021", "Q4_2021",
    "Q1_2022", "Q2_2022", "Q3_2022", "Q4_2022",
    "Q1_2023", "Q2_2023", "Q3_2023"
    ]

file_list = ["Q1_2019", "Q2_2019"]

def local_path(name):
    return "datasource/data_" + name + ".zip"

def download(name):
    if Path(local_path(name)).is_file():
        return
    with urllib.request.urlopen("https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_" + name + ".zip") as response:
        with open(local_path(name), "wb") as file:
            file.write(response.read())

def extract(name):
    with ZipFile(local_path(name)) as file:
        file.extractall("tmp/1-" + name)

for file in file_list:
    download(file)
    extract(file)

spark = SparkSession \
    .builder \
    .appName("load") \
    .getOrCreate()

path = "tmp/**/*.csv"

df = spark.read.option("header", True).csv(path)
df.write.mode("overwrite").parquet("output/1")