from pyspark.sql import SparkSession
from zipfile import ZipFile
import urllib.request
from pathlib import Path
from pyspark.sql import functions as sf

file_list2 = [
    "Q1_2019", "Q2_2019", "Q3_2019", "Q4_2019", 
    "Q1_2020", "Q2_2020", "Q3_2020", "Q4_2020", 
    "Q1_2021", "Q2_2021", "Q3_2021", "Q4_2021",
    "Q1_2022", "Q2_2022", "Q3_2022", "Q4_2022",
    "Q1_2023", "Q2_2023", "Q3_2023"
    ]

file_list = ["Q1_2019", "Q2_2019"]

output_path = "data_lake/1"

def zip_local_path(name):
    return f"cache/{zip_filename(name)}"

def zip_filename(name):
    return f"data_{name}.zip"

def download(name):
    if Path(zip_local_path(name)).is_file():
        return
    with urllib.request.urlopen(f"https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/{zip_filename(name)}") as response:
        with open(zip_local_path(name), "wb") as file:
            file.write(response.read())

def extract(name):
    with ZipFile(zip_local_path(name)) as file:
        file.extractall(f"{output_path}/{name}")

for file in file_list:
    download(file)
    extract(file)