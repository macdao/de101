#!/usr/bin/env python3

from zipfile import ZipFile
import urllib.request
import boto3
import tempfile
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("input_mode", choices=["local", "remote"])
parser.add_argument("output_path")
args = parser.parse_args()
input_mode = args.input_mode
output_path = args.output_path

def period_list():
    if input_mode == "local":
        return ["Q1_2019", "Q1_2022"]
    else:
        return [
            "Q1_2019", "Q2_2019", "Q3_2019", "Q4_2019", 
            "Q1_2020", "Q2_2020", "Q3_2020", "Q4_2020", 
            "Q1_2021", "Q2_2021", "Q3_2021", "Q4_2021",
            "Q1_2022", "Q2_2022", "Q3_2022", "Q4_2022",
            "Q1_2023", "Q2_2023", "Q3_2023"
        ]

def url_for(period):
    if input_mode == "local":
        return f"http://localhost:8000/data_{period}.zip"
    else:
        return f"https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_{period}.zip"

def download(period, zip_file):
    with urllib.request.urlopen(url_for(period)) as response:
        zip_file.write(response.read())

def save(file, period, name):
    if output_path.startswith("s3://"):
        (bucket, folder) = output_path[len("s3://"):].split("/",1)
        s3 = boto3.client('s3')
        s3.upload_fileobj(file, bucket, f'{folder}/{period}/{name}')
    else:
        csv_file_path = f"{output_path}/{period}/{name}"
        os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
        with open(csv_file_path, "wb") as csv_file:
            csv_file.write(file.read())    

def extract(zip_file, period):
    with ZipFile(zip_file) as zip:
        for name in zip.namelist():
            if not name.startswith("__MACOSX/") and name.endswith(".csv"):
                with zip.open(name) as file:
                    save(file, period, name)

for period in period_list():
    with tempfile.TemporaryFile() as zip_file:
        download(period, zip_file)
        zip_file.seek(0)
        extract(zip_file, period)