#!/usr/bin/env python3

from zipfile import ZipFile
import urllib.request
import boto3
import tempfile

period_list = [
    "Q1_2019", "Q2_2019", "Q3_2019", "Q4_2019", 
    "Q1_2020", "Q2_2020", "Q3_2020", "Q4_2020", 
    "Q1_2021", "Q2_2021", "Q3_2021", "Q4_2021",
    "Q1_2022", "Q2_2022", "Q3_2022", "Q4_2022",
    "Q1_2023", "Q2_2023", "Q3_2023"
    ]

def url_for(period):
    return f"https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_{period}.zip"

def download(period, zip_file):
    with urllib.request.urlopen(url_for(period)) as response:
        zip_file.write(response.read())

s3 = boto3.client('s3')

def extract_and_upload(zip_file, period):
    with ZipFile(zip_file) as zip:
        for name in zip.namelist():
            if not name.startswith("__MACOSX/") and name.endswith(".csv"):
                with zip.open(name) as file:
                    s3.upload_fileobj(file, 'jul-20-xqi', f'output/1/{period}/{name}')

for period in period_list:
    with tempfile.TemporaryFile() as zip_file:
        download(period, zip_file)
        zip_file.seek(0)
        extract_and_upload(zip_file, period)