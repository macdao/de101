from zipfile import ZipFile
import urllib.request
import os
import tempfile

period_list = ["Q1_2019", "Q1_2022"]

output_path = "data_lake/1"

def url_for(period):
    return f"http://localhost:8000/data_{period}.zip"
    # return f"https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_{period}.zip"

def download(period, zip_file):
    with urllib.request.urlopen(url_for(period)) as response:
        zip_file.write(response.read())

def extract(zip_file, period):
    with ZipFile(zip_file) as zip:
        for name in zip.namelist():
            if not name.startswith("__MACOSX/") and name.endswith(".csv"):
                csv_file_path = f"{output_path}/{period}/{name}"
                os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
                with zip.open(name) as file, open(csv_file_path, "wb") as csv_file:
                    csv_file.write(file.read())

for period in period_list:
    with tempfile.TemporaryFile() as zip_file:
        download(period, zip_file)
        zip_file.seek(0)
        extract(zip_file, period)