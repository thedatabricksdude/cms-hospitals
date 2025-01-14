# Databricks notebook source
import os
import requests
import csv
import tempfile
import datetime
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set the download directory
DOWNLOAD_DIR = "/Volumes/clinical_trials/volumes/landing_to_raw_tmp/tempdata"

# Ensure the directory exists
def ensure_directory_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path, exist_ok=True)

# Function to convert column names to snake_case
def convert_to_snake_case(columns):
    processed_columns = []
    for column in columns:
        new_column_name = (
            column.replace(" ", "_")
            .replace("'", "")
            .replace('"', "")
            .replace("%", "")
            .replace("(", "")
            .replace(")", "")
            .replace("-", "_")
            .lower()
        )
        processed_columns.append(new_column_name)
    return processed_columns

# Function to download and process a single CSV file
def download_and_process_file(file_url, last_modified):
    try:
        file_name = file_url.split("/")[-1]
        root_name = os.path.splitext(file_name)[0]  # Get the root filename without extension
        metadata_file = os.path.join(DOWNLOAD_DIR, f"{root_name}_metadata.txt")
        local_file_path = os.path.join(DOWNLOAD_DIR, file_name)

        response = requests.get(file_url)
        response.raise_for_status()

        with open(local_file_path, "wb") as f:
            f.write(response.content)

        processed_file_path = os.path.join(DOWNLOAD_DIR, f"processed_{file_name}")

        # Ensure `processed_file_path` does not exist as a directory
        if os.path.isdir(processed_file_path):
            os.rmdir(processed_file_path)

        # Read and process the file
        with open(local_file_path, "r") as infile:
            reader = csv.reader(infile)
            headers = next(reader)  # Get the headers

            processed_headers = convert_to_snake_case(headers)

            with tempfile.NamedTemporaryFile("w", delete=False, newline="", dir=DOWNLOAD_DIR) as temp_file:
                writer = csv.writer(temp_file)
                writer.writerow(processed_headers)
                writer.writerows(reader)
                temp_file_path = temp_file.name

        shutil.move(temp_file_path, processed_file_path)

        # Metadata Handling: Load, Update, Overwrite
        existing_metadata = []
        if os.path.exists(metadata_file):
            with open(metadata_file, "r") as meta_file:
                existing_metadata = meta_file.readlines()

        new_metadata_entry = f"{file_url},{last_modified},{datetime.datetime.now()}\n"
        existing_metadata.append(new_metadata_entry)

        with open(metadata_file, "w") as meta_file:
            meta_file.writelines(existing_metadata)

        print(f"Processed and saved: {processed_file_path}, Metadata: {metadata_file}")
    except Exception as e:
        print(f"Error processing file {file_url}: {e}")

# Ensure directories exist
ensure_directory_exists(DOWNLOAD_DIR)

# Fetch datasets from the CMS API
BASE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
response = requests.get(BASE_URL)
response.raise_for_status()
datasets = response.json()

# Filter datasets for theme "hospitals"
files_to_download = []
if os.path.exists(DOWNLOAD_DIR):
    all_metadata_files = [
        f
        for f in os.listdir(DOWNLOAD_DIR)
        if f.endswith("_metadata.txt")
    ]
    existing_metadata = {}
    for metadata_file in all_metadata_files:
        metadata_path = os.path.join(DOWNLOAD_DIR, metadata_file)
        with open(metadata_path, "r") as meta_file:
            for line in meta_file:
                file_url, last_modified, _ = line.strip().split(",")
                existing_metadata[file_url] = last_modified
else:
    existing_metadata = {}

for dataset in datasets:
    theme = dataset.get("theme", [])
    if isinstance(theme, list) and any("hospitals" in t.lower() for t in theme):
        file_url = dataset.get("distribution", [{}])[0].get("downloadURL")
        last_modified = dataset.get("modified")
        if file_url and (file_url not in existing_metadata or existing_metadata[file_url] != last_modified):
            files_to_download.append((file_url, last_modified))

# Process files concurrently
MAX_WORKERS = 16  # Adjust this based on the number of cores in your Databricks cluster
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    future_to_file = {executor.submit(download_and_process_file, url, modified): (url, modified) for url, modified in files_to_download}
    for future in as_completed(future_to_file):
        file_url, last_modified = future_to_file[future]
        try:
            future.result()
        except Exception as e:
            print(f"Error processing file {file_url}: {e}")