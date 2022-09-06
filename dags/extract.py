import os
import csv
import re
import tempfile
from zipfile import ZipFile

# set path data
base_path = os.path.abspath(__file__ + "/../../")
source_path = f"{base_path}/data/source/csv_sample_data.zip"
raw_path = f"{base_path}/data/raw/"


def create_folder_if_not_exists(path):
    """
    Create a new folder if it doesn't exists
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)

def extract_new_raw_data():
    """
    Extract new raw data from the current source from the source
    """

    create_folder_if_not_exists(raw_path)
    with tempfile.TemporaryDirectory() as dirpath:
        with ZipFile(
            source_path,
            "r"
        ) as zipfile:
            names_list = zipfile.namelist()
            for filelist in names_list:
                csv_file_path = zipfile.extract(filelist, path=dirpath)
                # Open the CSV file in read mode
                with open(csv_file_path, mode="r", encoding="windows-1252") as csv_file:
                    reader = csv.DictReader(csv_file)
                    row = next(reader)  # Get first row from reader
                    print("[Extract] First row example:", row)
                    print("[Extract] First row example:", filelist)

                    # Open the CSV file in write mode
                    with open(
                        raw_path+filelist,
                        mode="w",
                        encoding="windows-1252"
                    ) as csv_file:
                        # Rename field names so they're ready for the next step
                        fieldnames = {k: re.sub("\(.*?\)|[^a-zA-Z_]","", k).lower() for k in row}
                        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                        # Write headers as first line
                        writer.writerow(fieldnames)
                        for row in reader:
                            # Write all rows in file
                            writer.writerow(row)

def main():
    print("[Extract] Start")
    print(f"[Extract] Unzip data from '{source_path}' to '{raw_path}'")
    extract_new_raw_data()
    print(f"[Extract] End")