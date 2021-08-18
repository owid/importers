import os
import requests
import zipfile
import io
import json


URL = "http://walden.nyc3.digitaloceanspaces.com/faostat/2021/faostat_FS.zip"
URL_METADATA = (
    "https://github.com/owid/walden/raw/master/index/faostat/2021/faostat_FS.json"
)
FILENAME = "Food_Security_Data_E_All_Data_(Normalized).csv"


def main(output_path) -> str:
    path_data = download_data(output_path)
    path_metadata = download_metadata(output_path)
    return path_data, path_metadata


def download_data(output_path: str) -> str:
    r = requests.get(URL)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(output_path)
    return os.path.join(output_path, FILENAME)


def download_metadata(output_path: str) -> str:
    metadata = requests.get(URL_METADATA).json()
    path = os.path.join(output_path, "metadata.json")
    with open(path, "w") as f:
        json.dump(metadata, f)
    return path
