import os
import requests
import zipfile
import io


URL = "http://walden.nyc3.digitaloceanspaces.com/faostat/2021/Food_Security_Data_E_All_Data_(Normalized).zip"
FILENAME = "Food_Security_Data_E_All_Data_(Normalized).csv"


def main(output_path) -> str:
    r = requests.get(URL)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(output_path)
    return os.path.join(output_path, FILENAME)
