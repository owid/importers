"""snippet for downloading UN SDG data in CSV format from the SDG API.
"""
import glob
import json
import os
import pandas as pd
import requests
import zipfile
from io import BytesIO
from who_wash import INFILE, INPATH, OUTPATH
from typing import List
from pathlib import Path

base_url = "https://washdata.org/data/country/WLD/household/download"
keep_paths = ["standardized_entity_names.csv"] # must be a list []

def main():
    delete_output(keep_paths)
    download_data()

def delete_output(keep_paths: List[str]) -> None:
    for path in keep_paths:
        if os.path.exists(os.path.join(OUTPATH, path)):
            for CleanUp in glob.glob(os.path.join(OUTPATH, '*.*')):
                if not CleanUp.endswith(path):
                    print("Deleting ", CleanUp)    
                    os.remove(CleanUp)              

def download_data() -> None:
    Path(INPATH).mkdir(parents=True, exist_ok=True)
    resp = requests.get(base_url)
    output = open(INFILE, 'wb')
    output.write(resp.content)
    output.close()

if __name__ == '__main__':
    main()