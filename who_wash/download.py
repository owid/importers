"""snippet for downloading UN SDG data in CSV format from the SDG API.
"""
import glob
import json
import os
import pandas as pd
import requests
import zipfile
from io import BytesIO
from who_wash import INFILE, METAPATH, METADATA_LOC, OUTPATH
from typing import List
from pathlib import Path

base_url = "https://washdata.org/data/country/WLD/household/download"
keep_paths = ["standardized_entity_names.csv"] # must be a list []

def main():
    delete_output(keep_paths)
    download_data()
    download_metadata()


def delete_output(keep_paths: List[str]) -> None:
    for path in keep_paths:
        if os.path.exists(os.path.join(OUTPATH, path)):
            for CleanUp in glob.glob(os.path.join(OUTPATH, '*.*')):
                if not CleanUp.endswith(path):
                    print("Deleting ", CleanUp)    
                    os.remove(CleanUp)              

def download_data() -> None:
    Path(OUTPATH).mkdir(parents=True, exist_ok=True)
    resp = requests.get(base_url)
    output = open(os.path.join(OUTPATH, "who_unicef_wash.xlsx"), 'wb')
    output.write(resp.content)
    output.close()

def download_metadata() -> None:
    # Download metadata
    zip_url = METADATA_LOC
    r = requests.get(zip_url)  
    with open(os.path.join(METAPATH, 'sdg-metadata.zip'), 'wb') as f:
        f.write(r.content)
        
    # Unzip metadata
    with zipfile.ZipFile(os.path.join(METAPATH, 'sdg-metadata.zip'), 'r') as zip_ref:
        zip_ref.extractall(METAPATH)

    #docx metadata is downloaded as well as pdf, this deletes the docx
    files_in_directory = os.listdir(METAPATH)
    filtered_files = [file for file in files_in_directory if not file.endswith(".pdf")]
    for file in filtered_files:
	    path_to_file = os.path.join(METAPATH, file)
	    os.remove(path_to_file)

if __name__ == '__main__':
    main()