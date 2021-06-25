"""snippet for downloading UN SDG data in CSV format from the SDG API.
"""
import glob
import json
import os
import pandas as pd
import requests
import shutil
import zipfile
from io import BytesIO
from un_sdg import INFILE, METAPATH, METADATA_LOC, OUTPATH
from typing import List

base_url = "https://unstats.un.org/sdgapi"
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
                    print("Deleting ", CleanUp, "...")    
                    os.remove(CleanUp)              

def download_data() -> None:
    # retrieves all goal codes
    print("Retrieving SDG goal codes...")
    url = f"{base_url}/v1/sdg/Goal/List"
    res = requests.get(url)
    assert res.ok

    goals = json.loads(res.content)
    goal_codes = [int(goal['code']) for goal in goals]
    # retrieves all area codes
    print("Retrieving area codes...")
    url = f"{base_url}/v1/sdg/GeoArea/List"
    res = requests.get(url)
    assert res.ok

    areas = json.loads(res.content)
    area_codes = [int(area['geoAreaCode']) for area in areas]
    # retrieves csv with data for all codes and areas
    print("Retrieving data...")
    url = f"{base_url}/v1/sdg/Goal/DataCSV"
    res = requests.post(url, data={'goal': goal_codes, 'areaCodes': area_codes})
    assert res.ok
    df = pd.read_csv(BytesIO(res.content), low_memory = False)
    df.to_csv(INFILE, index=False)
    df.to_csv(INFILE + ".zip", index=False, compression='gzip')

def download_metadata() -> None:
    # Download metadata
    zip_url = METADATA_LOC
    print("Retrieving metadata...")
    r = requests.get(zip_url)  
    with open(os.path.join(METAPATH, 'sdg-metadata.zip'), 'wb') as f:
        f.write(r.content)
        
    # Unzip metadata
    with zipfile.ZipFile(os.path.join(METAPATH, 'sdg-metadata.zip'), 'r') as zip_ref:
        zip_ref.extractall(METAPATH)

    #docx metadata is downloaded as well as pdf, this deletes the docx
    files_in_directory = os.listdir(METAPATH)
    filtered_files = [file for file in files_in_directory if not file.endswith((".pdf"))]
    print("Removing .docx files...")
    for file in filtered_files:
	    path_to_file = os.path.join(METAPATH, file)
	    os.remove(path_to_file)
    shutil.make_archive(METAPATH, 'zip', METAPATH)

if __name__ == '__main__':
    main()