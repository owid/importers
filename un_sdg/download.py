"""snippet for downloading UN SDG data in CSV format from the SDG API.
"""
import json
import requests
import pandas as pd
import os
import zipfile
from io import BytesIO
from un_sdg import INFILE, METAPATH, METADATA_LOC
from typing import List

base_url = "https://unstats.un.org/sdgapi"

def main():
    delete_output()
    download_data()
    download_metadata()


## Not sure how well this works when the list is longer than one
def delete_output(keep_paths: List[str]) -> None:
    for path in keep_paths:
        if os.path.exists(os.path.join(DATA_PATH, path)):
            for CleanUp in glob.glob(os.path.join(DATA_PATH, '*.*')):
                if not CleanUp.endswith(path):    
                    os.remove(CleanUp)              

def download_data() -> None:
    # retrieves all goal codes
    url = f"{base_url}/v1/sdg/Goal/List"
    res = requests.get(url)
    assert res.ok

    goals = json.loads(res.content)
    goal_codes = [int(goal['code']) for goal in goals]
    # retrieves all area codes
    url = f"{base_url}/v1/sdg/GeoArea/List"
    res = requests.get(url)
    assert res.ok

    areas = json.loads(res.content)
    area_codes = [int(area['geoAreaCode']) for area in areas]
    # retrieves csv with data for all codes and areas
    url = f"{base_url}/v1/sdg/Goal/DataCSV"
    res = requests.post(url, data={'goal': goal_codes, 'areaCodes': area_codes})
    assert res.ok
    df = pd.read_csv(BytesIO(res.content), low_memory = False)
    df.to_csv(os.path.join(INFILE), index=False)

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