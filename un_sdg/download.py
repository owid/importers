"""snippet for downloading UN SDG data in CSV format from the SDG API.
"""
import glob
import json
import os
import pandas as pd
import requests
from io import BytesIO
from un_sdg import INFILE, OUTPATH
from typing import List

base_url = "https://unstats.un.org/sdgapi"
keep_paths = ["standardized_entity_names.csv"]  # must be a list []


def main():
    delete_output(keep_paths)
    download_data()


def delete_output(keep_paths: List[str]) -> None:
    output_files = glob.glob(os.path.join(OUTPATH, "*.*"))
    base_files = []
    for file in output_files:
        bf = os.path.basename(file)
        base_files.append(bf)
    ind_file = []
    for file in keep_paths:
        ind = base_files.index(file)
        print(file)
        ind_file.append(ind)
    file_sel = [output_files[i] for i in ind_file]
    clean_up = [x for x in output_files if x not in file_sel]
    for del_file in clean_up:
        os.remove(del_file)


def download_data() -> None:
    # retrieves all goal codes
    print("Retrieving SDG goal codes...")
    url = f"{base_url}/v1/sdg/Goal/List"
    res = requests.get(url)
    assert res.ok

    goals = json.loads(res.content)
    goal_codes = [int(goal["code"]) for goal in goals]
    # retrieves all area codes
    print("Retrieving area codes...")
    url = f"{base_url}/v1/sdg/GeoArea/List"
    res = requests.get(url)
    assert res.ok

    areas = json.loads(res.content)
    area_codes = [int(area["geoAreaCode"]) for area in areas]
    # retrieves csv with data for all codes and areas
    print("Retrieving data...")
    url = f"{base_url}/v1/sdg/Goal/DataCSV"
    res = requests.post(url, data={"goal": goal_codes, "areaCodes": area_codes})
    assert res.ok
    df = pd.read_csv(BytesIO(res.content), low_memory=False)
    df.to_csv(INFILE, index=False, compression="gzip")


if __name__ == "__main__":
    main()
