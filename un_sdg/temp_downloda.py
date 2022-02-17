import json
import pandas as pd
import requests
from io import BytesIO
import os

base_url = "https://unstats.un.org/sdgapi"
DATASET_VERSION = "2022-02"
INPATH = os.path.join("un_sdg", "input")
INFILE = os.path.join(INPATH, "un-sdg-" + DATASET_VERSION + ".csv.zip")


# retrieves all goal codes
print("Retrieving SDG goal codes...")
url = f"{base_url}/v1/sdg/Goal/List"
res = requests.get(url)
assert res.ok

goals = json.loads(res.content)
goal_codes = [str(goal["code"]) for goal in goals]
# retrieves all area codes
print("Retrieving area codes...")
url = f"{base_url}/v1/sdg/GeoArea/List"
res = requests.get(url)
assert res.ok

areas = json.loads(res.content)
area_codes = [str(area["geoAreaCode"]) for area in areas]
# retrieves csv with data for all codes and areas
print("Retrieving data...")
url = f"{base_url}/v1/sdg/Goal/DataCSV"
for goal in goal_codes:
    res = requests.post(url, data={"goal": goal, "areaCodes": area_codes})
    print(f"{goal} downloaded {res.ok}")
    assert res.ok
    df = pd.read_csv(BytesIO(res.content), low_memory=False)
    INFILE = os.path.join(INPATH, goal + "un-sdg-" + DATASET_VERSION + ".csv.zip")
    df.to_csv(INFILE, index=False, compression="gzip")
