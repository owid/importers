import pandas as pd
import glob
import os

from main import *
import numpy as np

import sys

sys.path.append("..")
sys.path.append("../..")
sys.path.append(".")
import pdb

from HeadCount_Files_Downloader import *

from db import connection
from db_utils import DBUtils
import requests


def query_api():
    for poverty_line in range(100):
        api_address = "http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx"
        params = {
            "Countries": "CHN",
            "YearSelected": "2000",
            "PovertyLine": poverty_line,
            "display": "C",
        }
        result = requests.get(
            api_address,
            params=params,
            timeout=30,
        )
        yield result.text


def insert_test():
    with connection.cursor() as c:
        db = DBUtils(c)
        db_dataset_id = db.upsert_dataset(
            name="AAAAA", description="AAAAA", namespace="AAAAA", user_id=23
        )


def extract_entity_names():
    entities = set()
    df = pd.read_csv("output/mega.csv")
    entities.update(df.CountryName.unique())

    df = pd.DataFrame.from_dict(entities)
    df.to_csv("entities/raw_entities.csv", index=False, header=["Country"])


def redo_files():
    all_files = glob.glob(HEADCOUNTS_DIR + "/*.csv")
    num_fails = 0
    for filename in all_files:
        # print(filename)
        df = pd.read_csv(filename, header=0)
        if df.duplicated(subset=["CountryName", "RequestYear"]).any():
            num_fails += 1
            os.remove(filename)
        # df = rename_columns(df)
        # df.to_csv(filename, index=False)
    print(num_fails)


if __name__ == "__main__":
    redo_files()
