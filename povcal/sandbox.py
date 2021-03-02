import pandas as pd
import glob
import os
from main import *
import numpy as np
from HeadCount_Files_Downloader import *


def extract_entity_names():
    entities = set()
    for df in all_country_year_headcount_files():
        entities.update(df.CountryName.unique())

    df = pd.DataFrame.from_dict(entities)
    df.to_csv("entities/raw_entities.csv", index=False, header=["Country"])


def redo_files():
    all_files = glob.glob(DETAILED_DATA_DIR + "/*.csv")
    for filename in all_files:
        print(filename)
        df = pd.read_csv(filename, header=0)
        # df = df.reset_index(drop=True)
        df = mark_missing_values_as_NaN(df)
        df.to_csv(filename, index=False)


if __name__ == "__main__":
    extract_entity_names()
