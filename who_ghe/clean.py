"""
After running load_and_clean() to create $ENTFILE use the country standardiser tool to standardise $ENTFILE
1. Open the OWID Country Standardizer Tool
   (https://owid.cloud/admin/standardize);
2. Change the "Input Format" field to "Non-Standard Country Name";
3. Change the "Output Format" field to "Our World In Data Name"; 
4. In the "Choose CSV file" field, upload $ENTFILE;
5. For any country codes that do NOT get matched, enter a custom name on
   the webpage (in the "Or enter a Custom Name" table column);
    * NOTE: For this dataset, you will most likely need to enter custom
      names for regions/continents (e.g. "Arab World", "Lower middle
      income");
6. Click the "Download csv" button;
7. Name the downloaded csv 'standardized_entity_names.csv' and save in the output folder;
8. Rename the "Country" column to "country_code".
"""

import pandas as pd
import os
import shutil
import json
import numpy as np
import re
from pathlib import Path
from tqdm import tqdm

pd.set_option("display.max_columns", None)

from who_ghe import INFILE, OUTPATH, ENTFILE


def load_and_clean() -> pd.DataFrame:
    # Load and clean the data
    print("Reading in original data...")
    original_df = pd.read_csv(INFILE, low_memory=False).drop(
        ["Unnamed: 0", "Unnamed: 0.1"], axis=1
    )
    # Check there aren't any null values
    assert sum(original_df.isnull().sum()) == 0, print("Null values in dataframe")
    print("Extracting unique entities to " + ENTFILE + "...")
    original_df[["COUNTRY_CODE"]].drop_duplicates().dropna().rename(
        columns={"COUNTRY_CODE": "Country"}
    ).to_csv(ENTFILE, index=False)
    # Make the datapoints folder
    Path(OUTPATH, "datapoints").mkdir(parents=True, exist_ok=True)
    return original_df
