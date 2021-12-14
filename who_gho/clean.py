"""Cleans data from the World Health Organization Global Health Observatory (WHO GHO).

https://www.who.int/data/gho/info/gho-odata-api

Usage:

    python -m who_gho.clean

Instructions for manually standardizing entity names:

0. Retrieve all unique entity names in the dataset:

1. Open the OWID Country Standardizer Tool
   (https://owid.cloud/admin/standardize);

2. Change the "Input Format" field to "Non-Standard Country Name";

3. Change the "Output Format" field to "Our World In Data Name";

4. In the "Choose CSV file" field, upload {outfpath};

5. For any country codes that do NOT get matched, enter a custom name on
   the webpage (in the "Or enter a Custom Name" table column);

    * NOTE: For this dataset, you will most likely need to enter custom
      names for regions/continents;

6. Click the "Download csv" button;

7. Replace {outfpath} with the downloaded CSV;

"""

from asyncio.futures import Future
import os
import re
import time
import simplejson as json
import logging
import pandas as pd
from pandas.api.types import is_numeric_dtype

from dotenv import load_dotenv
from who_gho import CONFIGPATH, SELECTED_VARS_ONLY, OUTPATH

from who_gho.core import (
    clean_datasets,
    get_variable_codes,
    get_metadata_url,
    clean_and_create_datapoints,
    clean_sources,
    clean_variables,
    get_distinct_entities,
)


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()

CURRENT_DIR = os.path.dirname(__file__)
# CURRENT_DIR = os.path.join(os.getcwd(), 'who_gho')
INPATH = os.path.join(CURRENT_DIR, "input")
OUTPATH = os.path.join(CURRENT_DIR, "output")


# DELETE_EXISTING_INPUTS: If True, deletes all existing input data on disk in
# `os.path.join(CURRENT_DIR, 'input')`
DELETE_EXISTING_INPUTS = True

# DOWNLOAD_INPUTS: If True, downloads input data and saves it to disk.
DOWNLOAD_INPUTS = True

# KEEP_PATHS: Names of files in `{CURRENT_DIR}/output` that you do NOT
# want deleted in the beginning of this script.
KEEP_PATHS = ["standardized_entity_names.csv"]


def main() -> None:

    # loads mapping of "{UNSTANDARDIZED_ENTITY_CODE}" -> "{STANDARDIZED_OWID_NAME}"
    # i.e. {"AFG": "Afghanistan", "SSF": "Sub-Saharan Africa", ...}
    entity2owid_name = (
        pd.read_csv(os.path.join(CONFIGPATH, "standardized_entity_names.csv"))
        .set_index("Code")["Our World In Data Name"]
        .squeeze()
        .to_dict()
    )

    # cleans datasets, datapoints, variables, and sources.
    df_datasets = clean_datasets()
    assert (
        df_datasets.shape[0] == 1
    ), f"Only expected one dataset in {os.path.join(OUTPATH, 'datasets.csv')}."

    df_sources = clean_sources(
        dataset_id=df_datasets["id"].iloc[0],
        dataset_name=df_datasets["name"].iloc[0],
    )

    variable_codes = get_variable_codes(selected_vars_only=SELECTED_VARS_ONLY)

    var_code2meta = clean_and_create_datapoints(
        variable_codes=variable_codes, entity2owid_name=entity2owid_name
    )

    df_variables = clean_variables(
        dataset_id=df_datasets["id"].iloc[0],
        source_id=df_sources["id"].iloc[0],
        variables=variable_codes,
        var_code2meta=var_code2meta,
    )

    df_distinct_entities = pd.DataFrame(get_distinct_entities(), columns=["name"])

    # saves datasets, sources, variables, and distinct entities to disk.
    df_datasets.to_csv(os.path.join(OUTPATH, "datasets.csv"), index=False)
    df_sources.to_csv(os.path.join(OUTPATH, "sources.csv"), index=False)
    df_variables.to_csv(os.path.join(OUTPATH, "variables.csv"), index=False)
    df_distinct_entities.to_csv(
        os.path.join(OUTPATH, "distinct_countries_standardized.csv"), index=False
    )


if __name__ == "__main__":
    main()
