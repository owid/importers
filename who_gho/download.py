import os
from pandas.core.frame import DataFrame
import json
from typing import List
from pathlib import Path
import requests
import numpy as np
import logging
import tqdm
import time
import pandas as pd
import shutil
from utils import batchify
from pandas.api.types import is_numeric_dtype
from who_gho import (
    CONFIGPATH,
    DELETE_EXISTING_INPUTS,
    DOWNLOAD_INPUTS,
    INPATH,
    OUTPATH,
    KEEP_PATHS,
    CURRENT_DIR,
    DATASET_LINK,
    SELECTED_VARS_ONLY,
)

from who_gho.core import _fetch_data_many_variables

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main() -> None:
    if DELETE_EXISTING_INPUTS:
        delete_input()
    get_entities()
    delete_output(KEEP_PATHS)
    make_dirs(INPATH, OUTPATH, CONFIGPATH)
    # loads variables to be cleaned and uploaded.
    if DOWNLOAD_INPUTS:
        download_data(SELECTED_VARS_ONLY)


def make_dirs(inpath: str, outpath: str, configpath: str) -> None:
    """
    Creating the necessary directories for the input, output and config files
    """
    Path(inpath).mkdir(parents=True, exist_ok=True)
    Path(outpath, "datapoints").mkdir(parents=True, exist_ok=True)
    Path(configpath).mkdir(parents=True, exist_ok=True)


def delete_input() -> None:
    """deletes all files and folders in `{INPATH}`.

    WARNING: this method deletes all input data and is only intended for use
    immediately prior to `download_data()`.

    Returns:

        None.
    """
    assert DOWNLOAD_INPUTS, (
        "You may only delete existing data inputs if `DOWNLOAD_DATA=True`. "
        "Existing data inputs have not been deleted."
    )
    if os.path.exists(INPATH):
        shutil.rmtree(INPATH)
    logger.info(f"Deleted all existing input files in {INPATH}")


def get_entities():
    dimensions = ["country", "region"]
    entities = []
    for dim in dimensions:
        entities += json.loads(
            requests.get(
                f"https://ghoapi.azureedge.net/api/DIMENSION/{dim}/DimensionValues"
            ).content
        )["value"]
    df_entities = pd.DataFrame(entities)
    df_entities = df_entities[df_entities["Title"] != "SPATIAL_SYNONYM"]
    df_entities[["Code", "Title"]].drop_duplicates().dropna().rename(
        columns={"Title": "Country"}
    ).to_csv(os.path.join(CONFIGPATH, "countries.csv"), index=False)


def delete_output(keep_paths: List[str]) -> None:
    """deletes all files in `{CURRENT_DIR}/output` EXCEPT for any file
    names in `keep_paths`.

    Arguments:

        keep_paths: List[str]. List of subpaths in `{CURRENT_DIR}/output`
            that you do NOT want deleted. They will be temporarily moved to
            `{CURRENT_DIR}` and then back into `{CURRENT_DIR}/output` after
            everything else in `{CURRENT_DIR}/output` has been deleted.

    Returns:

        None.
    """
    # temporarily moves some files out of the output directory so that they
    # are not deleted.
    for path in keep_paths:
        if os.path.exists(os.path.join(OUTPATH, path)):
            os.rename(os.path.join(OUTPATH, path), os.path.join(OUTPATH, "..", path))
    # deletes all remaining output files
    if os.path.exists(OUTPATH):
        shutil.rmtree(OUTPATH)
        os.makedirs(OUTPATH)
    # moves the exception files back into the output directory.
    for path in keep_paths:
        if os.path.exists(os.path.join(OUTPATH, "..", path)):
            os.rename(os.path.join(OUTPATH, "..", path), os.path.join(OUTPATH, path))


def load_variables_to_clean() -> List[dict]:
    """loads the array of variables to clean.

    Returns:

        variables: List[dict]. Array of variables to clean. Example:

            [
                {
                    "originalMetadata": {
                        "IndicatorCode": "MDG_0000000007",
                        "IndicatorName": "Under-five mortality rate (probability of dying by age 5 per 1000 live births)"
                    },
                    "name": "Under-five mortality rate (probability of dying by age 5 per 1000 live births)",
                    "unit": "%",
                    "shortUnit": "%",
                    "description": "The share of newborns who die before reaching the age of five",
                    "code": "MDG_0000000007",
                    "coverage": null,
                    "timespan": null,
                    "display": {"name": "Child mortality rate", "unit": "%", "shortUnit": "%", "numDecimalPlaces": 1}
                },
                ...
            ]
    """
    with open(os.path.join(CURRENT_DIR, "config", "variables_to_clean.json"), "r") as f:
        variables = json.load(f)["variables"]
    return variables


def download_data(selected_vars_only: bool) -> None:
    if selected_vars_only:
        variables_to_clean = load_variables_to_clean()
        variable_codes = [
            ind["originalMetadata"]["IndicatorCode"] for ind in variables_to_clean
        ]
    else:
        indicator_url = os.path.join(DATASET_LINK, "Indicator")
        ind_json = requests.get(indicator_url).json()["value"]
        ind = pd.DataFrame.from_records(ind_json)
        variable_codes = ind["IndicatorCode"].to_list()

    for ind_code in variable_codes:
        print(f"{INPATH}/{ind_code}.csv", end="", flush=True)
        data = get_ind_data(ind_code)
        data.to_csv(f"{INPATH}/{ind_code}.csv")
    return variable_codes


def get_ind_data(code: str) -> pd.DataFrame:
    data_url = "https://ghoapi.azureedge.net/api/{code}"
    url = data_url.format(code=code)
    data_json = requests.get(url).json()
    data_df = pd.DataFrame.from_records(data_json["value"]).set_index("Id")
    return data_df


def download_data_bob(variable_codes: List[str]) -> None:
    """Downloads the raw WHO GHO data for aa subset of variable codes and saves
    the data in csv format to `{INPATH}`.

    Arguments:

        variable_codes: List[str]. List of variable codes for which to download
            WHO GHO data. Example:

                ["MDG_0000000017", "WHS4_100", ...]

    Returns:

        None.
    """
    logger.info("Downloading data...")
    batch_size = 200
    wait = 2
    n_batches = int(np.ceil(len(variable_codes) / batch_size))
    dataframes = []
    # NOTE: we retrieve the indicators in batches of {batch_size} with a
    # brief between batchs in order to avoid sending >2000 requests to
    # the api simultaneously.
    for batch in tqdm(batchify(variable_codes, batch_size=batch_size), total=n_batches):
        dataframes += _fetch_data_many_variables(batch)
        time.sleep(wait)

    df = pd.concat([df for df in dataframes if df is not None], axis=0)

    codes_not_fetched = set(variable_codes).difference(df["IndicatorCode"].unique())
    assert len(codes_not_fetched) == 0, (
        "Failed to retrieve data for the following variable codes: "
        f"{codes_not_fetched}"
    )

    df.sort_values(["IndicatorCode", "SpatialDim", "TimeDim", "Dim1Type"], inplace=True)

    # KLUDGE: drops indicator-country-year duplicates. Some duplicates
    # may exist b/c the WHO GHO stores the same indicator-country-year
    # row twice for some indicators, one with (Dim1Type==None,
    # Dim1==None) and one with (Dim1Type=="SEX", Dim1=="BTSX").
    # e.g. MDG_0000000001, MDG_0000000007
    # df[df[['IndicatorCode', 'SpatialDim', 'TimeDim']].duplicated(keep=False)]
    df.drop_duplicates(
        subset=["IndicatorCode", "SpatialDim", "TimeDim"], keep="first", inplace=True
    )

    df.to_csv(os.path.join(INPATH, "data.csv"), index=False)
    logger.info(f"Data succcessfully downloaded to {INPATH}")
