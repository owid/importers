import os
import json
from typing import List
from pathlib import Path
import requests
import numpy as np
import logging
import pandas as pd
import shutil
from who_gho import (
    CONFIGPATH,
    DELETE_EXISTING_INPUTS,
    DOWNLOAD_INPUTS,
    INPATH,
    OUTPATH,
    KEEP_PATHS,
    CURRENT_DIR,
    SELECTED_VARS_ONLY,
)

from who_gho.core import get_variable_codes

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
    dimensions = [
        "country",
        "region",
        "GBDREGION",
        "MGHEREG",
        "UNREGION",
        "UNSDGREGION",
        "WHOINCOMEREGION",
        "WORLDBANKREGION",
        "WORLDBANKINCOMEGROUP",
    ]
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

    variable_codes = get_variable_codes(selected_vars_only)

    for ind_code in variable_codes:
        print(f"{ind_code}", end="", flush=True)
        data = get_ind_data(ind_code)
        data.to_csv(f"{INPATH}/{ind_code}.csv")
    return variable_codes


def get_ind_data(code: str) -> pd.DataFrame:
    data_url = "https://ghoapi.azureedge.net/api/{code}"
    url = data_url.format(code=code)
    data_json = requests.get(url).json()
    data_df = pd.DataFrame.from_records(data_json["value"]).set_index("Id")
    return data_df
