"""cleans BP metadata and data points in preparation for MySQL insert.
"""

import os
import simplejson as json
import shutil
from typing import Dict, List
from pandas.core.dtypes.common import is_numeric_dtype
import pandas as pd
from dotenv import load_dotenv

from bp_statreview import (
    DATASET_NAME,
    DATASET_AUTHORS,
    DATASET_VERSION,
    DATASET_LINK,
    DATASET_RETRIEVED_DATE,
    CONFIGPATH,
    INPATH,
    OUTPATH,
)

import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()

# KEEP_PATHS: Names of files in `{DATASET_DIR}/output` that you do NOT
# want deleted in the beginning of this script.
KEEP_PATHS = []


def main() -> None:

    delete_output(KEEP_PATHS)
    mk_output_dir()

    df_datasets = clean_datasets()
    assert (
        df_datasets.shape[0] == 1
    ), f"Only expected one dataset in {os.path.join(OUTPATH, 'datasets.csv')}."

    df_sources = clean_sources(
        dataset_name=df_datasets.squeeze().to_dict()["name"],
        dataset_id=df_datasets.squeeze().to_dict()["id"],
    )
    assert (
        df_sources.shape[0] == 1
    ), f"Only expected one source in {os.path.join(OUTPATH, 'sources.csv')}."

    )

    clean_and_create_datapoints(
        var_code2id=df_variables.set_index("code")["id"].to_dict(),
        entity2owid_name=entity2owid_name,
    )

    df_distinct_entities = pd.DataFrame(get_distinct_entities(), columns=["name"])

    # saves datasets, sources, variables, and distinct entities to disk.
    df_datasets.to_csv(os.path.join(OUTPATH, "datasets.csv"), index=False)
    df_sources.to_csv(os.path.join(OUTPATH, "sources.csv"), index=False)
    df_variables.to_csv(os.path.join(OUTPATH, "variables.csv"), index=False)
    df_distinct_entities.to_csv(
        os.path.join(OUTPATH, "distinct_countries_standardized.csv"), index=False
    )


def delete_output(keep_paths: List[str]) -> None:
    """deletes all files in `{DATASET_DIR}/output` EXCEPT for any file
    names in `keep_paths`.

    Arguments:

        keep_paths: List[str]. List of subpaths in `{DATASET_DIR}/output` that
            you do NOT want deleted. They will be temporarily move to `{DATASET_DIR}`
            and then back into `{DATASET_DIR}/output` after everything else in
            `{DATASET_DIR}/output` has been deleted.

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


def mk_output_dir() -> None:
    """creates output directory, if it does not already exist."""
    if not os.path.exists(OUTPATH):
        os.makedirs(OUTPATH)


def clean_datasets() -> pd.DataFrame:
    """Constructs a dataframe where each row represents a dataset to be upserted.

    Returns:

        pd.DataFrame. Cleaned dataframe of datasets to be uploaded.
    """
    data = [
        {"id": 0, "name": f"{DATASET_NAME} - {DATASET_AUTHORS} ({DATASET_VERSION})"}
    ]
    return pd.DataFrame(data)


def clean_sources(dataset_name: str, dataset_id: int) -> pd.DataFrame:
    """Cleans a dataframe of data sources in preparation for uploading the
    sources to the `sources` database table.

    Arguments:

        dataset_name: str. Dataset name.

        dataset_id: int. Temporary dataset id.

    Returns:

        df_sources: pd.DataFrame. Cleaned dataframe of data sources
            to be uploaded.
    """
    sources = [
        {
            "id": 0,
            "dataset_id": dataset_id,
            "name": dataset_name,
            "description": json.dumps(
                {
                    "link": DATASET_LINK,
                    "retrievedDate": DATASET_RETRIEVED_DATE,
                    # "additionalInfo": additional_info,
                    "dataPublishedBy": DATASET_AUTHORS,
                    "dataPublisherSource": DATASET_NAME,
                },
                ignore_nan=True,
            ),
        }
    ]
    return pd.DataFrame(sources)


def clean_variables(dataset_id: int, source_id: int) -> pd.DataFrame:
    """Cleans a dataframe of variables in preparation for uploading the
    sources to the `variables` database table.

    Arguments:

        dataset_id: int.
        source_id: int.

    Returns:

        df_variables: pd.DataFrame. Cleaned dataframe of variables to be
            uploaded.
    """
    with open(os.path.join(CONFIGPATH, "variables_to_clean.json"), "r") as f:
        df_variables = pd.DataFrame(json.load(f)["variables"])

    df = pd.read_csv(os.path.join(INPATH, "data.csv"))

    df_timespans = (
        df.dropna(subset=["Value"])
        .groupby("Var")["Year"]
        .apply(
            lambda gp: f"{gp.dropna().min().astype(int)}-{gp.dropna().max().astype(int)}"
        )
        .reset_index()
        .rename(columns={"Year": "timespan", "Var": "code"})
    )

    df_variables = df_variables.merge(
        df_timespans, how="left", on="code", validate="1:1"
    )
    df_variables["id"] = range(df_variables.shape[0])
    df_variables["dataset_id"] = dataset_id
    df_variables["source_id"] = source_id

    df_variables["description"] = ""

    required_fields = ["id", "name", "description", "dataset_id", "source_id"]
    for field in required_fields:
        assert field in df_variables.columns, f"`{field}` does not exist."
        assert (
            df_variables[field].notnull().all()
        ), f"Every variable must have a non-null `{field}` field."

    return df_variables


def create_datapoints(df: pd.DataFrame, var_name2id: Dict[str, int]) -> None:
    """Cleans all entity-variable-year data observations and saves all
    data points to csv in the `{OUTPATH}/datapoints` directory.

    The data for each variable is saved as a separate csv file.

    Arguments:

        var_name2id: Dict[str, int]. Mapping of variable name to temporary
            variable id.

                {"Wind Consumption - TWh": 0, ...}

    Returns:

        None.

    """
    out_path = os.path.join(OUTPATH, "datapoints")
    if not os.path.exists(out_path):
        os.makedirs(out_path)

    for var_name, gp in df.groupby("name"):
        gp_tmp = gp[["Country", "Year", "Value"]].dropna()
        fpath = os.path.join(out_path, f"datapoints_{var_name2id[var_name]}.csv")

        # sanity checks
        assert not gp_tmp.duplicated(subset=["Country", "Year"]).any()
        assert is_numeric_dtype(gp_tmp["Value"])
        assert is_numeric_dtype(gp_tmp["Year"])
        assert gp_tmp.shape[0] > 0
        assert gp_tmp.notnull().all().all()
        assert not os.path.exists(fpath), (
            f"{fpath} already exists. This should not be possible, because "
            "each variable is supposed to be assigned its own unique "
            "file name."
        )

        # saves datapoints to disk.
        gp_tmp.columns = gp_tmp.columns.str.lower()
        gp_tmp.to_csv(fpath, index=False)


def standardize_entities(entities: List[str]) -> List[str]:
    """standardizes entity names."""
    # loads mapping of "{UNSTANDARDIZED_ENTITY_NAME}" -> "{STANDARDIZED_OWID_NAME}"
    # i.e. {"Afghanistan": "Afghanistan", "Total Africa": "Africa", ...}
    entity2owid_name = (
        pd.read_csv(os.path.join(CONFIGPATH, "standardized_entity_names.csv"))
        .set_index("Country")
        .squeeze()
        .to_dict()
    )
    entities_std = [entity2owid_name[ent] for ent in entities]
    return entities_std


def get_distinct_entities() -> List[str]:
    """retrieves a list of all distinct entities that contain at least
    on non-null data point that was saved to disk during the
    `clean_and_create_datapoints()` method.

    Returns:

        entities: List[str]. List of distinct entity names.
    """
    fnames = [
        fname
        for fname in os.listdir(os.path.join(OUTPATH, "datapoints"))
        if fname.endswith(".csv")
    ]
    entities = set({})
    for fname in fnames:
        df_temp = pd.read_csv(os.path.join(OUTPATH, "datapoints", fname))
        entities.update(df_temp["country"].unique().tolist())

    entities = list(entities)
    assert pd.notnull(entities).all(), "All entities should be non-null."
    return entities
