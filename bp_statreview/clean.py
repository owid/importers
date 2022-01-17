"""cleans BP metadata and data points in preparation for MySQL insert.
"""

import os
import re
import simplejson as json
import shutil
from typing import Dict, List
from copy import deepcopy
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
from bp_statreview.unit_conversion import UnitConverter
from bp_statreview.clean_excel import clean_excel_datapoints

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

    df_variables, df_data = clean_variables_and_datapoints(
        dataset_id=df_datasets["id"].iloc[0],
        source_id=df_sources["id"].iloc[0],
        std_entities=True,
    )
    create_datapoints(
        df=df_data,
        var_name2id=df_variables.set_index("name")["id"].to_dict(),
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


def clean_variables_and_datapoints(
    dataset_id: int, source_id: int, std_entities: bool = True
) -> pd.DataFrame:
    """Cleans a dataframe of variables and of datapoints in preparation for
    uploading the sources to the `variables` and `data_values` database tables,
    respectively.

    Arguments:

        dataset_id: int.

        source_id: int.

        std_entities: bool = True. If True, standardizes entity names.

    Returns:

        df_variables, df_data: Tuple[pd.DataFrame, pd.Dataframe].

            df_variables: pd.DataFrame. Cleaned dataframe of variables to be
                uploaded.

            df_data: pd.DataFrame. Cleaned dataframe of data points to be
                uploaded.
    """
    with open(os.path.join(CONFIGPATH, "variables_to_clean.json"), "r") as f:
        variables = json.load(f)["variables"]

    df_data = pd.read_csv(os.path.join(INPATH, "data.csv"))
    assert (
        df_data["Country"].isnull().sum() == 0
    ), "One or more values in the 'Country' column is null."
    assert (
        df_data["Value"].isnull().sum() == 0
    ), "One or more values in the 'Value' column is null."
    assert (
        df_data["Var"].isnull().sum() == 0
    ), "One or more values in the 'Var' column is null."

    var_codes = [var["code"] for var in variables if pd.notnull(var["code"])]
    var_code2name = {
        var["code"]: var["name"] for var in variables if pd.notnull(var["code"])
    }
    df_data = df_data[df_data["Var"].isin(var_codes)]
    df_data["name"] = df_data["Var"].apply(lambda x: var_code2name.get(x))
    assert df_data["name"].notnull().all()

    vars_to_convert = [
        var for var in variables if pd.isnull(var["cleaningMetadata"]["dataSource"])
    ]
    for var in vars_to_convert:
        try:
            rows = None
            root_var_code = var["cleaningMetadata"]["convertFromCode"]
            to_unit = var["shortUnit"]
            root_var = [var for var in variables if var["code"] == root_var_code][0]
            from_unit = root_var["shortUnit"]
            if from_unit.lower() == "mt" and re.search(
                r"\boil\b", root_var["name"], re.IGNORECASE
            ):
                from_unit = "mtoe"
            uc = UnitConverter(frm=from_unit, to=to_unit)
            if uc.can_convert():
                rows = df_data[df_data["Var"] == root_var_code].copy()
                rows.loc[:, "Value"] = uc.convert(rows["Value"].values)
                rows.loc[:, "code"] = var["code"]
                rows.loc[:, "name"] = var["name"]
            assert rows is not None and rows.shape[0] > 0
            df_data = pd.concat([df_data, rows], axis=0)
        except Exception as e:
            logger.error(
                f"Failed to convert data for variable {var['name']}. Error: {e}"
            )

    excel_vars = [
        var for var in variables if var["cleaningMetadata"]["dataSource"] == "excel"
    ]
    for var in excel_vars:
        try:
            rows = clean_excel_datapoints(var)
            rows.loc[:, "code"] = var["code"]
            rows.loc[:, "name"] = var["name"]
            df_data = pd.concat([df_data, rows], axis=0)
        except Exception as e:
            logger.error(
                f"Failed to convert data for variable {var['name']}. Error: {e}"
            )

    # constructs "NaN-filled" variables, which have NaN values filled with
    # non-NaN values using pandas.Series.fillna
    # These variables are used in stacked area charts.
    name2fillna_kwargs = {
        v["name"]: v["cleaningMetadata"]["fillna"]
        for v in variables
        if pd.notnull(v.get("cleaningMetadata", {}).get("fillna"))
    }
    df_data_filled = df_data.query(f"name in @name2fillna_kwargs.keys()").sort_values(
        ["Country", "Year"]
    )
    uniq_dates = df_data_filled["Year"].drop_duplicates().sort_values().tolist()
    df_data_filled = (
        df_data_filled.groupby(["name", "Country"])
        .apply(lambda gp: gp.set_index("Year").reindex(uniq_dates))
        .drop(columns=["name", "Country"])  # gets re-inserted on reset_index()
        .reset_index()
        .sort_values(["name", "Country", "Year"])
    )
    df_data_filled["Value"] = df_data_filled.groupby("name")["Value"].apply(
        lambda gp: gp.fillna(**name2fillna_kwargs[gp.name])
    )
    df_data_filled["name"] = df_data_filled["name"] + " (zero filled)"
    assert df_data_filled["Value"].isnull().sum() == 0
    df_data = pd.concat([df_data, df_data_filled], axis=0)
    assert not df_data.duplicated(subset=["name", "Country", "Year"]).any()

    zero_filled_vars = []
    for var in variables:
        if pd.notnull(var.get("cleaningMetadata", {}).get("fillna")):
            zero_filled_var = deepcopy(var)
            zero_filled_var["name"] = f"{var['name']} (zero filled)"
            if len(zero_filled_var.get("description", "")) > 0:
                zero_filled_var["description"] += (
                    '\n Note: missing data values have been replaced with "0" '
                    "for the purposes of data visualization."
                )
            zero_filled_vars.append(zero_filled_var)
    variables += zero_filled_vars

    df_variables = pd.DataFrame(variables)
    df_timespans = (
        df_data.dropna(subset=["Value"])
        .groupby("name")["Year"]
        .apply(
            lambda gp: f"{gp.dropna().min().astype(int)}-{gp.dropna().max().astype(int)}"
        )
        .reset_index()
        .rename(columns={"Year": "timespan"})
    )
    df_variables = df_variables.merge(
        df_timespans, how="left", on="name", validate="1:1"
    ).sort_values("name")
    assert df_variables["name"].duplicated().sum() == 0
    df_variables["id"] = range(df_variables.shape[0])
    df_variables["dataset_id"] = dataset_id
    df_variables["source_id"] = source_id

    if "display" in df_variables.columns:
        df_variables["display"] = df_variables["display"].apply(
            lambda x: json.dumps(x) if pd.notnull(x) else None
        )

    required_fields = ["id", "name", "description", "dataset_id", "source_id"]
    for field in required_fields:
        assert field in df_variables.columns, f"`{field}` does not exist."
        assert (
            df_variables[field].notnull().all()
        ), f"Every variable must have a non-null `{field}` field."

    df_variables.drop("cleaningMetadata", axis=1, inplace=True)

    if std_entities:
        df_data["Country"] = standardize_entities(df_data["Country"])
    else:
        logger.warning("Entity names have NOT been standardized.")

    assert df_variables["name"].drop_duplicates().isin(df_data["name"].unique()).all()
    assert df_data["name"].drop_duplicates().isin(df_variables["name"].unique()).all()

    return df_variables, df_data


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
    `create_datapoints()` method.

    Returns:

        entities: List[str]. List of distinct entity names, sorted alphabetically.
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

    entities = sorted(entities)
    assert pd.notnull(entities).all(), "All entities should be non-null."
    return entities
