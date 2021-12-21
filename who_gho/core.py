import os
import re
import simplejson as json
import logging
import shutil
from typing import List
import requests
import pandas as pd
import numpy as np
from tqdm import tqdm
from bs4 import BeautifulSoup

import pyarrow as pa
import pyarrow.parquet as pq
from who_gho import (
    CONFIGPATH,
    DOWNLOAD_INPUTS,
    CURRENT_DIR,
    INPATH,
    OUTPATH,
    DATASET_AUTHORS,
    DATASET_NAME,
    DATASET_VERSION,
    DATASET_LINK,
    DATASET_RETRIEVED_DATE,
    SELECTED_VARS_ONLY,
)

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def delete_input(inpath: str) -> None:
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
    if os.path.exists(inpath):
        shutil.rmtree(inpath)
    logger.info(f"Deleted all existing input files in {inpath}")


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


def delete_output(keep_paths: List[str], outpath: str) -> None:
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
        if os.path.exists(os.path.join(outpath, path)):
            os.rename(os.path.join(outpath, path), os.path.join(outpath, "..", path))
    # deletes all remaining output files
    if os.path.exists(outpath):
        shutil.rmtree(outpath)
        os.makedirs(outpath)
    # moves the exception files back into the output directory.
    for path in keep_paths:
        if os.path.exists(os.path.join(outpath, "..", path)):
            os.rename(os.path.join(outpath, "..", path), os.path.join(outpath, path))


def clean_datasets() -> pd.DataFrame:
    """Constructs a dataframe where each row represents a dataset to be
    upserted.

    Note: often, this dataframe will only consist of a single row.

    Returns:

        df: pd.DataFrame. Dataframe where each row represents a dataset to be
            upserted. Example:

            id                                               name
        0   0  Global Health Observatory - World Health Organ...

    """
    data = [
        {"id": 0, "name": f"{DATASET_NAME} - {DATASET_AUTHORS} ({DATASET_VERSION})"}
    ]
    df = pd.DataFrame(data)
    return df


def clean_sources(dataset_id: int, dataset_name: str) -> pd.DataFrame:
    """Cleans data sources in preparation for uploading the sources to
    the `sources` database table.

    Arguments:

        dataset_id: int. Integer representing the dataset idx for all variables
            and sources.

        dataset_name: str. Dataset name.

    Returns:

        df_sources: pd.DataFrame. Cleaned Dataframe of data sources to be
            uploaded.

    """
    sources = [
        {
            "dataset_id": dataset_id,
            "name": dataset_name,
            "description": json.dumps(
                {
                    "link": DATASET_LINK,
                    "retrievedDate": DATASET_RETRIEVED_DATE,
                    "additionalInfo": None,
                    "dataPublishedBy": dataset_name,
                    "dataPublisherSource": None,
                },
                ignore_nan=True,
            ),
            "id": 0,
        }
    ]
    df_sources = pd.DataFrame(sources)

    return df_sources


def get_dimensions():
    dims_url = "https://ghoapi.azureedge.net/api/Dimension"
    resp = requests.get(dims_url)
    dim_json = resp.json()
    dim_names = pd.DataFrame.from_records(dim_json["value"]).set_index("Code")

    dim_dict = dim_names.squeeze().to_dict()

    dim_values = get_dim_values(dim_names)
    return dim_dict, dim_values


def get_dim_values(dims: pd.DataFrame):
    dim_val_url = "https://ghoapi.azureedge.net/api/DIMENSION/{code}/DimensionValues"
    dim_values_frames = []
    for code in dims.index:
        url = dim_val_url.format(code=code)
        value_json = requests.get(url).json()["value"]
        value_df = pd.DataFrame.from_records(value_json)
        dim_values_frames.append(value_df)

    dim_values = pd.concat(dim_values_frames)
    return dim_values


def add_missing_dims(dim_val_dict: dict):
    dim_val_dict["NGO_REHABILITATION"] = "Rehabilitation"
    dim_val_dict["NGO_ADVOCACY"] = "Advocacy"
    dim_val_dict["NGO_TREATMENT"] = "Treatment"
    dim_val_dict["NGO_PREVENTION"] = "Prevention"
    dim_val_dict["DROPIN_SERVICES"] = '"Drop-in" services'
    dim_val_dict["WHO_TOTL"] = "WHO Total"
    return dim_val_dict


def create_var_name(df: pd.DataFrame, dim_values: pd.DataFrame, dim_dict: dict):

    dims = df[["Dim1Type", "Dim2Type", "Dim3Type"]].drop_duplicates().stack().tolist()

    dim_val_dict = (
        dim_values[dim_values["Dimension"].isin(dims)]
        .set_index("Code")["Title"]
        .to_dict()
    )

    if any(x in dims for x in ["NGO", "PROGRAMME", "WEALTHQUINTILE"]):
        dim_val_dict = add_missing_dims(dim_val_dict)

    df[["Dim1m", "Dim2m", "Dim3m"]] = df[["Dim1", "Dim2", "Dim3"]].applymap(
        dim_val_dict.get
    )

    df[["Dim1Typem", "Dim2Typem", "Dim3Typem"]] = df[
        ["Dim1Type", "Dim2Type", "Dim3Type"]
    ].applymap(dim_dict.get)

    cols = ["Dim1Typem", "Dim2Typem", "Dim3Typem"]
    df[cols] = " - " + df[cols] + ":"

    col_com = [
        "indicator_name",
        "Dim1Typem",
        "Dim1m",
        "Dim2Typem",
        "Dim2m",
        "Dim3Typem",
        "Dim3m",
    ]

    df["variable_name"] = "Indicator:" + df[col_com].fillna("").sum(axis=1)

    # Check all of the dim types have an associated value
    end_check = (":", " - ")

    # df[df["variable_name"].str.endswith(end_check)]

    assert df["variable_name"].str.endswith(end_check).sum() == 0

    return df["variable_name"]


def clean_variables(df: pd.DataFrame, var_code2meta: dict):

    all_series = (
        df[["IndicatorCode", "variable"]].drop_duplicates().reset_index(drop=True)
    )

    variable_idx = 0
    variables = pd.DataFrame()
    for i, row in tqdm(all_series.iterrows(), total=len(all_series)):
        print(row["variable"])
        data_filtered = pd.DataFrame(
            df[
                (df.IndicatorCode == row["IndicatorCode"])
                & (df.variable == row["variable"])
            ]
        )

        values_to_exclude = ["Not applicable", "Not available"]
        data_filtered = data_filtered[
            ~data_filtered.NumericValue.isin(values_to_exclude)
        ]

        ignored_var_codes = set({})
        if data_filtered.shape[0] == 0:
            ignored_var_codes.add(row["IndicatorCode"])
        else:
            unit_var, short_unit_var = get_unit(row["variable"])
            variable = {
                "dataset_id": 0,
                "source_id": 0,
                "id": variable_idx,
                "name": row["variable"],
                "description": var_code2meta[row["IndicatorCode"]],
                "code": row["IndicatorCode"],
                "unit": unit_var,
                "short_unit": short_unit_var,
                "timespan": "%s - %s"
                % (
                    int(float(np.min(data_filtered["TimeDim"]))),
                    int(float(np.max(data_filtered["TimeDim"]))),
                ),
                "coverage": None,
                "display": None,
                "original_metadata": None,
            }
            variables = variables.append(variable, ignore_index=True)
            extract_datapoints(data_filtered).to_csv(
                os.path.join(OUTPATH, "datapoints", "datapoints_%d.csv" % variable_idx),
                index=False,
            )
            variable_idx += 1
            print(variable_idx)
    logger.info(
        f"Saved data points to csv for {variable_idx} variables. Excluded {len(ignored_var_codes)} variables."
    )
    return variables


def get_unit(var: str) -> List[str]:
    unit = "(%)"
    if unit in var:
        unit_out = "Percentage"
        short_unit_out = "%"
    else:
        unit_out = None
        short_unit_out = None
    return unit_out, short_unit_out


def extract_datapoints(df: pd.DataFrame) -> pd.DataFrame:

    df_out = pd.DataFrame(
        {
            "country": df["country"],
            "year": df["TimeDim"],
            "value": df["NumericValue"],
        }
    )

    df_out = df_out[df_out["value"].notna()]
    return df_out


def load_all_data_and_add_variable_name(
    variables: list, var_code2name: dict
) -> pd.DataFrame:

    dim_dict, dim_values = get_dimensions()

    spatial_dim_types_exclude = [
        "DHSMICSGEOREGION"
    ]  # Regional data we don't yet have capacity to use

    spatial_dims_to_exclude = [
        "SDG_LAMRC",
        "SDG_SSAFR",
        "WHO_GLOBAL",
        "REGION_WB_LI",
        "REGION_ WB_LI",
        "REGION_ WB_LMI",
        "REGION_ WB_UMI",
        "REGION_ WB_HI",
    ]  # spatial dims in the data that do not have aliases in the API e.g. REGION_WB_LI is not in  https://ghoapi.azureedge.net/api/DIMENSION/Region/DimensionValues

    var_list = []
    for var in variables:
        var_path = f"{INPATH}/{var}.csv"
        var_list.append(var_path)

    csv_to_parquet(var_list)
    main_df = pd.read_parquet(os.path.join(INPATH, "df_combined.parquet"))
    main_df["indicator_name"] = main_df["IndicatorCode"].apply(
        lambda x: var_code2name[x] if x in var_code2name else None
    )
    main_df["variable"] = create_var_name(main_df, dim_values, dim_dict)
    var_df = main_df[
        [
            "IndicatorCode",
            "SpatialDim",
            "SpatialDimType",
            "TimeDim",
            "DataSourceDimType",
            "DataSourceDim",
            "NumericValue",
            "variable",
        ]
    ]

    var_df = var_df[~var_df.SpatialDimType.isin(spatial_dim_types_exclude)]
    var_df = var_df[~var_df.SpatialDim.isin(spatial_dims_to_exclude)]

    assert var_df[var_df.SpatialDimType.isin(spatial_dim_types_exclude)].shape[0] == 0
    assert var_df[var_df.SpatialDim.isin(spatial_dims_to_exclude)].shape[0] == 0

    var_df = var_df[var_df["SpatialDimType"].notna()]

    return var_df


def csv_to_parquet(files: iter) -> None:
    chunksize = 1000000  # this is the number of lines
    pqwriter = None
    j = 0
    for file in files:
        print(file)
        for i, df in enumerate(
            pd.read_csv(
                file,
                chunksize=chunksize,
                usecols=[
                    "IndicatorCode",
                    "SpatialDimType",
                    "SpatialDim",
                    "TimeDimType",
                    "TimeDim",
                    "Dim1Type",
                    "Dim1",
                    "Dim2Type",
                    "Dim2",
                    "Dim3Type",
                    "Dim3",
                    "DataSourceDimType",
                    "DataSourceDim",
                    "Value",
                    "NumericValue",
                ],
                dtype={
                    "IndicatorCode": object,
                    "SpatialDimType": object,
                    "SpatialDim": object,
                    "TimeDimType": object,
                    "TimeDim": object,
                    "Dim1Type": object,
                    "Dim1": object,
                    "Dim2Type": object,
                    "Dim2": object,
                    "Dim3Type": object,
                    "Dim3": object,
                    "DataSourceDimType": object,
                    "DataSourceDim": object,
                    "Value": object,
                    "NumericValue": object,
                },
            ),
        ):

            fields = [
                pa.field("IndicatorCode", pa.string()),
                pa.field("SpatialDimType", pa.string()),
                pa.field("SpatialDim", pa.string()),
                pa.field("TimeDimType", pa.string()),
                pa.field("TimeDim", pa.string()),
                pa.field("Dim1Type", pa.string()),
                pa.field("Dim1", pa.string()),
                pa.field("Dim2Type", pa.string()),
                pa.field("Dim2", pa.string()),
                pa.field("Dim3Type", pa.string()),
                pa.field("Dim3", pa.string()),
                pa.field("DataSourceDimType", pa.string()),
                pa.field("DataSourceDim", pa.string()),
                pa.field("Value", pa.string()),
                pa.field("NumericValue", pa.string()),
            ]

            my_schema = pa.schema(fields)
            table = pa.Table.from_pandas(df, schema=my_schema, preserve_index=False)
            # for the first chunk of records
            if j == 0:
                # create a parquet write object giving it an output file
                pqwriter = pq.ParquetWriter(
                    os.path.join(INPATH, "df_combined.parquet"), table.schema
                )
            pqwriter.write_table(table)
        j += 1

    if pqwriter:
        pqwriter.close()


def standardise_country_name(country_col: pd.Series):
    # loads mapping of "{UNSTANDARDIZED_ENTITY_CODE}" -> "{STANDARDIZED_OWID_NAME}"
    # i.e. {"AFG": "Afghanistan", "SSF": "Sub-Saharan Africa", ...}
    entity2owid_name = (
        pd.read_csv(os.path.join(CONFIGPATH, "standardized_entity_names.csv"))
        .set_index("Code")["Our World In Data Name"]
        .squeeze()
        .to_dict()
    )

    country_col_owid = country_col.apply(
        lambda x: entity2owid_name[x] if x in entity2owid_name else None
    )
    return country_col_owid


def get_distinct_entities() -> List[str]:
    """retrieves a list of all distinct entities that contain at least
    on non-null data point that was saved to disk from the
    `clean_and_create_datapoints()` method.

    Returns:

        entities: List[str]. List of distinct entity names.
    """
    fnames = [
        fname
        for fname in os.listdir(os.path.join(OUTPATH, "datapoints"))
        if fname.endswith(".csv")
    ]
    entity_set = set({})
    for fname in fnames:
        df_temp = pd.read_csv(os.path.join(OUTPATH, "datapoints", fname))
        entity_set.update(df_temp["country"].unique().tolist())

    entities = sorted(entity_set)
    assert pd.notnull(entities).all(), (
        "All entities should be non-null. Something went wrong in "
        "`clean_and_create_datapoints()`."
    )
    return entities


def get_metadata_url() -> pd.DataFrame:
    url_json = requests.get(
        "https://apps.who.int/gho/athena/api/GHO/?format=json"
    ).json()

    ind_codes = pd.json_normalize(url_json, record_path=["dimension", "code"])[
        "label"
    ].to_list()
    ind_name = pd.json_normalize(url_json, record_path=["dimension", "code"])[
        "display"
    ].to_list()

    urls = pd.json_normalize(url_json, record_path=["dimension", "code"])[
        "url"
    ].to_list()

    assert len(ind_codes) == len(ind_name) == len(urls)

    url_dict = zip(ind_codes, urls)
    url_dict = dict(url_dict)

    name_dict = zip(ind_codes, ind_name)
    name_dict = dict(name_dict)

    return url_dict, name_dict


def get_variable_codes(selected_vars_only: bool) -> pd.DataFrame:
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

    return variable_codes


def get_metadata(var_code2url: dict) -> dict:
    if not os.path.isfile(os.path.join(CONFIGPATH, "variable_metadata.json")):
        indicators = get_variable_codes(selected_vars_only=SELECTED_VARS_ONLY)
        descs = {}
        for name in indicators:
            print(name)
            url = var_code2url[name]
            print(url)
            if url.startswith("http"):
                desc = _fetch_description_one_variable(url)
                descs[name] = desc
            else:
                descs[name] = ""
            with open(os.path.join(CONFIGPATH, "variable_metadata.json"), "w") as fp:
                json.dump(descs, fp, indent=2)
    else:
        with open(os.path.join(CONFIGPATH, "variable_metadata.json")) as fp:
            descs = json.load(fp)
    return descs


def _fetch_description_one_variable(url: str) -> str:
    """Fetches the description for one variable.

    Arguments:

        url: str. URL where a variable's description is available.

    Returns:

        text: str. String representing the variable's description.
    """
    headings_to_use = [
        "rationale",
        "definition",
        "method of measurement",
        "method of estimation",
    ]

    try:
        r = requests.get(url)
        soup = BeautifulSoup(r.content, features="lxml")
        divs = soup.find_all("div", {"class": "metadata-box"})
        text = ""
        for div in divs:
            heading_text = re.sub(
                r":$",
                "",
                div.find("div", {"class": "metadata-title"}).text.strip().lower(),
            )
            if heading_text in headings_to_use:
                text += f"\n\n{heading_text.capitalize()}: {div.find(text=True, recursive=False).strip()}"
        text = text.strip()
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        print(e)
        text = ""
    return text


def _clean_datapoints_custom(df: pd.DataFrame) -> pd.DataFrame:
    """custom logic for cleaning specific variables.

    Arguments:

        df: pd.DataFrame. Dataframe containing all datapoints to be cleaned.

    Returns:

        df: pd.DataFrame. Same dataframe as input, after custom data cleaning
            has been applied.
    """
    # WHOSIS_000001: Life expectancy at birth (years).
    # There is one data point for Canada in 1920 that is clearly a
    # mistake. Canada's life expectancy in 1920 was not 82.81! And no
    # other countries have data for this year (the earliest data point
    # is otherwise for 2000), so this seems to be an error.
    nonowid_id = "WHOSIS_000001"
    drop = (
        (df["nonowid_id"] == nonowid_id)
        & (df["country"] == "Canada")
        & (df["year"] == 1920)
    ).values
    df = df[~drop]

    return df
