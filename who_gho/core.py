import os
import re
import simplejson as json
import logging
import shutil
from typing import Any, List, Dict
import requests
import pandas as pd
import numpy as np
from pandas.api.types import is_numeric_dtype
from tqdm import tqdm
from bs4 import BeautifulSoup
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
from utils import batchify, camel_case2snake_case

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
        print(row["IndicatorCode"])
        data_filtered = pd.DataFrame(
            df[
                (df.IndicatorCode == row["IndicatorCode"])
                & (df.variable == row["variable"])
            ]
        )
        data_filtered.dropna(subset=["TimeDim"], inplace=True)
        values_to_exclude = ["Not applicable", "Not available"]
        data_filtered = data_filtered[
            ~data_filtered.NumericValue.isin(values_to_exclude)
        ]
        ignored_var_codes = set({})
        if data_filtered.shape[0] == 0:
            ignored_var_codes.add(row["IndicatorCode"])
        else:
            variable = {
                "dataset_id": 0,
                "source_id": 0,
                "id": variable_idx,
                "name": row["variable"],
                "description": var_code2meta[row["IndicatorCode"]],
                "code": row["IndicatorCode"],
                "unit": None,
                "short_unit": None,
                "timespan": "%s - %s"
                % (
                    int(np.min(data_filtered["TimeDim"])),
                    int(np.max(data_filtered["TimeDim"])),
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
        f"Saved data points to csv for {i} variables. Excluded {len(ignored_var_codes)} variables."
    )


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

    var_df = []
    for var in variables:
        print(var)
        df = pd.read_csv(f"{INPATH}/{var}.csv")

        df["indicator_name"] = df["IndicatorCode"].apply(
            lambda x: var_code2name[x] if x in var_code2name else None
        )
        df["variable"] = create_var_name(df, dim_values, dim_dict)
        var_df.append(df)

    var_df = pd.concat(var_df)

    var_df = var_df[
        [
            "IndicatorCode",
            "SpatialDim",
            "TimeDim",
            "DataSourceDimType",
            "DataSourceDim",
            "NumericValue",
            "variable",
        ]
    ]

    return var_df


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


def clean_and_create_datapoints(
    variable_codes: List[str], entity2owid_name: Dict[str, str]
) -> Dict[str, dict]:
    """Cleans all entity-variable-year data observations and saves all
    data points to csv in the `{OUTPATH}/datapoints` directory.

    The data for each variable is saved as a separate csv file.

    Arguments:

        variable_codes: List[str]. List of variable codes for which to download
            WHO GHO data. Example:

            ["MDG_0000000017", "WHS4_100", ...]

        entity2owid_name: Dict[str, str]. Dict of
            "{UNSTANDARDIZED_ENTITY_NAME}" -> "{STANDARDIZED_OWID_NAME}"
            key-value mappings. Example:

            {"AFG": "Afghanistan", "SSF": "Sub-Saharan Africa", ...}

    Returns:

        var_code2meta: Dict[str, Dict]. Dict where keys are variable codes and
            values are dict representing some basic metadata derived from the
            data points. The metadata dicts also contain a temporary ID
            assigned to the variable that matches it to the corresponding csv
            that has just been created in `{OUTPATH}/datapoints` is Example:

            {'HIV_0000000006': {'id': 0, 'timespan': '1990-2019'}, ...}


    """

    for var in variable_codes:
        # loads data
        df_data = pd.read_csv(
            f"{INPATH}/{var}.csv",
            usecols=[
                "SpatialDim",
                "SpatialDimType",
                "TimeDim",
                "IndicatorCode",
                "TimeDim",
                "TimeDimType",
                "Dim1",
                "Dim1Type",
                "NumericValue",
            ],
        )
    df_data.columns = df_data.columns.str.lower().str.replace(
        r"[\s/-]+", "_", regex=True
    )

    assert all(df_data["timedimtype"].dropna() == "YEAR")
    df_data.rename(
        columns={
            "timedim": "year",
            "indicatorcode": "nonowid_id",
            "numericvalue": "value",
        },
        inplace=True,
    )

    # subsets data to variable codes to be cleaned.
    df_data = df_data[df_data["nonowid_id"].isin(variable_codes)]

    # standardizes entity names.
    df_data["country"] = df_data["spatialdim"].apply(
        lambda x: entity2owid_name[x] if x in entity2owid_name else None
    )

    out_path = os.path.join(OUTPATH, "datapoints")
    if not os.path.exists(out_path):
        os.makedirs(out_path)

    # df_data.dim1type.value_counts()
    # cleans each variable and saves it to csv.
    df_data.dropna(
        subset=["nonowid_id", "country", "year", "value"], how="any", inplace=True
    )
    df_data["year"] = df_data["year"].astype(int)
    df_data.sort_values(
        by=["nonowid_id", "country", "year", "dim1type", "dim1"], inplace=True
    )

    grouped = df_data.groupby(["nonowid_id"])  # 'dim1type', 'dim1'
    i = 0
    ignored_var_codes = set({})
    # kept_var_codes = set({})
    var_code2meta = {}
    logger.info("Saving data points for each variable to csv...")
    for var_code, gp in tqdm(grouped, total=len(grouped)):
        # print(var_code, dim1type, dim1val)
        # if (dim1type or dim1val) and (dim1type != "SEX" and dim1val != "BTSX"):
        #     raise NotImplementedError
        gp_temp = gp[["country", "year", "value"]]
        assert not gp_temp.duplicated(subset=["country", "year"]).any()
        assert is_numeric_dtype(gp_temp["value"])
        assert is_numeric_dtype(gp_temp["year"])
        assert gp_temp.notnull().all().all()
        if gp_temp.shape[0] == 0:
            ignored_var_codes.add(var_code)
        else:
            # kept_var_codes.add(var_code)
            assert var_code not in var_code2meta
            timespan = f"{int(gp_temp['year'].min())}-{int(gp_temp['year'].max())}"
            var_code2meta[var_code] = {"id": i, "timespan": timespan}
            fpath = os.path.join(out_path, f"datapoints_{i}.csv")
            assert not os.path.exists(fpath), (
                f"{fpath} already exists. This should not be possible, because "
                "each variable is supposed to be assigned its own unique "
                "file name."
            )
            gp_temp.to_csv(fpath, index=False)
            i += 1

    logger.info(
        f"Saved data points to csv for {i} variables. Excluded {len(ignored_var_codes)} variables."
    )
    return var_code2meta


def clean_variables_bob(
    variables: List[dict],
    var_code2meta: Dict[str, dict],
) -> pd.DataFrame:

    assert all(
        [pd.notnull(variable["code"]) for variable in variables]
    ), "One or more variables has a null `code` field."
    missing_var_codes = set([var["code"] for var in variables]).difference(
        var_code2meta.keys()
    )
    assert len(missing_var_codes) == 0, (
        "The following variable codes are not in `var_code2meta`: "
        f"{missing_var_codes}"
    )

    # adds the variable metadata from `var_code2meta` to the variable metadata
    # in `variables.`
    for variable in variables:
        meta = var_code2meta[variable["code"]]
        for field in meta:
            if field in variable and variable[field]:
                logger.warning(
                    f"The `{field}` field for variable {variable['code']} is "
                    f"being overwritten. Existing value: {variable[field]}; "
                    f"New value: {meta[field]}."
                )
            variable[field] = meta[field]

    # converts the "originalMetadata" and "display" json fields to strings
    # for variable in variables:
    #     for field in ['originalMetadata', 'display']:
    #         if field in variable:
    #             variable[field] = json.dumps(variable[field], ignore_nan=True)

    df_variables = pd.DataFrame(variables)

    json_fields = ["display", "originalMetadata"]
    for field in json_fields:
        df_variables[field] = df_variables[field].apply(
            lambda x: json.dumps(x, ignore_nan=True) if pd.notnull(x) else None
        )

    # fetches description for each variable.
    df_variables["description"]

    ## Add in json template here

    code2desc = _fetch_description_many_variables(df_variables.code.tolist())

    # cleans variable names.
    df_variables["name"] = df_variables["name"].str.replace(r"\s+", " ", regex=True)

    df_variables["dataset_id"] = dataset_id
    df_variables["source_id"] = source_id

    # converts column names to snake case b/c this is what is expected in the
    # `standard_importer.import_dataset` module.
    df_variables.columns = df_variables.columns.map(camel_case2snake_case)

    required_fields = ["id", "name"]
    for field in required_fields:
        assert (
            df_variables[field].notnull().all()
        ), f"Every variable must have a non-null `{field}` field."

    df_variables = df_variables.set_index(["id", "name"]).reset_index()
    return df_variables


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
