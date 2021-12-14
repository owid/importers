from asyncio.futures import Future
import os
import re
import time
import simplejson as json
import logging
import shutil
from typing import Any, List, Dict
import asyncio
from aiohttp import ClientSession
from pathlib import Path
import requests
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype
from tqdm import tqdm
from dotenv import load_dotenv
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


def download_data(variable_codes: List[str]) -> None:
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
    # loads data
    df_data = pd.read_csv(
        os.path.join(INPATH, "data.csv"),
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
    # df_data = df_data[df_data['SpatialDimType'].isin(["COUNTRY","REGION"])]
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

    df_data = _clean_datapoints_custom(df_data)

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


def clean_variables(
    dataset_id: int,
    source_id: int,
    variables: List[dict],
    var_code2meta: Dict[str, dict],
) -> pd.DataFrame:
    """Cleans a dataframe of variables in preparation for uploading the
    variables to the `variables` database table.


    Arguments:

        dataset_id: int. Integer representing the dataset id for all variables.

        source_id: int. Integer representing the data source id for all variables.

        variables: List[dict]. List of variables to clean. Example:

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

        var_code2meta: Dict[str, dict]. Dict of `variable code` -> `{variable meta}`
            mappings. Contains some metadata for each variable that was
            constructed during the `clean_and_create_datapoints` step. All
            variable codes in `variables` MUST have a corresponding key in
            `var_cod2meta`. Example:

                {"MDG_0000000001": {"id": 0, "timespan": "2000-2019"}, ...}

    Returns:

        df_variables: pd.DataFrame. Cleaned dataframe of variables
            to be uploaded.
    """
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
    df_variables["description"] = _fetch_description_many_variables(
        df_variables.code.tolist()
    )

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


def clean_sources(dataset_id: int, dataset_name: str) -> pd.DataFrame:
    """Cleans data sources in preparation for uploading the sources to
    the `sources` database table.

    Arguments:

        dataset_id: int. Integer representing the dataset id for all variables
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


def _fetch_data_many_variables(variable_codes: List[str]) -> List[pd.DataFrame]:
    """Fetches datapoints for many WHO GHO variables asynchronously.

    Arguments:

        variable_codes: List[str]. List of variable codes for which to download
            WHO GHO data. Example:

            ["MDG_0000000017", "WHS4_100", ...]

    Returns:

        dataframes: List[pd.DataFrame]. List of pandas dataframes, where each
            dataframe contains the datapoints for a single variable.
    """
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    try:
        future = asyncio.ensure_future(_gather_fetch_tasks(variable_codes, loop))  # type: ignore
        res_jsons = loop.run_until_complete(future)
        dataframes = [pd.DataFrame(res_json["value"]) for res_json in res_jsons]
    finally:
        pass
        # loop.close()
    # synchronous code, for comparison.
    # ~200 seconds for 100 requests (synchronous) vs. ~25 seconds for 100 requests (asynchronous)
    # dataframes = []
    # params = {"$filter": 'SpatialDimType in ("COUNTRY","REGION")'}
    # for code in variable_codes:
    #     resp = requests.get(f"https://ghoapi.azureedge.net/api/{code}", params=params)
    #     df = pd.DataFrame(json.loads(resp.content)['value'])
    #     dataframes.append(df)
    return dataframes


async def _gather_fetch_tasks(
    variable_codes: List[str], loop: asyncio.unix_events._UnixSelectorEventLoop
) -> "Future[Any]":
    """Utility method for

    Arguments:

        variable_codes: List[str]. List of variable codes for which to download
            WHO GHO data. Example:

            ["MDG_0000000017", "WHS4_100", ...]

        loop: asyncio.unix_events._UnixSelectorEventLoop. Asynchronous Event
            loop. e.g. `loop = asyncio.get_event_loop()`.

    Returns:

        asyncio.Future.
    """
    async with ClientSession() as session:
        async_tasks = []
        for code in variable_codes:
            task = loop.create_task(_fetch_data_one_variable(code, session))
            # task = asyncio.ensure_future(_async_get_one_indicator_data(code, session))
            # task = get_case_details_async(state, year, case, session=session)
            async_tasks.append(task)
        return await asyncio.gather(*async_tasks)


async def _fetch_data_one_variable(code: str, session: ClientSession = None) -> dict:
    """Fetches data points for one variable, asynchronously.

    Arguments:

        code: str. WHO GHO variable code.

        session: ClientSession = None.

    Returns:

        res_json: dict. Dict containing the WHO GHO variable data.
    """
    if session is None:
        session = ClientSession()
    res_json = None
    try:
        params = {
            "$filter": "SpatialDimType in ('COUNTRY','REGION') and "
            "(Dim1 eq null or (Dim1Type eq 'SEX' and Dim1 eq 'BTSX'))"
        }
        # res = pd.DataFrame(json.loads(requests.get(f"https://ghoapi.azureedge.net/api/{code}", params=params).content)['value'])
        async with session.get(
            f"https://ghoapi.azureedge.net/api/{code}", params=params
        ) as resp:
            resp.raise_for_status()
            res_json = await resp.json()
    except Exception as e:
        logger.error(f'Encountered an error for code "{code}": {e}')

    return res_json  # type: ignore


def _fetch_meta_all_variables() -> List[dict]:
    """retrieves the name and code for all indicators in the WHO GHO
    dataset.

    Returns:

        indicators: List[dict]. List of WHO GHO variables, as retrieved from
            https://ghoapi.azureedge.net/api/indicator.
    """
    url_indicators = "https://ghoapi.azureedge.net/api/indicator"
    indicators = json.loads(requests.get(url_indicators).content)["value"]
    assert len(indicators) > 2000
    return indicators


def _fetch_description_many_variables(codes: List[str]) -> List[str]:
    """Fetches the description for multiple variables.

    Arguments:

        codes: List[str]. List of variable codes for which to download WHO GHO
            data. Example:

                ["MDG_0000000017", "WHS4_100", ...]

    Returns:

        descs: List[str]. List of variable descriptions.
    """
    indicators = _fetch_meta_all_variables()
    var_code2name = {
        ind["IndicatorCode"]: ind["IndicatorName"]
        for ind in indicators
        if ind["IndicatorCode"] in codes
    }
    names = [var_code2name[code] for code in codes]
    var_name2url = _fetch_description_urls()
    descs = []
    for name in names:
        url = var_name2url[name]
        desc = _fetch_description_one_variable(url)
        descs.append(desc)
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
    soup = BeautifulSoup(requests.get(url).content, features="lxml")
    divs = soup.find("div", {"id": "metadata"}).find_all(
        "div", {"class": "metadata-box"}
    )
    text = ""
    for div in divs:
        heading_text = re.sub(
            r":$", "", div.find("div", {"class": "metadata-title"}).text.strip().lower()
        )
        if heading_text in headings_to_use:
            text += f"\n\n{heading_text.capitalize()}: {div.find(text=True, recursive=False).strip()}"
    text = text.strip()
    return text


def _fetch_description_urls() -> Dict[str, str]:
    """Fetches variable urls for where descriptions are available to be scraped.

    Returns:

        res: Dict[str, str]. Dict where keys represent a variable's name and
            values represent a variable's URL.
    """
    soup = BeautifulSoup(
        requests.get("https://www.who.int/data/gho/data/indicators").content,
        features="lxml",
    )
    a_tags = (
        soup.find("div", {"id": "PageContent_T0AFD33E6006_Col00"})
        .find("div", {"class": "alphabetical-group"})
        .find_all("a")
    )
    assert len(a_tags) > 1500
    res = {}
    for a in a_tags:
        name = re.sub(r"\s+", " ", a.text.strip())
        res[name] = a.get("href")
    return res


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

    url_df = pd.DataFrame(
        list(zip(ind_codes, ind_name, urls)), columns=["code", "name", "url"]
    )
    return url_dict, url_df


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
