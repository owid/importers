"""

https://datacatalog.worldbank.org/dataset/world-development-indicators

Usage:

    python -m worldbank_wdi.clean

Instructions for manually standardizing entity names:

0. Retrieve all unique entity names in the dataset:

    ```
    >>> df_entities = pd.read_csv(os.path.join(INPATH, 'WDICountry.csv.zip'))
    >>> assert not df_entities['Country code'].duplicated().any()
    >>> df_entities[['Country code']].drop_duplicates() \
    >>>                                     .dropna() \
    >>>                                     .rename(columns={'Country code': 'Country'}) \
    >>>                                     .to_csv(outfpath, index=False)
    ```

1. Open the OWID Country Standardizer Tool
   (https://owid.cloud/admin/standardize);

2. Change the "Input Format" field to "ISO 3166-1 ALPHA-3 CODE";

3. Change the "Output Format" field to "Our World In Data Name"; 

4. In the "Choose CSV file" field, upload {outfpath};

5. For any country codes that do NOT get matched, enter a custom name on
   the webpage (in the "Or enter a Custom Name" table column);

    * NOTE: For this dataset, you will most likely need to enter custom
      names for regions/continents (e.g. "Arab World", "Lower middle
      income");

6. Click the "Download csv" button;

7. Replace {outfpath} with the downloaded CSV;

8. Rename the "Country" column to "country_code".

"""

import os
import re
import simplejson as json
import shutil
from typing import List, Tuple, Dict
import pandas as pd
from pandas.api.types import is_numeric_dtype
from tqdm import tqdm
from dotenv import load_dotenv

from utils import camel_case2snake_case
from worldbank_wdi import (
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
KEEP_PATHS = ["variables_to_clean.json"]

# Max length of source name.
MAX_SOURCE_NAME_LEN = 256


def main() -> None:

    delete_output(KEEP_PATHS)
    mk_output_dir()

    # loads variables to be cleaned and uploaded.
    variables_to_clean = load_variables_to_clean()
    variable_codes = [ind["code"] for ind in variables_to_clean]
    assert all([pd.notnull(c) for c in variable_codes])

    # loads mapping of "{UNSTANDARDIZED_ENTITY_CODE}" -> "{STANDARDIZED_OWID_NAME}"
    # i.e. {"AFG": "Afghanistan", "SSF": "Sub-Saharan Africa", ...}
    entity2owid_name = (
        pd.read_csv(os.path.join(CONFIGPATH, "standardized_entity_names.csv"))
        .set_index("country_code")
        .squeeze()
        .to_dict()
    )

    # cleans datasets, datapoints, variables, and sources.
    df_datasets = clean_datasets()
    var_code2meta = clean_and_create_datapoints(
        variable_codes=variable_codes, entity2owid_name=entity2owid_name
    )
    assert (
        df_datasets.shape[0] == 1
    ), f"Only expected one dataset in {os.path.join(OUTPATH, 'datasets.csv')}."

    df_sources, var_code2source_id = clean_sources(
        dataset_id=df_datasets["id"].iloc[0],
        dataset_name=df_datasets["name"].iloc[0],
        variable_codes=variable_codes,
    )
    for var_code, source_id in var_code2source_id.items():
        var_code2meta[var_code]["source_id"] = source_id

    df_variables = clean_variables(
        dataset_id=df_datasets["id"].iloc[0],
        variables=[var for var in variables_to_clean if var["code"] in var_code2meta],
        var_code2meta=var_code2meta,
    )
    assert df_sources["id"].isin(df_variables["source_id"].unique()).all()

    df_distinct_entities = pd.DataFrame(get_distinct_entities(), columns=["name"])

    # saves datasets, sources, variables, and distinct entities to disk.
    df_datasets.to_csv(os.path.join(OUTPATH, "datasets.csv"), index=False)
    df_sources.to_csv(os.path.join(OUTPATH, "sources.csv"), index=False)
    df_variables.to_csv(os.path.join(OUTPATH, "variables.csv"), index=False)
    df_distinct_entities.to_csv(
        os.path.join(OUTPATH, "distinct_countries_standardized.csv"), index=False
    )


def load_variables_to_clean() -> List[dict]:
    """loads the array of variables to clean."""
    try:
        with open(os.path.join(CONFIGPATH, "variables_to_clean.json"), "r") as f:
            variables = json.load(f)["variables"]
    except:
        with open(os.path.join(OUTPATH, "variables_to_clean.json"), "r") as f:
            variables = json.load(f)["variables"]
    return variables


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


def clean_datasets():
    """Constructs a dataframe where each row represents a dataset to be upserted."""
    data = [
        {"id": 0, "name": f"{DATASET_NAME} - {DATASET_AUTHORS} ({DATASET_VERSION})"}
    ]
    df = pd.DataFrame(data)
    return df


def clean_and_create_datapoints(
    variable_codes: List[str], entity2owid_name: dict
) -> Dict[str, dict]:
    """Cleans all entity-variable-year data observations and saves all
    data points to csv in the `{OUTPATH}/datapoints` directory.

    The data for each variable is saved as a separate csv file.

    Arguments:

        variable_codes: List[str]. List of World Bank WDI variable codes to
            clean. Example::

                ["EG.ELC.ACCS.ZS", ...]

        entity2owid_name: dict. Dict of "{UNSTANDARDIZED_ENTITY_CODE}" -> "{STANDARDIZED_OWID_NAME}"
            mappings. Example::

                {"AFG": "Afghanistan", "SSF": "Sub-Saharan Africa", ...}

    Returns:

        var_code2meta: Dict[str, dict]. Dictionary that maps each var code to
            a dict of metadata including a temporary id and any metadata that
            is derived from datapoints (e.g. timespan). Example::

                {"EG.ELC.ACCS.ZS": {"id": 0, "timespan": "1960-2019"}}

    """
    # loads data
    df_data = pd.read_csv(os.path.join(INPATH, "WDIData.csv.zip"), compression="gzip")
    df_data.columns = df_data.columns.str.lower().str.replace(
        r"[\s/-]+", "_", regex=True
    )
    df_data["indicator_code"] = df_data["indicator_code"].str.upper()
    years = (
        df_data.columns[df_data.columns.str.contains(r"^\d{4}$")].sort_values().tolist()
    )
    df_data.dropna(subset=years, how="all", inplace=True)

    # standardizes entity names.
    df_data["country"] = df_data["country_code"].apply(lambda x: entity2owid_name[x])

    assert (
        df_data.groupby("indicator_code")["indicator_name"].apply(
            lambda gp: gp.nunique()
        )
        == 1
    ).all(), "A variable code in `WDIData.csv` has multiple variable names."

    uniq_codes = df_data["indicator_code"].unique().tolist()
    for code in variable_codes:
        if code not in uniq_codes:
            logger.warning(
                f'Variable code "{code}" is not a valid World Bank WDI '
                "variable code."
            )

    df_data = df_data[df_data["indicator_code"].isin(variable_codes)]

    # compares indicator names in WDIData.csv to WDISeries.csv
    # df_variables = pd.read_csv(os.path.join(DATASET_DIR, 'input', 'WDISeries.csv'))
    # df_variables.columns = df_variables.columns.str.lower().str.replace(r'[\s/-]+', '_', regex=True)
    # d = df_variables.set_index('series_code')[['indicator_name']].rename(columns={'indicator_name': 'series'}).to_dict(orient='index')
    # for code, name in df_data.set_index('indicator_code')['indicator_name'].to_dict().items():
    #     d[code]['data'] = name
    # for k, subd in d.items():
    #     if subd['series'] != subd['data']:
    #         print(f"{k:20s}: '{str(subd['series'])}' ; '{str(subd['data'])}'")

    # cleans each variable and saves it to csv.
    out_path = os.path.join(OUTPATH, "datapoints")
    if not os.path.exists(out_path):
        os.makedirs(out_path)

    i = 0
    ignored_var_codes = set({})
    kept_var_codes = set({})
    var_code2meta = {}
    grouped = df_data.groupby("indicator_code")
    logger.info("Saving data points for each variable to csv...")
    for var_code, gp in tqdm(grouped, total=len(grouped)):
        gp_long = (
            gp.set_index("country")[years]
            .stack()
            .sort_index()
            .reset_index()
            .rename(columns={"level_1": "year", 0: "value"})
        )
        gp_long["year"] = gp_long["year"].astype(int)
        assert not gp_long.duplicated(subset=["country", "year"]).any()
        assert is_numeric_dtype(gp_long["value"])
        assert is_numeric_dtype(gp_long["year"])
        assert gp_long.notnull().all().all()
        if gp_long.shape[0] == 0:
            ignored_var_codes.add(var_code)
        else:
            kept_var_codes.add(var_code)
            assert var_code not in var_code2meta
            timespan = f"{int(gp_long['year'].min())}-{int(gp_long['year'].max())}"
            var_code2meta[var_code] = {"id": i, "timespan": timespan}
            fpath = os.path.join(out_path, f"datapoints_{i}.csv")
            assert not os.path.exists(fpath), (
                f"{fpath} already exists. This should not be possible, because "
                "each variable is supposed to be assigned its own unique "
                "file name."
            )
            gp_long.to_csv(fpath, index=False)
            i += 1

    logger.info(
        f"Saved data points to csv for {i} variables. Excluded {len(ignored_var_codes)} variables."
    )

    # df_variables = df_data[['indicator_code', 'indicator_name']].drop_duplicates()
    # df_variables = df_variables[df_variables['indicator_code'].isin(kept_var_codes)]
    # df_variables['id'] = df_variables['indicator_code'].apply(lambda x: var_code2temp_id[x])
    # kept_variables = df_variables.to_dict(orient='records')
    return var_code2meta


def clean_sources(
    dataset_id: int, dataset_name: str, variable_codes: List[str]
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """Cleans a dataframe of data sources in preparation for uploading the
    sources to the `sources` database table.

    Arguments:

        variable_codes: List[str].

    Returns:

        df_sources: pd.DataFrame. (Partially) Cleaned Dataframe of data sources
            to be uploaded.

        var_code2source_id: Dict[str, int]. Dict of "{var_code}" ->
            "{source_id}" pairings. Example::

                {
                    'SH.MED.SAOP.P5': 0,
                    'NY.ADJ.DPEM.GN.ZS': 1,
                    'NY.ADJ.DPEM.CD': 1,
                    ...
                }


    """
    df_variables = _load_variables(codes=variable_codes)
    i = 0
    sources = []
    source_temp_id2var_codes = {}
    for nm, gp in df_variables.groupby("source"):
        # if source name begins and ends with parentheses, remove the parentheses.
        regex = re.search(r"^[\.\,]?\((.+)\)[\.\,]?$", nm)
        if regex:
            nm = regex.groups()[0]
        # remove a few selected prefixes from source names.
        regex_prefixes = re.compile(
            r"Data are from|Derived using data from", re.IGNORECASE
        )
        nm = regex_prefixes.sub("", nm).strip()

        # retrieves additional info, if it exists.
        additional_info = None
        if gp["notes_from_original_source"].notnull().any():
            notes = gp["notes_from_original_source"].dropna().unique().tolist()
            if len(notes) == 1:
                additional_info = notes[0]

        # constructs the source dict.
        if len(nm) <= MAX_SOURCE_NAME_LEN:
            source_nm = f"Data compiled by {DATASET_AUTHORS} from: {nm}"
            source = {
                "dataset_id": dataset_id,
                "name": source_nm,
                "description": json.dumps(
                    {
                        "link": DATASET_LINK,
                        "retrievedDate": DATASET_RETRIEVED_DATE,
                        "additionalInfo": additional_info,
                        "dataPublishedBy": dataset_name,
                        "dataPublisherSource": source_nm,
                    },
                    ignore_nan=True,
                ),
                "id": i,
            }
            assert source["id"] not in source_temp_id2var_codes
            sources.append(source)
            source_temp_id2var_codes[source["id"]] = (
                gp["indicator_code"].unique().tolist()
            )
            i += 1
        else:
            for _, row in gp.iterrows():
                var_name = row["indicator_name"]
                source_nm = f"Data compiled by {DATASET_AUTHORS} from multiple sources for variable: {var_name}"
                data_publisher_source = (
                    f"Data compiled by {DATASET_AUTHORS} from multiple sources"
                )
                additional_info2 = f"Data compiled by {DATASET_AUTHORS} from the following sources: {nm}"
                if additional_info is not None:
                    additional_info2 += f"\n{additional_info}"
                source = {
                    "dataset_id": dataset_id,
                    "name": source_nm,
                    "description": json.dumps(
                        {
                            "link": DATASET_LINK,
                            "retrievedDate": DATASET_RETRIEVED_DATE,
                            "additionalInfo": additional_info2,
                            "dataPublishedBy": dataset_name,
                            "dataPublisherSource": data_publisher_source,
                        },
                        ignore_nan=True,
                    ),
                    "id": i,
                }
                assert source["id"] not in source_temp_id2var_codes
                sources.append(source)
                source_temp_id2var_codes[source["id"]] = [row["indicator_code"]]
                i += 1

    # creates a generic source for any variables that contain a null
    # `source` column.
    var_codes_null_source = (
        df_variables.loc[df_variables["source"].isnull(), "indicator_code"]
        .unique()
        .tolist()
    )
    if len(var_codes_null_source):
        source = {
            "dataset_id": dataset_id,
            "name": dataset_name,
            "description": json.dumps(
                {
                    "link": DATASET_LINK,
                    "retrievedDate": DATASET_RETRIEVED_DATE,
                    "additionalInfo": None,
                    "dataPublishedBy": dataset_name,
                    "dataPublisherSource": DATASET_AUTHORS,
                },
                ignore_nan=True,
            ),
            "id": i,
        }
        # assert generic_source['name'] not in source_nm2var_codes
        sources.append(source)
        source_temp_id2var_codes[source["id"]] = var_codes_null_source
        i += 1

    df_sources = pd.DataFrame(sources)
    var_code2source_id = {
        var_code: source_id
        for source_id, var_codes in source_temp_id2var_codes.items()
        for var_code in var_codes
    }
    return df_sources, var_code2source_id


def clean_variables(
    dataset_id: int, variables: List[dict], var_code2meta: Dict[str, dict]
) -> pd.DataFrame:
    """Cleans a dataframe of variables in preparation for uploading the
    variables to the `variables` database table.

    Arguments:

        dataset_id: int. Integer representing the dataset id for all variables.

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
                        "display": {"name": "Child mortality rate", "unit": "%", "shortUnit": "%", "numDecimalPlaces": 2},
                        "replaces": null
                    },
                    ...
                ]

        var_code2meta: Dict[dict]. Dict of `variable code` -> `{variable meta}`
            mappings. Contains some metadata for each variable that was
            constructed during the `clean_and_create_datapoints` step. All
            variable codes in `variables` MUST have a corresponding key in
            `var_code2meta`. Example:

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

    # Removes the "replaces" field if it exists, since it is not part of the SQL
    # schema.
    for variable in variables:
        if "replaces" in variable:
            variable.pop("replaces")

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
    df_variables["description"] = _load_description_many_variables(
        df_variables.code.tolist()
    )

    # cleans variable names.
    df_variables["name"] = df_variables["name"].str.replace(r"\s+", " ", regex=True)

    df_variables["dataset_id"] = dataset_id

    # converts column names to snake case b/c this is what is expected in the
    # `standard_importer.import_dataset` module.
    df_variables.columns = df_variables.columns.map(camel_case2snake_case)

    required_fields = ["id", "name", "dataset_id", "source_id"]
    for field in required_fields:
        assert field in df_variables.columns, f"`{field}` does not exist."
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
    entities = set({})
    for fname in fnames:
        df_temp = pd.read_csv(os.path.join(OUTPATH, "datapoints", fname))
        entities.update(df_temp["country"].unique().tolist())

    entities = list(entities)
    assert pd.notnull(entities).all(), (
        "All entities should be non-null. Something went wrong in "
        "`clean_and_create_datapoints()`."
    )
    return entities


def _load_variables(codes: List[str]) -> pd.DataFrame:
    df_variables = pd.read_csv(
        os.path.join(INPATH, "WDISeries.csv.zip"), compression="gzip"
    )
    df_variables.columns = df_variables.columns.str.lower().str.replace(
        r"[\s/-]+", "_", regex=True
    )
    df_variables.rename(columns={"series_code": "indicator_code"}, inplace=True)
    df_variables["indicator_code"] = df_variables["indicator_code"].str.upper()
    df_variables = df_variables[df_variables["indicator_code"].isin(codes)]
    return df_variables


def _load_description_many_variables(codes: List[str]) -> List[str]:
    df_variables = _load_variables(codes)
    # creates `description` column
    df_variables["description"] = df_variables.apply(
        lambda s: s["long_definition"]
        if pd.notnull(s["long_definition"])
        else s["short_definition"],
        axis=1,
    )
    if df_variables["description"].isnull().any():
        logger.warning(
            "The `description` column (i.e. variable definition) is null for the "
            "following variables:\n"
            f"{json.dumps(df_variables.loc[df_variables['description'].isnull(), 'indicator_name'].tolist(), indent=2)}\n"
            "These variables will not have a variable description."
        )
    var_code2desc = df_variables.set_index("indicator_code")["description"].to_dict()
    descriptions = [var_code2desc[c] for c in codes]
    return descriptions


if __name__ == "__main__":
    main()
