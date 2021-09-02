"""Cleans WDI metadata and data points in preparation for MySQL insert.

Usage:

    python -m worldbank_wdi.clean

"""

import os
import re
import simplejson as json
import shutil
from typing import List, Tuple, Dict, Optional
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
    var_code2meta = {ind["code"]: ind for ind in variables_to_clean}
    assert all([pd.notnull(c) for c in var_code2meta.keys()])

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
    var_code2meta_temp = clean_and_create_datapoints(
        variable_codes=list(var_code2meta.keys()), entity2owid_name=entity2owid_name
    )

    # updates variable metadata with metadata constructed during data point
    # creation and removes variables from cleaning that do not have any data
    # values associated with them.
    remove = []
    for var_code, meta in var_code2meta.items():
        if var_code in var_code2meta_temp:
            meta.update(var_code2meta_temp[var_code])
        else:  # no data values were constructed for this variable
            remove.append(var_code)

    for var_code in remove:
        del var_code2meta[var_code]

    assert (
        df_datasets.shape[0] == 1
    ), f"Only expected one dataset in {os.path.join(OUTPATH, 'datasets.csv')}."

    df_sources, var_code2source_id = clean_sources(
        dataset_id=df_datasets["id"].iloc[0],
        dataset_name=df_datasets["name"].iloc[0],
        variable_codes=list(var_code2meta.keys()),
    )
    for var_code, source_id in var_code2source_id.items():
        var_code2meta[var_code]["source_id"] = source_id

    df_variables = clean_variables(
        dataset_id=df_datasets["id"].iloc[0],
        variables=[var for var in var_code2meta.values()],
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
    except:  # noqa
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
        dataset_name: str. Dataset name.
        dataset_id: int. Temporary dataset id.
        variables_codes: List[str]. List of variable codes for which to clean
            source metadata.

    Returns:
        df_sources: pd.DataFrame. Cleaned dataframe of data sources
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
    with open(os.path.join(CONFIGPATH, "standardized_source_names.json"), "r") as f:
        df_source_names = pd.DataFrame(json.load(f))
        assert df_source_names["rawName"].duplicated().sum() == 0
    df_variables = _load_variables(variable_codes)

    df_source_names = (
        df_source_names.merge(
            df_variables.groupby("source")
            .apply(lambda gp: gp["indicator_code"].unique().tolist())
            .reset_index()
            .rename(columns={0: "variable_codes"}),
            left_on="rawName",
            right_on="source",
            how="left",
            validate="1:1",
        )
        .drop("source", axis=1)
        .dropna(subset=["variable_codes"])
    )
    sources = []
    var_code2source_id = {}
    for i, ((name, data_publisher_source), gp) in enumerate(
        df_source_names.groupby(["name", "dataPublisherSource"])
    ):
        var_codes = [c for codes in gp["variable_codes"] for c in codes]
        source = {
            "id": i,
            "dataset_id": dataset_id,
            "name": name,
            "description": json.dumps(
                {
                    "link": DATASET_LINK,
                    "retrievedDate": DATASET_RETRIEVED_DATE,
                    "additionalInfo": None,
                    "dataPublishedBy": dataset_name,
                    "dataPublisherSource": data_publisher_source,
                },
                ignore_nan=True,
            ),
        }
        sources.append(source)
        for c in var_codes:
            assert c not in var_code2source_id
            var_code2source_id[c] = i
    df_sources = pd.DataFrame(sources)
    missing_var_codes = [c for c in variable_codes if c not in var_code2source_id]
    assert len(missing_var_codes) == 0, (
        "All variable codes must have a source ID, but the following variables "
        f"do not: {missing_var_codes}. Are the source names for these variables "
        "missing from `standardized_source_names.json`?"
    )
    return df_sources, var_code2source_id


def clean_variables(dataset_id: int, variables: List[dict]) -> pd.DataFrame:
    """Cleans a dataframe of variables in preparation for uploading the
    variables to the `variables` database table.

    Arguments:

        dataset_id: int. Integer representing the dataset id for all variables.

        variables: List[dict]. List of variables with metadata. Contains some
            metadata for each variable that was constructed during the
            `clean_and_create_datapoints` step and the `clean_sources` step.
            Also contains variable metadata that is present in
            `variable_to_clean.json`. Example:

                [
                    {
                        "name": "Under-five mortality rate (probability of dying by age 5 per 1000 live births)",
                        "code": "MDG_0000000007",
                        "old": {
                            "unit": "%",
                            "shortUnit": "%",
                            "display": {"name": "Child mortality rate", "unit": "%", "shortUnit": "%", "numDecimalPlaces": 2},
                        }
                        "id": 0,
                        "timespan": "2000-2019",
                        "source_id": 0
                    },
                    ...
                ]

    Returns:

        df_variables: pd.DataFrame. Cleaned dataframe of variables
            to be uploaded.
    """
    assert all(
        [pd.notnull(var["code"]) for var in variables]
    ), "One or more variables has a null `code` field."

    df_variables = pd.DataFrame(variables)

    # fetches description for each variable
    df_variables["description"] = _load_description_many_variables(
        df_variables.code.tolist()
    )

    # cleans name column
    df_variables["name"] = df_variables["name"].str.replace(r"\s+", " ", regex=True)

    # cleans display column
    if "old" in df_variables.columns:
        displays = []
        for _, row in df_variables.iterrows():
            display = row["old"].get("display") if pd.notnull(row["old"]) else None
            if display:
                year_in_name_regex = re.search(r"\b([1-2]\d{3})\b", row["name"])
                if year_in_name_regex:
                    for k in ["name", "unit"]:
                        if k in display:
                            year = year_in_name_regex.groups()[0]
                            year_in_val_regex = re.search(
                                r"\b([1-2]\d{3})\b", display[k]
                            )
                            if year_in_val_regex:
                                val_year = year_in_val_regex.groups()[0]
                                if year != val_year:
                                    new_val = re.sub(
                                        r"\b([1-2]\d{3})\b", year, display[k]
                                    )
                                    logger.warning(
                                        f'The "display.{k}" field for variable "{row["name"]}" '
                                        f'contains a different year ("{display[k]}"). The {k} '
                                        f'year is being replaced to become: "{new_val}"'
                                    )
                                    val = new_val
                                    display[k] = val
                display = json.dumps(display, ignore_nan=True)
            displays.append(display)
        df_variables["display"] = displays

    # cleans shortUnit column
    if "old" in df_variables.columns:
        df_variables["shortUnit"] = df_variables["old"].apply(
            lambda x: x.get("shortUnit") if pd.notnull(x) else None
        )

    # cleans unit column
    if "old" in df_variables.columns:
        units = []
        for _, row in df_variables.iterrows():
            unit = row["old"].get("unit") if pd.notnull(row["old"]) else None
            if unit:
                year_in_name_regex = re.search(r"\b([1-2]\d{3})\b", row["name"])
                if year_in_name_regex:
                    year = year_in_name_regex.groups()[0]
                    year_in_unit_regex = re.search(r"\b([1-2]\d{3})\b", unit)
                    if year_in_unit_regex:
                        unit_year = year_in_unit_regex.groups()[0]
                        if year != unit_year:
                            new_unit = re.sub(r"\b([1-2]\d{3})\b", year, unit)
                            logger.warning(
                                f'The unit field for variable "{row["name"]}" '
                                f'contains a different year ("{unit}"). The unit '
                                f'year is being replaced to become: "{new_unit}"'
                            )
                            unit = new_unit
            units.append(unit)
        df_variables["unit"] = units

    # cleans originalMetadata column
    if "old" in df_variables.columns:
        df_variables["originalMetadata"] = df_variables["old"].apply(
            lambda x: x.get("originalMetadata") if pd.notnull(x) else None
        )
        df_variables["originalMetadata"] = df_variables["originalMetadata"].apply(
            lambda x: json.dumps(x, ignore_nan=True) if pd.notnull(x) else None
        )

    df_variables["dataset_id"] = dataset_id

    # converts column names to snake case b/c this is what is expected in the
    # `standard_importer.import_dataset` module.
    df_variables.columns = df_variables.columns.map(camel_case2snake_case)

    if "old" in df_variables.columns:
        df_variables.drop("old", axis=1, inplace=True)

    required_fields = ["id", "name", "dataset_id", "source_id"]
    for field in required_fields:
        assert field in df_variables.columns, f"`{field}` does not exist."
        assert df_variables[field].notnull().all(), (
            f"The following variables have a null `{field}` field:\n"
            f"{df_variables.loc[df_variables[field].isnull(), required_fields]}"
        )

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

    entity_list = sorted(entities)
    assert pd.notnull(entity_list).all(), (
        "All entities should be non-null. Something went wrong in "
        "`clean_and_create_datapoints()`."
    )
    return entity_list


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


def _load_description_many_variables(codes: List[str]) -> List[Optional[str]]:
    df_variables = _load_variables(codes)
    # creates `description` column
    descriptions: List[Optional[str]] = []
    for _, var in df_variables.iterrows():
        desc = ""
        if (
            pd.notnull(var["long_definition"])
            and len(var["long_definition"].strip()) > 0
        ):
            desc += var["long_definition"]
        elif (
            pd.notnull(var["short_definition"])
            and len(var["short_definition"].strip()) > 0
        ):
            desc += var["short_definition"]

        if (
            pd.notnull(var["limitations_and_exceptions"])
            and len(var["limitations_and_exceptions"].strip()) > 0
        ):
            desc += (
                f'\n\nLimitations and exceptions: {var["limitations_and_exceptions"]}'
            )

        if (
            pd.notnull(var["statistical_concept_and_methodology"])
            and len(var["statistical_concept_and_methodology"].strip()) > 0
        ):
            desc += f'\n\nStatistical concept and methodology: {var["statistical_concept_and_methodology"]}'

        # retrieves additional source info, if it exists.
        if (
            pd.notnull(var["notes_from_original_source"])
            and len(var["notes_from_original_source"].strip()) > 0
        ):
            desc += (
                f'\n\nNotes from original source: {var["notes_from_original_source"]}'
            )

        desc = re.sub(r" *(\n+) *", r"\1", re.sub(r"[ \t]+", " ", desc)).strip()
        if len(desc) == 0:
            descriptions.append(None)
        else:
            descriptions.append(desc)

    df_variables.loc[:, "description"] = descriptions
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
