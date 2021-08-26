"""
Usage: python -m edstats.clean
"""

import json
import os
import re
import shutil

import pandas as pd
from tqdm import tqdm

from edstats import (
    DATASET_NAME,
    DATASET_AUTHORS,
    DATASET_VERSION,
    DATASET_LINK,
    DATASET_RETRIEVED_DATE,
    CONFIGPATH,
    INPATH,
    OUTPATH,
)

VARIABLE_FILENAME = "EdStatsSeries.csv"
DATA_FILENAME = "EdStatsData.csv"


def main() -> None:
    delete_output()
    mk_output_dir()
    prepare_dataset()
    vars = prepare_variables()
    prepare_datapoints(vars)


def delete_output() -> None:
    if os.path.exists(OUTPATH):
        shutil.rmtree(OUTPATH)
        os.makedirs(OUTPATH)


def mk_output_dir() -> None:
    if not os.path.exists(OUTPATH):
        os.makedirs(OUTPATH)
    if not os.path.exists(os.path.join(OUTPATH, "datapoints")):
        os.makedirs(os.path.join(OUTPATH, "datapoints"))


def prepare_dataset() -> pd.DataFrame:
    print("Preparing dataset…")

    dataset = pd.DataFrame(
        {
            "id": 0,
            "name": [DATASET_NAME + " " + DATASET_VERSION],
        }
    )
    dataset.to_csv(os.path.join(OUTPATH, "datasets.csv"), index=False)
    return dataset


def prepare_variables() -> pd.DataFrame:
    print("Preparing variables…")

    path = os.path.join(INPATH, VARIABLE_FILENAME)
    vars = (
        pd.read_csv(
            path,
            usecols=[
                "Series Code",
                "Indicator Name",
                "Long definition",
                "Limitations and exceptions",
                "General comments",
                "Source",
            ],
        )
        .rename(
            columns={
                "Series Code": "code",
                "Indicator Name": "name",
                "Long definition": "description",
                "Limitations and exceptions": "limitations",
                "General comments": "comments",
                "Source": "source_name",
            }
        )
        .sort_values("code")
        .reset_index()
        .rename(columns={"index": "id"})
    )

    vars["code"] = vars.code.str.upper().str.strip()

    vars.loc[vars.description.str.contains("%|[Pp]ercent|[Ss]hare of"), "unit"] = "%"
    vars["short_unit"] = vars.unit

    vars.loc[vars.source_name.str.contains("http"), "description"] = (
        vars.description + "\n" + vars.source_name
    )
    vars.loc[vars.limitations.notnull(), "description"] = (
        vars.description + "\n" + vars.limitations
    )
    vars.loc[vars.comments.notnull(), "description"] = (
        vars.description + "\n" + vars.comments
    )
    vars = vars.drop(columns=["limitations", "comments"])

    vars["dataset_id"] = 0

    sources = preprocess_sources(vars[["source_name"]])

    vars = vars.merge(sources, on="source_name").drop(columns=["source_name"])

    sources = postprocess_sources(sources)

    vars.to_csv(os.path.join(OUTPATH, "variables.csv"), index=False)
    sources.to_csv(os.path.join(OUTPATH, "sources.csv"), index=False)
    return vars


def preprocess_sources(sources: pd.DataFrame) -> pd.DataFrame:
    return (
        sources.drop_duplicates()
        .reset_index(drop=True)
        .reset_index()
        .rename(columns={"index": "source_id"})
    )


def postprocess_sources(sources: pd.DataFrame) -> pd.DataFrame:
    print("Preparing sources…")

    sources = sources.rename(
        columns={
            "source_name": "name",
            "source_id": "id",
        }
    ).assign(
        dataset_id=pd.NA,
        description=json.dumps(
            {
                "dataPublishedBy": "",
                "dataPublisherSource": "",
                "additionalInfo": "",
            }
        ),
    )

    # Add main dataset source
    main_source = pd.DataFrame(
        {
            "id": sources.id.max() + 1,
            "name": [DATASET_NAME + " (" + DATASET_VERSION + ")"],
            "dataset_id": 0,
            "description": json.dumps(
                {
                    "dataPublishedBy": DATASET_AUTHORS,
                    "dataPublisherSource": DATASET_NAME,
                    "link": DATASET_LINK,
                    "retrievedDate": DATASET_RETRIEVED_DATE,
                    "additionalInfo": "The World Bank EdStats data holds over 4,000 internationally comparable indicators that describe education access, progression, completion, literacy, teachers, population, and expenditures. The indicators cover the education cycle from pre-primary to vocational and tertiary education. The data also holds learning outcome data from international and regional learning assessments (e.g. PISA, TIMSS, PIRLS), equity data from household surveys, and projection/attainment data to 2050. For further information, please visit the EdStats website: https://datatopics.worldbank.org/education/",
                }
            ),
        }
    )

    sources["name"] = sources.name.str.replace("\.$", "", regex=True)
    sources["name"] = sources.name.str.replace(": http.*", "", regex=True)

    sources = pd.concat([sources, main_source])
    return sources


def prepare_datapoints(vars: pd.DataFrame) -> None:
    print("Preparing datapoints…")

    path = os.path.join(INPATH, DATA_FILENAME)
    df = (
        pd.read_csv(path)
        .drop(columns=["Country Code", "Unnamed: 69", "Indicator Name"])
        .rename(columns={"Indicator Code": "code"})
    )

    # Standardize country names
    shape_before = df.shape
    country_mapping = pd.read_csv(
        os.path.join(CONFIGPATH, "standardized_entity_names.csv")
    )
    df = (
        pd.merge(df, country_mapping, left_on="Country Name", right_on="Country")
        .drop(columns=["Country Name", "Country"])
        .rename(columns={"Our World In Data Name": "country"})
    )
    shape_after = df.shape
    assert (
        shape_before == shape_after
    ), "Country name standardization has changed the shape of the dataframe"

    # Reshape and drop NAs
    df = df.melt(id_vars=["country", "code"], var_name="year").dropna()

    # Map variable codes to variable ids
    shape_before = df.shape
    df["code"] = df.code.str.upper().str.strip()
    df = pd.merge(vars[["id", "code"]], df, on="code").drop(columns=["code"])
    shape_after = df.shape
    assert (
        shape_before == shape_after
    ), "Code mapping has changed the shape of the dataframe"

    # Write distinct_countries_standardized.csv
    df[["country"]].drop_duplicates().rename(columns={"country": "name"}).to_csv(
        os.path.join(OUTPATH, "distinct_countries_standardized.csv"), index=False
    )

    # Write datapoint files
    for id in tqdm(df.id.unique()):
        datapoints = df[df.id == id][["country", "year", "value"]]
        datapoints.to_csv(
            os.path.join(OUTPATH, "datapoints", f"datapoints_{id}.csv"), index=False
        )

    print("All done.")


if __name__ == "__main__":
    main()
