"""
After running load_and_clean() to create $ENTFILE use the country standardiser tool to standardise $ENTFILE
1. Open the OWID Country Standardizer Tool
   (https://owid.cloud/admin/standardize);
2. Change the "Input Format" field to "Non-Standard Country Name";
3. Change the "Output Format" field to "Our World In Data Name"; 
4. In the "Choose CSV file" field, upload $ENTFILE;
5. For any country codes that do NOT get matched, enter a custom name on
   the webpage (in the "Or enter a Custom Name" table column);
    * NOTE: For this dataset, you will most likely need to enter custom
      names for regions/continents (e.g. "Arab World", "Lower middle
      income");
6. Click the "Download csv" button;
7. Name the downloaded csv 'standardized_entity_names.csv' and save in the output folder;
8. Rename the "Country" column to "country_code".
"""

import pandas as pd
import os
import shutil
import json
import numpy as np
import re
from pathlib import Path
from tqdm import tqdm

pd.set_option("display.max_columns", None)

from who_ghe import (
    INFILE,
    INPATH,
    CONFIGPATH,
    OUTPATH,
    ENTFILE,
    DATASET_NAME,
    DATASET_AUTHORS,
    DATASET_VERSION,
    DATASET_RETRIEVED_DATE,
)

from who_ghe.core import clean_datasets


def main():
    df = load_and_clean()
    create_datasets()
    create_sources(dataset_retrieved_date=DATASET_RETRIEVED_DATE, outpath=OUTPATH)
    create_variables_datapoints(data=df, configpath=CONFIGPATH, outpath=OUTPATH)
    create_distinct_entities(configpath=CONFIGPATH, outpath=OUTPATH)


def load_and_clean() -> pd.DataFrame:
    # Load and clean the data
    print("Reading in original data...")
    original_df = pd.read_csv(INFILE, low_memory=False).drop(
        ["Unnamed: 0", "Unnamed: 0.1"], axis=1
    )
    # Check there aren't any null values
    assert sum(original_df.isnull().sum()) == 0, print("Null values in dataframe")
    print("Extracting unique entities to " + ENTFILE + "...")
    original_df[["COUNTRY_CODE"]].drop_duplicates().dropna().rename(
        columns={"COUNTRY_CODE": "Country"}
    ).to_csv(ENTFILE, index=False)
    # Make the datapoints folder
    Path(OUTPATH, "datapoints").mkdir(parents=True, exist_ok=True)

    original_df = original_df.drop(
        [
            "GHE_CAUSE_CODE",
            "POPULATION",
            "CAUSEGROUP",
            "LEVEL",
            "FL_SINGLECAUSE",
            "FL_RANKABLE",
            "FL_TREEMAP",
            "FL_SHOW",
            "_RecordID",
            "Sys_PK",
            "Sys_OriginID",
            "Sys_OriginCode",
            "Sys_LoadBy",
            "Sys_CommitDateUtc",
            "Sys_FirstLoadBy",
            "Sys_FirstCommitDateUtc",
            "Sys_ID",
            "Sys_BatchID",
            "Sys_FirstBatchID",
            "Sys_RowTitle",
            "Sys_Version",
            "Sys_VersionID",
            "DEATHS_100K",
            "DALY_100K",
        ],
        axis=1,
    )

    return original_df


def create_datasets() -> pd.DataFrame:
    df_datasets = clean_datasets(DATASET_NAME, DATASET_AUTHORS, DATASET_VERSION)
    assert (
        df_datasets.shape[0] == 1
    ), f"Only expected one dataset in {os.path.join(OUTPATH, 'datasets.csv')}."
    print("Creating datasets csv...")
    df_datasets.to_csv(os.path.join(OUTPATH, "datasets.csv"), index=False)
    return df_datasets


def create_sources(dataset_retrieved_date: str, outpath: str) -> None:
    """Creating the information to go into the source tab.
    We don't have any additional variable level metadata for this dataset so we just have this generic source tab."""
    source_description = {
        "dataPublishedBy": "World Health Organization",
        "dataPublisherSource": "World Health Organization",
        "link": "https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_COD_COMPLETE",
        "retrievedDate": dataset_retrieved_date,
        "additionalInfo": None,
    }

    df_sources = pd.DataFrame(
        {
            "id": [0],
            "name": [source_description["dataPublisherSource"]],
            "description": [json.dumps(source_description)],
            "dataset_id": [0],
            "series_code": [None],
        }
    )

    print("Creating sources csv...")
    df_sources.to_csv(os.path.join(outpath, "sources.csv"), index=False)


def create_variables_datapoints(
    data: pd.DataFrame, configpath: str, outpath: str
) -> None:
    ### make the data long and create the variables column

    dfm = pd.melt(
        data,
        id_vars=[
            "COUNTRY_CODE",
            "GHE_CAUSE_TITLE",
            "YEAR",
            "SEX_CODE",
            "AGEGROUP_CODE",
        ],
        value_vars=["DEATHS", "DEATHS_RATE", "DALY", "DALY_RATE"],
    )

    entity2owid_name = (
        pd.read_csv(os.path.join(configpath, "standardized_entity_names.csv"))
        .set_index("Country")
        .squeeze()
        .to_dict()
    )

    dfm["country"] = dfm["COUNTRY_CODE"].apply(lambda x: entity2owid_name[x])

    # Convert sex

    sex_dict = {"BTSX": "Both", "MLE": "Male", "FMLE": "Female"}
    dfm["sex"] = dfm["SEX_CODE"].apply(lambda x: sex_dict[x])
    # Convert age groups

    age_dict = {
        "ALLAges": "All Ages",
        "YEARS0-1": "0-1 years",
        "YEARS1-4": "1-4 years",
        "YEARS5-9": "5-9 years",
        "YEARS10-14": "10-14 years",
        "YEARS15-19": "15-19 years",
        "YEARS20-24": "20-24 years",
        "YEARS25-29": "25-29 years",
        "YEARS30-34": "30-34 years",
        "YEARS35-39": "35-39 years",
        "YEARS40-44": "40-44 years",
        "YEARS45-49": "45-49 years",
        "YEARS50-54": "50-54 years",
        "YEARS55-59": "55-59 years",
        "YEARS60-64": "60-64 years",
        "YEARS65-69": "65-69 years",
        "YEARS70-74": "70-74 years",
        "YEARS75-79": "75-79 years",
        "YEARS80-84": "80-84 years",
        "YEARS85PLUS": "85+ years",
    }
    dfm["age_group"] = dfm["AGEGROUP_CODE"].apply(lambda x: age_dict[x])

    metric_dict = {
        "DEATHS": "Number",
        "DEATHS_RATE": "Rate",
        "DALY": "Number",
        "DALY_RATE": "Rate",
    }

    dfm["metric"] = dfm["variable"].apply(lambda x: metric_dict[x])

    unit_dict = {
        "DEATHS": "Deaths",
        "DEATHS_RATE": "Deaths",
        "DALY": "DALYs (Disability-Adjusted Life Years)",
        "DALY_RATE": "DALYs (Disability-Adjusted Life Years)",
    }

    dfm["units"] = dfm["variable"].apply(lambda x: unit_dict[x])

    dfm = dfm.drop(["COUNTRY_CODE", "SEX_CODE", "AGEGROUP_CODE", "variable"], axis=1)

    dfm["variable"] = [
        p1 + " - " + p2 + " - Sex: " + p3 + " - Age: " + p4 + " (" + p5 + ")"
        for p1, p2, p3, p4, p5 in zip(
            dfm["units"],
            dfm["GHE_CAUSE_TITLE"],
            dfm["sex"],
            dfm["age_group"],
            dfm["metric"],
        )
    ]

    var_list = dfm["variable"].drop_duplicates()

    variable_idx = 0
    variables = pd.DataFrame()
    for var in tqdm(var_list):
        var_df = dfm[dfm["variable"] == var]
        variable = {
            "dataset_id": int(0),
            "source_id": int(0),
            "id": variable_idx,
            "name": "%s" % (var),
            "description": None,
            "code": None,
            "unit": var_df["metric"].iloc[0],
            "short_unit": "",
            "timespan": "%s - %s"
            % (
                int(np.min(var_df["YEAR"])),
                int(np.max(var_df["YEAR"])),
            ),
            "coverage": None,
            "display": None,
            "original_metadata": None,
        }
        variables = variables.append(variable, ignore_index=True)

        var_df[["country", "YEAR", "value"]].rename(columns={"YEAR": "year"}).to_csv(
            os.path.join(outpath, "datapoints", "datapoints_%d.csv" % variable_idx),
            index=False,
        )
        print(var)
        variable_idx += 1

    variables.to_csv(os.path.join(outpath, "variables.csv"), index=False)


def create_distinct_entities(configpath: str, outpath: str) -> None:
    """Creating a list of distinct entities for use in upserting to the grapher db"""
    df_distinct_entities = pd.read_csv(
        os.path.join(configpath, "standardized_entity_names.csv")
    )
    df_distinct_entities = df_distinct_entities[["Our World In Data Name"]].rename(
        columns={"Our World In Data Name": "name"}
    )

    df_distinct_entities.to_csv(
        os.path.join(outpath, "distinct_countries_standardized.csv"), index=False
    )
