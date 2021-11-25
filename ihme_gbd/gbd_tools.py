from typing import Tuple
import requests
import os
import glob
import pandas as pd
import zipfile
import io
import json
import numpy as np
from tqdm import tqdm
from pathlib import Path
import feather


def make_dirs(inpath: str, outpath: str, configpath: str) -> None:
    """
    Creating the necessary directories for the input, output and config files
    """
    Path(inpath).mkdir(parents=True, exist_ok=True)
    Path(outpath, "datapoints").mkdir(parents=True, exist_ok=True)
    Path(configpath).mkdir(parents=True, exist_ok=True)


def download_data(url: str, inpath: str) -> None:
    """
    Downloading the input data from a given URL STUB - these expire after a few days so need to be requested and updated following the instructions in download.py
    """
    status = True
    while status:
        for i in range(1, 9999):
            fname = url + "%s.zip" % i
            print(fname)
            r = requests.get(fname)
            if r.ok:
                z = zipfile.ZipFile(io.BytesIO(r.content))
                z.extractall(os.path.join(inpath, "csv"))
            else:
                status = False
                os.remove(os.path.join(inpath, "csv", "citation.txt"))
                break


def load_and_filter(inpath: str, entfile: str, column_fields: tuple) -> None:
    """
    Loading and merging all of the input csv files into one large csv files.
    We standardise the column names here as this can vary based on what is selected from the source.
    We select out only the columns we need for the rest of the import in order to keep the size of the file down.
    """
    if not os.path.isfile(os.path.join(inpath, "all_data_filtered.ftr")):
        all_files = [i for i in glob.glob(os.path.join(inpath, "csv", "*.csv"))]
        df_from_each_file = (pd.read_csv(f, sep=",") for f in all_files)
        df_merged = pd.concat(df_from_each_file, ignore_index=True)
        assert sum(df_merged.isnull().sum()) == 0, print("Null values in dataframe")
        # standardising column names
        df_merged = df_merged.rename(
            columns={
                "measure": "measure_name",
                "location": "location_name",
                "sex": "sex_name",
                "age": "age_name",
                "cause": "cause_name",
                "metric": "metric_name",
                "rei": "rei_name",
            }
        )
        df_merged = df_merged[column_fields]
        # df_merged = df_merged[df_merged['sex_name'].isin(sex_list) & df_merged['age_name'].isin(age_list) & df_merged['metric_name'].isin(metric_list)]
        df_merged.to_feather(os.path.join(inpath, "all_data_filtered.ftr"))
        print("Saving all data from raw csv files")
    if not os.path.isfile(entfile):
        df_merged[["location_name"]].drop_duplicates().dropna().rename(
            columns={"location_name": "Country"}
        ).to_csv(entfile, index=False)
        print(
            "Saving entity files"
        )  # use this file in the country standardizer tool - save standardized file as config/standardized_entity_names.csv


def create_datasets(
    dataset_name: str, dataset_authors: str, dataset_version: str, outpath: str
) -> pd.DataFrame:
    """Constructs a dataframe where each row represents a dataset to be
    upserted.
    Note: often, this dataframe will only consist of a single row.
    """
    data = [
        {"id": 0, "name": f"{dataset_name} - {dataset_authors} ({dataset_version})"}
    ]
    df_datasets = pd.DataFrame(data)
    assert (
        df_datasets.shape[0] == 1
    ), f"Only expected one dataset in {os.path.join(outpath, 'datasets.csv')}."
    print("Creating datasets csv...")
    df_datasets.to_csv(os.path.join(outpath, "datasets.csv"), index=False)
    return df_datasets


def create_sources(dataset_retrieved_date: str, outpath: str) -> None:
    """Creating the information to go into the source tab.
    We don't have any additional variable level metadata for this dataset so we just have this generic source tab."""
    source_description = {
        "dataPublishedBy": "Global Burden of Disease Collaborative Network. Global Burden of Disease Study 2019 (GBD 2019) Results. Seattle, United States: Institute for Health Metrics and Evaluation (IHME), 2021.",
        "dataPublisherSource": "Institute for Health Metrics and Evaluation",
        "link": "http://ghdx.healthdata.org/gbd-results-tool",
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


def get_variables(df: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """Outputting a list of each unique combination of measure, cause, sex, age and metric.
    We add this variable as a column name to the filtered dataset so that we can access it
    and iterate through variables in the next step."""
    df["variable_name"] = [
        p1 + " - " + p2 + " - Sex: " + p3 + " - Age: " + p4 + " (" + p5 + ")"
        for p1, p2, p3, p4, p5 in zip(
            df["measure_name"],
            df["cause_name"],
            df["sex_name"],
            df["age_name"],
            df["metric_name"],
        )
    ]
    df_fil = df.drop(["measure_name", "sex_name", "age_name", "cause_name"], axis=1)
    var_list = df_fil["variable_name"].drop_duplicates().to_list()
    return (df_fil, var_list)


def create_variables_datapoints(inpath: str, configpath: str, outpath: str) -> None:
    """Iterating through each variable and pulling out the relevant datapoints.
    Formatting the data for the variables.csv file and outputting the associated csv files into the datapoints folder."""

    df_all = pd.read_feather(os.path.join(inpath, "all_data_filtered.ftr"))

    df, var_list = get_variables(df_all)

    variable_idx = 0
    variables = pd.DataFrame()

    units_dict = {"Percent": "%", "Rate": "", "Number": ""}

    entity2owid_name = (
        pd.read_csv(os.path.join(configpath, "standardized_entity_names.csv"))
        .set_index("Country")
        .squeeze()
        .to_dict()
    )

    df["location_name"] = df["location_name"].apply(  # move this out of loop
        lambda x: entity2owid_name[x]
    )

    for var in tqdm(var_list):
        var_df = df.query(
            'variable_name == "%s"' % var
        )  # faster than df[df["variable_name"] == var]
        variable = {
            "dataset_id": int(0),
            "source_id": int(0),
            "id": variable_idx,
            "name": "%s" % (var),
            "description": None,
            "code": None,  # removed the columns used for this as I don't think we actually use it and it made the data handling a lot easier if we got rid off them
            "unit": var_df["metric_name"].iloc[0],
            "short_unit": units_dict[var_df["metric_name"].iloc[0]],
            "timespan": "%s - %s"
            % (
                int(np.min(var_df["year"])),
                int(np.max(var_df["year"])),
            ),
            "coverage": None,
            "display": None,
            "original_metadata": None,
        }
        variables = variables.append(variable, ignore_index=True)

        if var_df.loc[0, "metric_name"] == "Percent":
            var_df["val"] = var_df["val"] * 100

        var_df[["location_name", "year", "val"]].rename(
            columns={"location_name": "country", "val": "value"}
        ).to_csv(
            os.path.join(outpath, "datapoints", "datapoints_%d.csv" % variable_idx),
            index=False,
        )

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
