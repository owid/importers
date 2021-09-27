import os
import glob
import pandas as pd
import glob
import numpy as np
from pathlib import Path

from ihme_gbd import (
    INPATH,
    ENTFILE,
    DATASET_NAME,
    DATASET_AUTHORS,
    DATASET_VERSION,
    OUTPATH,
    DATASET_RETRIEVED_DATE,
    CONFIGPATH,
)
from ihme_gbd.core import clean_datasets


def main() -> None:
    load_and_clean()
    create_datasets()
    create_sources()
    var_list, df_list = get_variables()
    create_variables_datapoints(var_list, df_list)


def load_and_clean() -> None:
    if not os.path.isfile(os.path.join(INPATH, "all_data.csv")):
        all_files = [
            i for i in glob.glob(os.path.join(INPATH, "gbd_cause", "csv", "*.csv"))
        ]
        df_from_each_file = (pd.read_csv(f, sep=",") for f in all_files)
        df_merged = pd.concat(df_from_each_file, ignore_index=True)
        assert sum(df_merged.isnull().sum()) == 0, print("Null values in dataframe")
        df_merged.to_csv(os.path.join(INPATH, "all_data.csv"))
    if not os.path.isfile(ENTFILE):
        df_merged[["location_name"]].drop_duplicates().dropna().rename(
            columns={"location_name": "Country"}
        ).to_csv(ENTFILE, index=False)
    Path(OUTPATH, "datapoints").mkdir(parents=True, exist_ok=True)


def create_datasets() -> pd.DataFrame:
    df_datasets = clean_datasets(DATASET_NAME, DATASET_AUTHORS, DATASET_VERSION)
    assert (
        df_datasets.shape[0] == 1
    ), f"Only expected one dataset in {os.path.join(OUTPATH, 'datasets.csv')}."
    print("Creating datasets csv...")
    df_datasets.to_csv(os.path.join(OUTPATH, "datasets.csv"), index=False)
    return df_datasets


def create_sources() -> None:
    df_sources = pd.DataFrame(
        {
            "dataPublishedBy": [
                "Global Burden of Disease Collaborative Network. Global Burden of Disease Study 2019 (GBD 2019) Results. Seattle, United States: Institute for Health Metrics and Evaluation (IHME), 2021."
            ],
            "dataPublisherSource": [None],
            "link": ["http://ghdx.healthdata.org/gbd-results-tool"],
            "retrievedDate": [DATASET_RETRIEVED_DATE],
            "additionalInfo": [None],
        }
    )
    print("Creating sources csv...")
    df_sources.to_csv(os.path.join(OUTPATH, "sources.csv"), index=False)


def get_variables() -> None:
    df_merged = pd.read_csv(
        os.path.join(INPATH, "all_data.csv"), chunksize=10 ** 5
    )  # working through the data 100k rows at a time
    var_list = []
    df_list = []
    for chunk in df_merged:
        chunk["variable_name"] = (
            chunk["measure_name"]
            + " - "
            + chunk["cause_name"]
            + " - Sex:"
            + chunk["sex_name"]
            + " - Age:"
            + chunk["age_name"]
            + " ("
            + chunk["metric_name"]
            + ")"
        )
        var_list.append(chunk["variable_name"])
        df_list.append(chunk)
    var_list = pd.concat(var_list).drop_duplicates()
    print("Creating list of variables")
    return var_list, df_list


def create_variables_datapoints(var_list: list, df_list: list):
    variable_idx = 0
    variables = pd.DataFrame()

    entity2owid_name = (
        pd.read_csv(os.path.join(CONFIGPATH, "standardized_entity_names.csv"))
        .set_index("Country")
        .squeeze()
        .to_dict()
    )

    for var in var_list:
        var_data = []
        for df in df_list:
            var_df = df[df["variable_name"] == var]
            var_data.append(var_df)
        df = pd.concat(var_data)
        df["country"] = df["location_name"].apply(lambda x: entity2owid_name[x])
        df[["location_name", "year", "val"]].rename(
            columns={"location_name": "country", "val": "value"}
        ).to_csv(
            os.path.join(OUTPATH, "datapoints", "datapoints_%d.csv" % variable_idx),
            index=False,
        )
        print("Creating datapoints_%d.csv" % variable_idx)

        variable = {
            "dataset_id": int(0),
            "source_id": int(0),
            "id": variable_idx,
            "name": "%s" % (var),
            "description": None,
            "code": None,
            "unit": df[["metric_name"]].drop_duplicates(),
            "short_unit": None,
            "timespan": "%s - %s"
            % (
                int(np.min(df["year"])),
                int(np.max(df["year"])),
            ),
            "coverage": None,
            "display": None,
            "original_metadata": None,
        }
        variables = variables.append(variable, ignore_index=True)
        variable_idx += 1

    variables.to_csv(os.path.join(OUTPATH, "variables.csv"), index=False)


if __name__ == "__main__":
    main()
