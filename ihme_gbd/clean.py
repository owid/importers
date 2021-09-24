import os
import time
import glob
import pandas as pd
import glob

# os.environ["MODIN_ENGINE"] = "ray"
# import ray
# ray.init(num_gpus=0)
# import modin.pandas as pd #requires pandas 1.3.2

from tqdm import tqdm
from time import time

from ihme_gbd import (
    INPATH,
    ENTFILE,
    DATASET_NAME,
    DATASET_AUTHORS,
    DATASET_VERSION,
    OUTPATH,
    DATASET_RETRIEVED_DATE,
)
from ihme_gbd.core import clean_datasets


def main() -> None:
    df_merged = load_and_clean()
    create_datasets()


def load_and_clean() -> None:
    if os.path.isfile(os.path.join("input", "all_data.csv")):
        df_merged = pd.read_csv(os.path.join(INPATH, "all_data.csv"), chunksize=10 ** 5)
    else:
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
    return df_merged


## Not sure how to best deal with the IHME data as it is split into four distinct datasets - causes, prevalence, risk and mental health. We should have a different dataset for each of these?


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
    df_sources.to_csv(os.path.join(OUTPATH, "sources.csv"), index=False)


def create_variables(mdf_merged: pd.DataFrame):

    all_series = mdf_merged[
        ["measure_name", "cause_name", "sex_name", "age_name", "metric_name"]
    ].drop_duplicates()
    all_series["variable_name"] = (
        all_series["measure_name"]
        + " - "
        + all_series["cause_name"]
        + " - Sex:"
        + all_series["sex_name"]
        + " - Age:"
        + all_series["age_name"]
        + " ("
        + all_series["metric_name"]
        + ")"
    )

    for i, row in tqdm(all_series.iterrows(), total=len(all_series)):
        data_filtered = pd.DataFrame(
            mdf_merged[(mdf_merged.variable_name == row["variable_name"])]
        )
        print(data_filtered.shape)


for data in df_merged:
    print(data.shape)


def get_variables() -> None:
    df_merged = pd.read_csv(os.path.join(INPATH, "all_data.csv"), chunksize=10 ** 5)
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
        #  print(chunk["variable_name"])
        var_list.append(chunk["variable_name"])
        df_list.append(chunk)
    var_list = pd.concat(var_list).drop_duplicates()
    return var_list, df_list


var_list, df_list = get_variables()


for chunk in df_list:
    print(chunk.shape)


df_merged_it = pd.read_csv(os.path.join(INPATH, "all_data.csv"), iterator=True)


df_merged_it.get_chunk(10)

if __name__ == "__main__":
    main()
