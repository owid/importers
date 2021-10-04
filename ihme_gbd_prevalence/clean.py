import os
import json
import pandas as pd
import numpy as np

from ihme_gbd_prevalence import (
    INPATH,
    DATASET_NAME,
    DATASET_AUTHORS,
    DATASET_VERSION,
    OUTPATH,
    DATASET_RETRIEVED_DATE,
    CONFIGPATH,
)


def main() -> None:
    create_datasets()
    create_sources()
    get_variables()
    create_variables_datapoints()
    create_distinct_entities()


def clean_datasets(
    DATASET_NAME: str, DATASET_AUTHORS: str, DATASET_VERSION: str
) -> pd.DataFrame:
    """Constructs a dataframe where each row represents a dataset to be
    upserted.
    Note: often, this dataframe will only consist of a single row.
    """
    data = [
        {"id": 0, "name": f"{DATASET_NAME} - {DATASET_AUTHORS} ({DATASET_VERSION})"}
    ]
    df = pd.DataFrame(data)
    return df


def create_datasets() -> pd.DataFrame:
    df_datasets = clean_datasets(DATASET_NAME, DATASET_AUTHORS, DATASET_VERSION)
    assert (
        df_datasets.shape[0] == 1
    ), f"Only expected one dataset in {os.path.join(OUTPATH, 'datasets.csv')}."
    print("Creating datasets csv...")
    df_datasets.to_csv(os.path.join(OUTPATH, "datasets.csv"), index=False)
    return df_datasets


def create_sources() -> None:

    source_description = {
        "dataPublishedBy": "Global Burden of Disease Collaborative Network. Global Burden of Disease Study 2019 (GBD 2019) Results. Seattle, United States: Institute for Health Metrics and Evaluation (IHME), 2021.",
        "dataPublisherSource": "Institute for Health Metrics and Evaluation",
        "link": "http://ghdx.healthdata.org/gbd-results-tool",
        "retrievedDate": DATASET_RETRIEVED_DATE,
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
    df_sources.to_csv(os.path.join(OUTPATH, "sources.csv"), index=False)


def get_variables() -> None:
    if not os.path.isfile(
        os.path.join(INPATH, "all_data_with_var.csv")
    ) and os.path.isfile(os.path.join(INPATH, "all_variables.csv")):
        var_list = []
        df_merged = pd.read_csv(
            os.path.join(INPATH, "all_data_filtered.csv")
        )  # working through the data 1mil rows at a time
        df_merged["variable_name"] = [
            p1 + " - " + p2 + " - Sex: " + p3 + " - Age: " + p4 + " (" + p5 + ")"
            for p1, p2, p3, p4, p5 in zip(
                df_merged["measure_name"],
                df_merged["cause_name"],
                df_merged["sex_name"],
                df_merged["age_name"],
                df_merged["metric_name"],
            )
        ]
        var_list = df_merged["variable_name"].drop_duplicates()
        var_list.to_csv(os.path.join(INPATH, "all_variables.csv"), index=False)
        df_merged.to_csv(os.path.join(INPATH, "all_data_with_var.csv"), index=False)


def get_metric_value():
    if row["metric_name"] == "Percent":
        return str(float(row["val"]) * 100)
    else:
        return row["val"]


def create_variables_datapoints() -> None:

    var_list = pd.read_csv(os.path.join(INPATH, "all_variables.csv"))[
        "variable_name"
    ].to_list()
    fields = [
        "variable_name",
        "location_name",
        "metric_name",
        "year",
        "val",
    ]  # this is a very large dataframe but we only need five columns from it so we'll just read those ones in

    df = pd.read_csv(os.path.join(INPATH, "all_data_with_var.csv"), usecols=fields)

    variable_idx = 0
    variables = pd.DataFrame()

    units_dict = {"Percent": "%", "Rate": "", "Number": ""}

    entity2owid_name = (
        pd.read_csv(os.path.join(CONFIGPATH, "standardized_entity_names.csv"))
        .set_index("Country")
        .squeeze()
        .to_dict()
    )

    for var in var_list:
        print(var)
        var_df = df[df["variable_name"] == var]

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

        var_df["location_name"] = var_df["location_name"].apply(
            lambda x: entity2owid_name[x]
        )

        if var_df["metric_name"].iloc[0] == "Percent":
            var_df["val"] = var_df["val"] * 100

        var_df[["location_name", "year", "val"]].rename(
            columns={"location_name": "country", "val": "value"}
        ).to_csv(
            os.path.join(OUTPATH, "datapoints", "datapoints_%d.csv" % variable_idx),
            index=False,
        )

        variable_idx += 1

    variables.to_csv(os.path.join(OUTPATH, "variables.csv"), index=False)


def create_distinct_entities() -> None:
    df_distinct_entities = pd.read_csv(
        os.path.join(CONFIGPATH, "standardized_entity_names.csv")
    )
    df_distinct_entities = df_distinct_entities[["Our World In Data Name"]].rename(
        columns={"Our World In Data Name": "name"}
    )

    df_distinct_entities.to_csv(
        os.path.join(OUTPATH, "distinct_countries_standardized.csv"), index=False
    )


if __name__ == "__main__":
    main()
