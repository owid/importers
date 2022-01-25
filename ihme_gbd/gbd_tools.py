import requests
import os
import pandas as pd
import zipfile
import io
import json
from pathlib import Path
import shutil
import re

from ihme_gbd.ihme_gbd_risk import OUTPATH


def make_dirs(inpath: str, outpath: str, configpath: str) -> None:
    """
    Creating the necessary directories for the input, output and config files
    """
    Path(inpath).mkdir(parents=True, exist_ok=True)
    Path(outpath, "datapoints").mkdir(parents=True, exist_ok=True)
    Path(configpath).mkdir(parents=True, exist_ok=True)


def delete_datapoints(datapoints_dir) -> None:
    if os.path.exists(datapoints_dir):
        shutil.rmtree(datapoints_dir)
    os.makedirs(datapoints_dir)


def list_input_files(inpath: str) -> list:
    paths = []
    d = os.path.join(inpath, "csv")
    for path in os.listdir(d):
        full_path = os.path.join(d, path)
        if os.path.isfile(full_path):
            if full_path.endswith(".csv"):
                paths.append(full_path)
    return paths


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


def find_countries(country_col: str, inpath: str, entfile: str) -> None:

    paths = list_input_files(inpath)

    all_countries = []
    for path in paths:
        countries = pd.read_csv(path, usecols=[country_col]).drop_duplicates()
        all_countries.append(countries)

    all_count_cat = pd.concat(all_countries)
    all_count_cat.drop_duplicates().rename(columns={country_col: "Country"}).to_csv(
        entfile, index=False
    )


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


def get_variable_names(inpath: str, filter_fields: list) -> pd.Series:

    paths = list_input_files(inpath)
    if "rei" in filter_fields:
        r = re.compile(r"measure|sex|age|cause|rei|metric|year")
    else:
        r = re.compile(r"measure|sex|age|cause|metric|year")

    fields = list(filter(r.match, filter_fields))

    vars_out = []
    for path in paths:
        df = pd.read_csv(path, usecols=fields).drop_duplicates()
        df["name"] = create_var_name(df)
        vars_out.append(df["name"])

    vars = pd.concat(vars_out)

    return vars


def list_variables_to_clean(configpath: str):
    f = open(os.path.join(configpath, "variables_to_clean.json"))
    ch_vars = json.load(f)
    ch_vars = ch_vars["variables"]

    if os.path.isfile(os.path.join(configpath, "manual_variables_to_clean.json")):
        fm = open(os.path.join(configpath, "manual_variables_to_clean.json"))
        ch_vars_man = json.load(fm)
        ch_vars_man = ch_vars_man["variables"]
        ch_vars = ch_vars + ch_vars_man
        ch_vars = list(dict.fromkeys(ch_vars))
    return ch_vars


def create_variables(
    inpath: str,
    filter_fields: list,
    outpath: str,
    clean_all_vars: bool,
    configpath: str,
) -> pd.DataFrame:
    """Iterating through each variable and pulling out the relevant datapoints.
    Formatting the data for the variables.csv file and outputting the associated csv files into the datapoints folder."""

    units_dict = {"Percent": "%", "Rate": "", "Number": ""}

    paths = list_input_files(inpath)

    if "rei" in filter_fields:
        r = re.compile(r"measure|sex|age|cause|rei|metric|year")
    else:
        r = re.compile(r"measure|sex|age|cause|metric|year")

    fields = list(filter(r.match, filter_fields))

    if "rei" in filter_fields:
        rd = re.compile(r"measure|sex|age|cause|rei")
    else:
        rd = re.compile(r"measure|sex|age|cause")

    field_drop = list(filter(rd.match, fields))

    ch_vars = list_variables_to_clean(configpath)

    vars_out = []
    print("Creating variables.csv")
    for path in paths:
        df = pd.read_csv(path, usecols=fields).drop_duplicates()
        df["name"] = create_var_name(df)
        if not clean_all_vars:
            df = df[df["name"].isin(ch_vars)]
        if (
            df.shape[0]
            > 0  # check there are some rows left after the previous if statement
        ):
            df_t = df.drop(field_drop, axis=1).drop_duplicates()
            df_t["dataset_id"] = int(0)
            df_t["source_id"] = int(0)
            df_t[
                ["description", "code", "coverage", "display", "original_metadata"]
            ] = None
            if "metric_name" in df_t.columns:
                df_t = df_t.rename(columns={"metric_name": "unit"})
            if "metric" in df_t.columns:
                df_t = df_t.rename(columns={"metric": "unit"})
            assert "unit" in df_t.columns
            df_t["short_unit"] = df_t["unit"].map(units_dict)
            vars_out.append(df_t)

    df = pd.concat(vars_out)
    df_t = df.join(df.groupby("name")["year"].agg(["min", "max"]), on="name")
    df_t["min"] = df_t["min"].astype("str")
    df_t["max"] = df_t["max"].astype("str")
    df_t["timespan"] = df_t["min"] + " - " + df_t["max"]
    df_t = df_t.drop(["min", "max", "year"], axis=1).drop_duplicates()

    df_t = add_owid_variables(df_t)

    # df_t = df_t.drop_duplicates()
    df_t["id"] = range(0, len(df_t))
    df_t.to_csv(os.path.join(outpath, "variables.csv"), index=False)
    return df_t


def create_datapoints(
    vars: pd.DataFrame, inpath: str, configpath: str, outpath: str
) -> None:
    print("Creating datapoints")
    paths = list_input_files(inpath)

    entity2owid_name = (
        pd.read_csv(os.path.join(configpath, "standardized_entity_names.csv"))
        .set_index("Country")
        .squeeze()
        .to_dict()
    )

    for path in paths:
        print(path)
        df = pd.read_csv(path)
        df["name"] = create_var_name(df)

        if "metric_name" in df.columns:
            df["val"][df["metric_name"] == "Percent"] = (
                df["val"][df["metric_name"] == "Percent"] * 100
            )
        if "metric" in df.columns:
            df["val"][df["metric"] == "Percent"] = (
                df["val"][df["metric"] == "Percent"] * 100
            )

        df_m = df.merge(vars[["name", "id"]], on="name")
        if "location_name" in df_m.columns:
            df_m = df_m[["location_name", "year", "val", "id"]].rename(
                columns={"location_name": "country", "val": "value"}
            )
        if "location" in df_m.columns:
            df_m = df_m[["location", "year", "val", "id"]].rename(
                columns={"location": "country", "val": "value"}
            )
        df_m["country"] = df_m["country"].map(entity2owid_name)

        df_g = df_m.groupby("id")
        for name, group in df_g:
            if os.path.isfile(
                os.path.join(outpath, "datapoints", "datapoints_%d.csv" % name)
            ):
                group[["country", "year", "value"]].to_csv(
                    os.path.join(outpath, "datapoints", "datapoints_%d.csv" % name),
                    mode="a",
                    header=False,
                    index=False,
                )
            else:
                group[["country", "year", "value"]].to_csv(
                    os.path.join(outpath, "datapoints", "datapoints_%d.csv" % name),
                    index=False,
                )
    add_owid_var_data(vars, outpath=OUTPATH)


def add_owid_var_data(vars: pd.DataFrame, outpath: str) -> None:

    cancer_smoking_vars = [
        "Deaths - Cause: All causes - Risk: Smoking - Sex: Both - Age: Age-standardized (Percent)",
        "Deaths - Cause: All causes - Risk: Secondhand smoke - Sex: Both - Age: Age-standardized (Percent)",
    ]

    outdoor_air_pollution_vars = [
        "Deaths - Cause: All causes - Risk: Ambient particulate matter pollution - Sex: Both - Age: Age-standardized (Percent)",
        "Deaths - Cause: All causes - Risk: Ambient ozone pollution - Sex: Both - Age: Age-standardized (Percent)",
    ]

    if vars.name.isin(cancer_smoking_vars).sum() == 2:
        id_can = vars.loc[
            vars["name"]
            == "Deaths - Cause: All causes - Risk: Smoking - OWID - Sex: Both - Age: Age-standardized (Percent)"
        ].id
        dp = vars[vars.name.isin(cancer_smoking_vars)].id.to_list()
        df_can = []
        for file in dp:
            df = pd.read_csv(
                os.path.join(outpath, "datapoints", "datapoints_%d.csv" % file),
                index_col=None,
                header=0,
            )
            df["id"] = file
            df_can.append(df)
        df = pd.concat(df_can, ignore_index=True)
        df = df.drop_duplicates()
        df.groupby(["country", "year"])["value"].sum().reset_index().to_csv(
            os.path.join(outpath, "datapoints", "datapoints_%d.csv" % id_can)
        )

    if vars.name.isin(outdoor_air_pollution_vars).sum() == 2:
        id_oap = vars.loc[
            vars["name"]
            == "Deaths - Cause: All causes - Risk: Outdoor air pollution - OWID - Sex: Both - Age: Age-standardized (Percent)"
        ].id
        dp = vars[vars.name.isin(outdoor_air_pollution_vars)].id.to_list()
        df_can = []
        for file in dp:
            df = pd.read_csv(
                os.path.join(outpath, "datapoints", "datapoints_%d.csv" % file),
                index_col=None,
                header=0,
            )
            df["id"] = file
            df_can.append(df)
        df = pd.concat(df_can, ignore_index=True)
        df = df.drop_duplicates()
        df.groupby(["country", "year"])["value"].sum().reset_index().to_csv(
            os.path.join(outpath, "datapoints", "datapoints_%d.csv" % id_oap)
        )


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


def create_var_name(df: pd.DataFrame) -> pd.Series:

    if "cause_name" in df.columns:
        df["name"] = (
            df["measure_name"]
            + " - "
            + df["cause_name"]
            + " - Sex: "
            + df["sex_name"]
            + " - Age: "
            + df["age_name"]
            + " ("
            + df["metric_name"]
            + ")"
        )
    if "cause" in df.columns:
        df["name"] = (
            df["measure"]
            + " - "
            + df["cause"]
            + " - Sex: "
            + df["sex"]
            + " - Age: "
            + df["age"]
            + " ("
            + df["metric"]
            + ")"
        )
    # For risk factor variables we want to include the risk factor and the cause of death so need a slightly different variable format
    if "rei" in df.columns:
        df["name"] = (
            df["measure"]
            + " - Cause: "
            + df["cause"]
            + " - Risk: "
            + df["rei"]
            + " - Sex: "
            + df["sex"]
            + " - Age: "
            + df["age"]
            + " ("
            + df["metric"]
            + ")"
        )
    if "rei_name" in df.columns:
        df["name"] = (
            df["measure_name"]
            + " - Cause: "
            + df["cause_name"]
            + " - Risk: "
            + df["rei_name"]
            + " - Sex: "
            + df["sex_name"]
            + " - Age: "
            + df["age_name"]
            + " ("
            + df["metric_name"]
            + ")"
        )
    assert "name" in df.columns
    return df["name"]


def add_owid_variables(vars: pd.DataFrame) -> pd.DataFrame:
    cancer_smoking_vars = [
        "Deaths - Cause: All causes - Risk: Smoking - Sex: Both - Age: Age-standardized (Percent)",
        "Deaths - Cause: All causes - Risk: Secondhand smoke - Sex: Both - Age: Age-standardized (Percent)",
    ]
    outdoor_air_pollution_vars = [
        "Deaths - Cause: All causes - Risk: Ambient particulate matter pollution - Sex: Both - Age: Age-standardized (Percent)",
        "Deaths - Cause: All causes - Risk: Ambient ozone pollution - Sex: Both - Age: Age-standardized (Percent)",
    ]

    if vars.name.isin(cancer_smoking_vars).sum() == 2:
        cs_df = vars.loc[
            vars["name"]
            == "Deaths - Cause: All causes - Risk: Smoking - Sex: Both - Age: Age-standardized (Percent)"
        ]
        cs_df[
            "name"
        ] = "Deaths - Cause: All causes - Risk: Smoking - OWID - Sex: Both - Age: Age-standardized (Percent)"
        cs_df[
            "description"
        ] = "A variable calculated by OWID; the sum of 'Smoking' and 'Secondhand smoke'."
        vars = pd.concat([vars, cs_df])

    if vars.name.isin(outdoor_air_pollution_vars).sum() == 2:
        oap_df = vars.loc[
            vars["name"]
            == "Deaths - Cause: All causes - Risk: Ambient particulate matter pollution - Sex: Both - Age: Age-standardized (Percent)"
        ]
        oap_df[
            "name"
        ] = "Deaths - Cause: All causes - Risk: Outdoor air pollution - OWID - Sex: Both - Age: Age-standardized (Percent)"
        oap_df[
            "description"
        ] = "A variable calculated by OWID; the sum of 'Ambient particulate matter pollution' and 'Ambient ozone pollution'."
        vars = pd.concat([vars, oap_df])

    return vars
