import io
import json
import os
import os.path
import re
import shutil
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import requests


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
                if os.path.exists(os.path.join(inpath, "csv", "citation.txt")):
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
        "dataPublisherSource": "Institute for Health Metrics and Evaluation, Global Burden of Disease (2019)",
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
        df = create_var_name(df)
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


def create_units(df: pd.DataFrame) -> pd.DataFrame:

    conds = [
        (
            (df["measure"] == "DALYs (Disability-Adjusted Life Years)")
            & (df["metric"] == "Rate")
        ),
        (
            (df["measure"] == "DALYs (Disability-Adjusted Life Years)")
            & (df["metric"] == "Number")
        ),
        (
            (df["measure"] == "DALYs (Disability-Adjusted Life Years)")
            & (df["metric"] == "Percent")
        ),
        ((df["measure"] == "Deaths") & (df["metric"] == "Number")),
        ((df["measure"] == "Deaths") & (df["metric"] == "Rate")),
        ((df["measure"] == "Deaths") & (df["metric"] == "Percent")),
        ((df["measure"] == "Prevalence") & (df["metric"] == "Number")),
        ((df["measure"] == "Prevalence") & (df["metric"] == "Rate")),
        ((df["measure"] == "Prevalence") & (df["metric"] == "Percent")),
        ((df["measure"] == "Incidence") & (df["metric"] == "Number")),
        ((df["measure"] == "Incidence") & (df["metric"] == "Rate")),
        ((df["measure"] == "Incidence") & (df["metric"] == "Percent")),
    ]

    choices = [
        "DALYs per 100,000 people",
        "DALYs",
        "%",
        "deaths",
        "deaths per 100,000 people",
        "%",
        "",
        "",
        "%",
        "",
        "",
        "%",
    ]
    df["metric"] = np.select(conds, choices)
    return df


def create_variables(
    inpath: str,
    filter_fields: list,
    outpath: str,
    clean_all_vars: bool,
    configpath: str,
    calculate_owid_vars: str,
) -> pd.DataFrame:
    """Iterating through each variable and pulling out the relevant datapoints.
    Formatting the data for the variables.csv file and outputting the associated csv files into the datapoints folder."""
    paths = list_input_files(inpath)

    if "rei" in filter_fields:
        r = re.compile(r"measure|sex|age|cause|rei|metric|year")
        rd = re.compile(r"measure|sex|age|cause|rei")
    else:
        r = re.compile(r"measure|sex|age|cause|metric|year")
        rd = re.compile(r"measure|sex|age|cause")

    fields = list(filter(r.match, filter_fields))

    field_drop = list(filter(rd.match, fields))
    field_drop = [w.replace("_name", "") for w in field_drop]

    ch_vars = list_variables_to_clean(configpath)

    vars_out = []
    print("Creating variables.csv")
    for path in paths:
        print(path)
        df = pd.read_csv(path, usecols=fields)
        df = create_var_name(df)
        df = create_units(df)
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
            df_t = df_t.rename(columns={"metric": "unit"})
            assert "unit" in df_t.columns

            df_t["short_unit"] = np.where(df_t["unit"] == "%", "%", "")
            vars_out.append(df_t)

    df = pd.concat(vars_out)
    df_t = df.join(df.groupby("name")["year"].agg(["min", "max"]), on="name")
    df_t["min"] = df_t["min"].astype("str")
    df_t["max"] = df_t["max"].astype("str")
    df_t["timespan"] = df_t["min"] + " - " + df_t["max"]
    df_t = df_t.drop(["min", "max", "year"], axis=1).drop_duplicates()

    if calculate_owid_vars:
        df_t = add_owid_variables(df_t, configpath)

    df_t["id"] = range(0, len(df_t))
    df_t.to_csv(os.path.join(outpath, "variables.csv"), index=False)
    return df_t


def clean_units_and_values(df: pd.DataFrame) -> pd.DataFrame:

    df["val"][df["metric"] == "Percent"] = df["val"][df["metric"] == "Percent"] * 100
    df["val"] = df["val"].astype(float).round(2)
    df["val"][
        (df["measure"].isin(["Prevalence", "Incidence", "Deaths"]))
        & (df["metric"] == "Number")
    ] = round(
        df["val"][
            (df["measure"].isin(["Prevalence", "Incidence", "Deaths"]))
            & (df["metric"] == "Number")
        ],
        0,
    )
    return df


def remove_regions(df: pd.DataFrame) -> pd.DataFrame:
    regions_to_remove = [
        "Four World Regions",
        "World Bank Income Levels",
        "World Bank Regions",
        "WHO region",
        "Low SDI",
        "Low-middle SDI",
        "Middle SDI",
        "High-middle SDI",
        "High SDI",
        "Central Europe, Eastern Europe and Central Asia",
        "Nordic Region",
        "African Union",
        "Africa",
        "Asia",
        "America",
        "Europe",
        "Southern Latin America",
        "Latin America and Caribbean",
        "Caribbean",
        "Central Latin America",
        "Commonwealth Low Income",
        "Commonwealth Middle Income",
        "East Asia",
        "Central Sub-Saharan Africa",
        "Western Sub-Saharan Africa",
        "Eastern Sub-Saharan Africa",
        "Sub-Saharan Africa",
        "Australasia",
        "Central Asia",
        "Eastern Europe",
        "Southern Sub-Saharan Africa",
        "Central Europe",
        "High-income North America",
        "Southeast Asia, East Asia, and Oceania",
        "Southeast Asia",
        "Commonwealth High Income",
        "North Africa and Middle East",
        "Tropical Latin America",
        "Andean Latin America",
        "European Union",
        "Oceania",
        "Commonwealth",
        "High-income",
        "Central Europe, Eastern Europe, and Central Asia",
        "High-income Asia Pacific",
        "South Asia",
        "Western Europe",
    ]

    df = df[~df["country"].isin(regions_to_remove)]

    return df


def create_datapoints(
    vars: pd.DataFrame,
    inpath: str,
    parent_dir: str,
    configpath: str,
    outpath: str,
    calculate_owid_vars: str,
) -> None:

    print("Creating datapoints")
    paths = list_input_files(inpath)

    entity2owid_name = (
        pd.read_csv(os.path.join(parent_dir, "standardized_entity_names.csv"))
        .set_index("Country")
        .squeeze()
        .to_dict()
    )

    for path in paths:
        df = pd.read_csv(path)
        df = create_var_name(df)
        print(df["name"][0])
        df = clean_units_and_values(df)
        df_m = df.merge(vars[["name", "id"]], on="name")
        df_m = df_m[["location", "year", "val", "id"]].rename(
            columns={"location": "country", "val": "value"}
        )
        df_m = remove_regions(df=df_m)
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
    if calculate_owid_vars:
        calc_owid_var_data(vars, outpath, configpath)


def calc_owid_var_data(vars: pd.DataFrame, outpath: str, configpath: str) -> None:

    f = open(os.path.join(configpath, "variables_to_sum.json"))
    vars_to_calc = json.load(f)

    for var in vars_to_calc:
        print(var)
        id = vars.loc[vars["name"] == var].id
        assert (
            vars["name"] == var
        ).any(), "%s not in list of variables, check spelling!" % (var)
        vars_to_sum = vars[vars.name.isin(vars_to_calc[var])].id.to_list()
        df_sum = []
        for file in vars_to_sum:
            df = pd.read_csv(
                os.path.join(outpath, "datapoints", "datapoints_%d.csv" % file),
                index_col=None,
                header=0,
            )
            df["id"] = file
            df_sum.append(df)
        df = pd.concat(df_sum, ignore_index=True)
        df = df.drop_duplicates()
        df.groupby(["country", "year"])["value"].sum().reset_index().to_csv(
            os.path.join(outpath, "datapoints", "datapoints_%d.csv" % id)
        )


def create_distinct_entities(parent_dir: str, outpath: str) -> None:
    """Creating a list of distinct entities for use in upserting to the grapher db"""
    df_distinct_entities = pd.read_csv(
        os.path.join(parent_dir, "standardized_entity_names.csv")
    )
    df_distinct_entities = df_distinct_entities[["Our World In Data Name"]].rename(
        columns={"Our World In Data Name": "name"}
    )

    df_distinct_entities.to_csv(
        os.path.join(outpath, "distinct_countries_standardized.csv"), index=False
    )


def create_var_name(df: pd.DataFrame) -> pd.Series:

    df.columns = df.columns.str.replace(r"_name$", "", regex=True)
    # For risk factor variables we want to include the risk factor and the cause of death so need a slightly different variable format

    age_dict = {
        "Early Neonatal": "0-6 days",
        "Late Neonatal": "7-27 days",
        "Post Neonatal": "28-364 days",
        "1 to 4": "1-4 years",
    }

    df = df.replace({"age": age_dict}, regex=False)

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
    else:
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

    assert "name" in df.columns
    return df


def add_owid_variables(vars: pd.DataFrame, configpath: str) -> pd.DataFrame:

    f = open(os.path.join(configpath, "variables_to_sum.json"))
    vars_to_calc = json.load(f)

    vars_out = []
    for item in vars_to_calc:
        print(item)
        assert (vars.name == vars_to_calc[item][0]).any()
        var_out = vars[vars.name == vars_to_calc[item][0]]
        var_out["name"] = item
        var_out[
            "description"
        ] = f"Variable calculated by OWID: the sum of {vars_to_calc[item]}"
        vars_out.append(var_out)

    vars_out = pd.concat(vars_out)

    vars = pd.concat([vars, vars_out])
    return vars
