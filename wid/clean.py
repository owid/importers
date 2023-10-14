"""
Usage: python -m wid.clean
"""

import json
import os
import shutil

import pandas as pd
from pandas.core.frame import DataFrame
from tqdm import tqdm

from wid import (
    DATASET_NAME,
    DATASET_AUTHORS,
    DATASET_VERSION,
    DATASET_LINK,
    DATASET_ADDITIONAL_INFO,
    DATASET_RETRIEVED_DATE,
    CONFIGPATH,
    INPATH,
    OUTPATH,
)


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
    dataset = pd.DataFrame({"id": 0, "name": [DATASET_NAME + " " + DATASET_VERSION]})
    dataset.to_csv(os.path.join(OUTPATH, "datasets.csv"), index=False)
    return dataset


def fix_variable_codes(vars: pd.DataFrame) -> pd.DataFrame:
    # In the 2020 version of WID, variable codes are (mistakenly) presented differently in the data
    # and metadata files. The code `spllin992m` in the data will instead be reference as
    # `spllinm992` in the metadata. We therefore rearrange the variable codes to match the format
    # used in data file (xxxxxxNNNx).
    vars["variable"] = (
        vars.variable.str.slice(0, 6)
        + vars.variable.str.slice(-3)
        + vars.variable.str.slice(6, 7)
    )
    return vars


def create_description(vars: pd.DataFrame) -> pd.DataFrame:
    vars = vars.reset_index(drop=True)
    vars["description"] = vars.description.fillna("")

    vars.loc[
        vars.longpop
        == "The base unit is the individual (rather than the household). This is equivalent to assuming no sharing of resources within couples.",
        "longpop",
    ] = pd.NA
    vars.loc[vars.longpop.notnull(), "description"] = (
        vars.description + "\n" + vars.longpop
    )

    vars["longage"] = vars.longage.str.replace(
        "individuals of in the", "individuals in the"
    )
    vars.loc[
        vars.longage == "The population is comprised of individuals of all ages.",
        "longage",
    ] = pd.NA
    vars.loc[vars.longage.notnull(), "description"] = (
        vars.description + "\n" + vars.longage
    )

    vars["description"] = vars.description.str.strip()

    return vars.drop(columns=["longpop", "longage"])


def infer_unit(vars: pd.DataFrame) -> pd.DataFrame:
    vars.loc[(vars.unit == "share") | (vars.unit.str.contains("%")), "short_unit"] = "%"
    vars["unit"] = vars.short_unit
    return vars.drop_duplicates()


def clean_sources(vars: pd.DataFrame) -> pd.DataFrame:
    multi_sources = vars[["code", "source"]].groupby("code", as_index=False).size()
    multi_sources = multi_sources[multi_sources["size"] > 1]
    vars.loc[
        vars.code.isin(multi_sources.code), "source"
    ] = "Data compiled by WID.world from multiple sources"

    vars["source"] = (
        vars.source.str.replace(r"\[URL_LINK\][^[]+\[.URL_LINK\]", " ", regex=True)
        .str.replace(r"\[.?URL(_TEXT)?\]", "", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"[;\.]$", "", regex=True)
        .str.strip()
        .fillna(f"{DATASET_NAME} ({DATASET_VERSION})")
    )
    return vars


def preprocess_sources(sources: pd.DataFrame) -> pd.DataFrame:
    return (
        sources.drop_duplicates()
        .reset_index(drop=True)
        .reset_index()
        .rename(columns={"index": "source_id"})
    )


def prepare_variables() -> pd.DataFrame:
    print("Preparing variables…")
    var_files = [file for file in os.listdir(INPATH) if "_metadata_" in file]
    vars = []
    for var_file in tqdm(var_files):
        vars.append(
            pd.read_csv(
                os.path.join(INPATH, var_file),
                sep=";",
                encoding_errors="ignore",
                na_values="",
            ).drop(
                columns=[
                    "country",
                    "age",
                    "pop",
                    "countryname",
                    "technicaldes",
                    "longtype",
                    "method",
                ]
            )
        )
    vars = pd.concat(vars)

    vars = fix_variable_codes(vars)

    vars = vars.rename(columns={"variable": "code", "simpledes": "description"})

    # Create full variable name
    vars["name"] = vars.apply(
        lambda x: f"{x.shortname} ({x.shorttype.lower()}, {x.shortpop.lower()}, {x.shortage.lower()}) [{x.code}]",
        axis=1,
    )
    vars = vars.drop(columns=["shortname", "shorttype", "shortpop", "shortage"])

    vars = create_description(vars)
    vars = infer_unit(vars)
    vars = clean_sources(vars)
    vars = vars.drop_duplicates()
    assert vars.code.is_unique

    sources = preprocess_sources(vars[["source"]])
    vars = pd.merge(vars, sources, on="source", validate="many_to_one").drop(
        columns="source"
    )
    sources = postprocess_sources(sources)
    sources.to_csv(os.path.join(OUTPATH, "sources.csv"), index=False)
    vars = vars.sort_values("code").assign(id=range(0, len(vars)), dataset_id=0)
    return vars


def postprocess_sources(sources: pd.DataFrame) -> pd.DataFrame:
    print("Preparing sources…")

    sources = sources.rename(columns={"source": "name", "source_id": "id"}).assign(
        dataset_id=pd.NA,
        description=json.dumps(
            {
                "dataPublishedBy": DATASET_AUTHORS,
                "dataPublisherSource": DATASET_NAME,
                "additionalInfo": DATASET_ADDITIONAL_INFO,
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
                    "additionalInfo": DATASET_ADDITIONAL_INFO,
                }
            ),
        }
    )

    return pd.concat([sources, main_source])


def remove_multiple_observations(df: pd.DataFrame) -> pd.DataFrame:
    # Some variables have multiple observations per country-year, for example because they have data
    # for different percentiles. For the first version of this importer (September 2021), we do not
    # process these variables given the uncertainty of their relevance for OWID.
    obs_tally = df.groupby(["variable", "year"], as_index=False).size()
    multiple_variables = obs_tally[obs_tally["size"] > 1].variable.unique()
    return df[-df.variable.isin(multiple_variables)]


def prepare_datapoints(vars: pd.DataFrame) -> None:
    print("Preparing datapoints…")

    data_files = [file for file in os.listdir(INPATH) if "_data_" in file]
    df = []
    for var_file in tqdm(data_files):
        tmp = (
            pd.read_csv(
                os.path.join(INPATH, var_file),
                sep=";",
                encoding_errors="ignore",
                na_values="",
            )
            .pipe(remove_multiple_observations)
            .drop(columns=["percentile", "age", "pop"])
        )
        df.append(tmp)
    df = pd.concat(df).rename(columns={"variable": "code"})

    assert len(df) == len(
        df[["country", "code", "year"]].drop_duplicates()
    ), "Concatenated dataframe has more than 1 value per variable-country-year"

    # Standardize country names
    wid_country_codes = pd.read_csv(
        os.path.join(INPATH, "WID_countries.csv"),
        usecols=["alpha2", "shortname"],
        na_values="",
        sep=";",
    ).dropna()
    wid_country_codes = wid_country_codes[-wid_country_codes.alpha2.str.contains("-")]
    df = pd.merge(
        df,
        wid_country_codes,
        left_on="country",
        right_on="alpha2",
        validate="many_to_one",
    ).drop(columns=["country", "alpha2"])

    country_mapping = pd.read_csv(
        os.path.join(CONFIGPATH, "standardized_entity_names.csv")
    )
    df = (
        pd.merge(df, country_mapping, left_on="shortname", right_on="Country")
        .drop(columns=["shortname", "Country"])
        .rename(columns={"Our World In Data Name": "country"})
    )

    # Map variable codes to variable ids
    shape_before = df.shape
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
    print("Writing datapoints…")
    for id in tqdm(df.id.unique()):
        datapoints = df[df.id == id][["country", "year", "value"]]
        datapoints.to_csv(
            os.path.join(OUTPATH, "datapoints", f"datapoints_{id}.csv"), index=False
        )

    return df.id.unique()


def main() -> None:
    delete_output()
    mk_output_dir()
    prepare_dataset()
    vars = prepare_variables()
    processed_var_ids = prepare_datapoints(vars)
    vars[vars.id.isin(processed_var_ids)].to_csv(
        os.path.join(OUTPATH, "variables.csv"), index=False
    )
    print("All done.")


if __name__ == "__main__":
    main()
