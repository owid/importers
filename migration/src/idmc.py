from operator import index
import pandas as pd
import requests
import json

from migration.src.utils import is_number, standardise_countries, owid_population


def download_data() -> None:
    r = requests.get("https://api.idmcdb.org/api/displacement_data?ci=IDMCWSHSOLO009")
    assert r.ok
    data = json.loads(r.content)
    df = pd.json_normalize(data["results"])
    return df


def annual_internal_displacement_conflict():
    df = download_data()
    df = df[["geo_name", "year", "conflict_new_displacements_raw"]].dropna()
    df["geo_name"] = standardise_countries(df["geo_name"])
    df["conflict_new_displacements_raw"] = df["conflict_new_displacements_raw"].astype(
        int
    )
    df.rename(
        columns={
            "geo_name": "Country",
            "year": "Year",
            "conflict_new_displacements_raw": "idmc_annual_internal_displacement_conflict",
        },
        inplace=True,
    )
    df["idmc_annual_internal_displacement_conflict"] = df.groupby("Country")[
        "idmc_annual_internal_displacement_conflict"
    ].transform(lambda x: x.rolling(5, 1).mean())
    df.to_csv(
        "migration/ready/idmc_annual_internal_displacement_conflict.csv", index=False
    )

    return df


def share_annual_internal_displacement_conflict() -> pd.DataFrame:
    displaced = annual_internal_displacement_conflict()
    population = owid_population()
    displaced = displaced.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    displaced["idmc_share_annual_internal_displacement_conflict"] = (
        displaced["idmc_annual_internal_displacement_conflict"]
        / displaced["Population"]
    )
    displaced = displaced[
        displaced["idmc_share_annual_internal_displacement_conflict"].apply(is_number)
    ]
    displaced["idmc_share_annual_internal_displacement_conflict"] = (
        displaced["idmc_share_annual_internal_displacement_conflict"] * 100
    )

    displaced = displaced[
        ["Year", "Country", "idmc_share_annual_internal_displacement_conflict"]
    ]
    displaced.to_csv(
        "migration/ready/omm_idmc_share_annual_internal_displacement_conflict.csv",
        index=False,
    )
    return displaced


def annual_internal_displacement_disaster():
    df = download_data()
    df = df[["geo_name", "year", "disaster_new_displacements_raw"]].dropna()
    df["geo_name"] = standardise_countries(df["geo_name"])
    df["disaster_new_displacements_raw"] = df["disaster_new_displacements_raw"].astype(
        int
    )
    df.rename(
        columns={
            "geo_name": "Country",
            "year": "Year",
            "disaster_new_displacements_raw": "idmc_annual_internal_displacement_disaster",
        },
        inplace=True,
    )
    df["idmc_annual_internal_displacement_disaster"] = df.groupby("Country")[
        "idmc_annual_internal_displacement_disaster"
    ].transform(lambda x: x.rolling(5, 1).mean())
    df.to_csv(
        "migration/ready/idmc_annual_internal_displacement_disaster.csv", index=False
    )

    return df


def share_annual_internal_displacement_disaster() -> pd.DataFrame:
    displaced = annual_internal_displacement_disaster()
    population = owid_population()
    displaced = displaced.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    displaced["idmc_share_annual_internal_displacement_disaster"] = (
        displaced["idmc_annual_internal_displacement_disaster"]
        / displaced["Population"]
    )
    displaced = displaced[
        displaced["idmc_share_annual_internal_displacement_disaster"].apply(is_number)
    ]
    displaced["idmc_share_annual_internal_displacement_disaster"] = (
        displaced["idmc_share_annual_internal_displacement_disaster"] * 100
    )

    displaced = displaced[
        ["Year", "Country", "idmc_share_annual_internal_displacement_disaster"]
    ]
    displaced.to_csv(
        "migration/ready/omm_idmc_share_annual_internal_displacement_disaster.csv",
        index=False,
    )
    return displaced


def total_internal_displacement_conflict():
    df = download_data()
    df = df[["geo_name", "year", "conflict_stock_displacement_raw"]].dropna()
    df["geo_name"] = standardise_countries(df["geo_name"])
    df["conflict_stock_displacement_raw"] = df[
        "conflict_stock_displacement_raw"
    ].astype(int)
    df.rename(
        columns={
            "geo_name": "Country",
            "year": "Year",
            "conflict_stock_displacement_raw": "idmc_total_internal_displacement_conflict",
        },
        inplace=True,
    )
    df["idmc_total_internal_displacement_conflict"] = df.groupby("Country")[
        "idmc_total_internal_displacement_conflict"
    ].transform(lambda x: x.rolling(5, 1).mean())
    df.to_csv(
        "migration/ready/idmc_total_internal_displacement_conflict.csv", index=False
    )

    return df


def share_total_internal_displacement_conflict() -> pd.DataFrame:
    displaced = total_internal_displacement_conflict()
    population = owid_population()
    displaced = displaced.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    displaced["share_idmc_total_internal_displacement_conflict"] = (
        displaced["idmc_total_internal_displacement_conflict"] / displaced["Population"]
    )
    displaced = displaced[
        displaced["share_idmc_total_internal_displacement_conflict"].apply(is_number)
    ]
    displaced["share_idmc_total_internal_displacement_conflict"] = (
        displaced["share_idmc_total_internal_displacement_conflict"] * 100
    )

    displaced = displaced[
        ["Year", "Country", "share_idmc_total_internal_displacement_conflict"]
    ]
    displaced.to_csv(
        "migration/ready/omm_share_idmc_total_internal_displacement_conflict.csv",
        index=False,
    )
    return displaced


def total_internal_displacement_disaster():
    df = download_data()
    df = df[["geo_name", "year", "disaster_stock_displacement_raw"]].dropna()
    df["geo_name"] = standardise_countries(df["geo_name"])
    df["disaster_stock_displacement_raw"] = df[
        "disaster_stock_displacement_raw"
    ].astype(int)
    df.rename(
        columns={
            "geo_name": "Country",
            "year": "Year",
            "disaster_stock_displacement_raw": "idmc_total_internal_displacement_disaster",
        },
        inplace=True,
    )
    df.to_csv(
        "migration/ready/idmc_total_internal_displacement_disaster.csv", index=False
    )

    return df


def share_total_internal_displacement_disaster() -> pd.DataFrame:
    displaced = total_internal_displacement_disaster()
    population = owid_population()
    displaced = displaced.merge(
        population,
        how="inner",
        left_on=["Country", "Year"],
        right_on=["Country", "Year"],
    )
    displaced["share_idmc_total_internal_displacement_disaster"] = (
        displaced["idmc_total_internal_displacement_disaster"] / displaced["Population"]
    )
    displaced = displaced[
        displaced["share_idmc_total_internal_displacement_disaster"].apply(is_number)
    ]
    displaced["share_idmc_total_internal_displacement_disaster"] = (
        displaced["share_idmc_total_internal_displacement_disaster"] * 100
    )

    displaced = displaced[
        ["Year", "Country", "share_idmc_total_internal_displacement_disaster"]
    ]
    displaced.to_csv(
        "migration/ready/omm_share_idmc_total_internal_displacement_disaster.csv",
        index=False,
    )
    return displaced
