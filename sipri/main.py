"""
Downloaded from https://www.sipri.org/databases/milex
-> "Data for all countries 1949–2020 (excel spreadsheet)"
"""

import pandas as pd

from owid import site

SIPRI_INPUT = "input/SIPRI-Milex-data-1949-2020_0.xlsx"
SIPRI_MAPPING = "config/sipri_country_standardized.csv"

CONTINENTS = site.get_chart_data(slug="continents-according-to-our-world-in-data")
NATO_COUNTRIES = "config/nato_member_states.csv"


def import_expenditure() -> pd.DataFrame:
    df = pd.read_excel(
        SIPRI_INPUT,
        sheet_name="Constant (2019) USD",
        skiprows=5,
        na_values=["xxx", ". .", ". . "],
    ).drop(columns=["Notes", "Unnamed: 1"])

    # Find end of table
    last_idx = df.index[df.Country.isna()].min()
    df = df.iloc[:last_idx]

    # Reshape table
    df = df.melt(
        id_vars="Country", var_name="year", value_name="military_expenditure"
    ).dropna(subset=["military_expenditure"])
    df = df[df.year != "2020 Current"]

    # Variable cleaning
    df["year"] = df.year.astype(int)
    df["Country"] = df.Country.str.strip()

    # Expenditure is originally expressed in millions of USD
    df["military_expenditure"] = (df.military_expenditure * 1000000).round()

    df[["Country"]].drop_duplicates().to_csv("~/Downloads/sipri1.csv", index=False)
    return df


def import_share_gdp() -> pd.DataFrame:
    df = pd.read_excel(
        SIPRI_INPUT, sheet_name="Share of GDP", skiprows=5, na_values=["xxx", ". ."]
    ).drop(columns="Notes")

    # Find end of table
    last_idx = df.index[df.Country.isna()].min()
    df = df.iloc[:last_idx]

    # Reshape table
    df = df.melt(
        id_vars="Country", var_name="year", value_name="military_expenditure_share_gdp"
    ).dropna(subset=["military_expenditure_share_gdp"])

    # Variable cleaning
    df["year"] = df.year.astype(int)
    df["Country"] = df.Country.str.strip()

    # Expenditure is originally expressed in percentage
    df["military_expenditure_share_gdp"] = (
        df.military_expenditure_share_gdp * 100
    ).round(2)

    df[["Country"]].drop_duplicates().to_csv("~/Downloads/sipri2.csv", index=False)
    return df


def import_per_capita() -> pd.DataFrame:
    df = pd.read_excel(
        SIPRI_INPUT, sheet_name="Per capita", skiprows=6, na_values=["xxx", ". .", ".."]
    ).drop(columns="Notes")

    # Find end of table
    last_idx = df.index[df.Country.isna()].min()
    df = df.iloc[:last_idx]

    # Reshape table
    df = df.melt(
        id_vars="Country", var_name="year", value_name="military_expenditure_per_capita"
    ).dropna(subset=["military_expenditure_per_capita"])

    # Variable cleaning
    df["year"] = df.year.astype(int)
    df["Country"] = df.Country.str.strip()
    df["military_expenditure_per_capita"] = df.military_expenditure_per_capita.round(2)

    df[["Country"]].drop_duplicates().to_csv("~/Downloads/sipri3.csv", index=False)
    return df


def map_countries(df: pd.DataFrame) -> pd.DataFrame:
    mapping = pd.read_csv(SIPRI_MAPPING).dropna(subset=["Our World In Data Name"])
    return (
        pd.merge(mapping, df, on="Country", validate="one_to_many")
        .rename(columns={"Our World In Data Name": "country"})
        .drop(columns=["Country"])
    )


def build_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """
    ONLY WORKS FOR ABSOLUTE EXPENDITURE (don't use for per-capita or share of GDP)
    Aggregate our own totals according to OWID continent definitions.
    """
    df = df[
        -df.country.isin(
            [
                "North America",
                "South America",
                "Europe",
                "Africa",
                "Asia",
                "Oceania",
                "World",
            ]
        )
    ]

    mapping = CONTINENTS[["entity", "value"]].rename(
        columns={"entity": "country", "value": "continent"}
    )

    continents = (
        df.merge(mapping, on="country", how="inner", validate="many_to_one")
        .drop(columns="country")
        .groupby(["continent", "year"], as_index=False)
        .sum()
        .rename(columns={"continent": "country"})
    )

    nato_members = pd.read_csv(NATO_COUNTRIES)
    nato = (
        df[df.country.isin(nato_members.country)][["year", "military_expenditure"]]
        .groupby("year")
        .sum()
        .reset_index()
        .assign(country="NATO members")
    )

    world = (
        df[["year", "military_expenditure"]]
        .groupby("year")
        .sum()
        .reset_index()
        .assign(country="World")
    )

    return pd.concat([df, continents, nato, world], ignore_index=True)


def main():

    expenditure = import_expenditure().pipe(map_countries).pipe(build_aggregates)
    share_gdp = import_share_gdp().pipe(map_countries)
    per_capita = import_per_capita().pipe(map_countries)

    df = (
        expenditure.merge(
            share_gdp, on=["country", "year"], validate="one_to_one", how="outer"
        )
        .merge(per_capita, on=["country", "year"], validate="one_to_one", how="outer")
        .sort_values(["country", "year"])
    )

    df.to_csv("output/SIPRI Military Expenditure Database.csv", index=False)


if __name__ == "__main__":
    main()
