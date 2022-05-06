"""Download and prepare dataset from the latest Global Hunger Index data release.

"""

import argparse
import os

import numpy as np
import pandas as pd
from owid import catalog

# Define common paths.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
GRAPHER_DIR = os.path.join(CURRENT_DIR, "grapher")
COUNTRIES_FILE = os.path.join(CURRENT_DIR, "config", "country_standardized.csv")
DATA_URL = "https://www.globalhungerindex.org/xlsx/2021.xlsx"
GHI_NAME = "Global Hunger Index (2021)"
OUTPUT_FILE = os.path.join(GRAPHER_DIR, GHI_NAME + ".csv")


def split_country_groups(data, grouped_countries_name):
    grouped_countries_filter = data["Country"] == grouped_countries_name
    grouped_countries = (
        grouped_countries_name.replace("*", "").replace("and ", "").split(", ")
    )
    clean = data[~grouped_countries_filter].reset_index(drop=True)
    for country in grouped_countries:
        added = data[data["Country"] == grouped_countries_name].replace(
            {grouped_countries_name: country}
        )
        clean = pd.concat([clean, added], ignore_index=True)

    return clean


def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


# List of entities that correspond to countries grouped in the same row.
GROUPED_COUNTRIES_CASES = [
    "Burundi, Comoros, South Sudan, and Syrian Arab Republic*",
    "Guinea, Guinea-Bissau, Niger, Uganda, Zambia, and Zimbabwe*",
]

COUNTRY_REMAPPING = {
    row["ghi_name"]: row["owid_name"]
    for _, row in pd.read_csv(COUNTRIES_FILE).iterrows()
}

SPECIAL_CASES_REMAPPING = {
    "<5": 2.5,
    "—": np.nan,
}


def main():
    print("Loading new GHI data.")
    data = (
        pd.read_excel(DATA_URL, sheet_name="2021 GHI Ranking", skiprows=2)
        .drop(columns=["Unnamed: 0", "Rank1"])
        .dropna(how="all")
    )

    print("Loading owid population dataset.")
    population = (
        catalog.find("population", namespace="owid")
        .load()
        .reset_index()
        .rename(
            columns={"country": "Country", "year": "Year", "population": "Population"}
        )[["Country", "Year", "Population"]]
    )

    print("Preparing data.")

    # Add new rows for all countries that have been grouped in the same row.
    clean = data.copy()
    for case in GROUPED_COUNTRIES_CASES:
        clean = split_country_groups(data=clean, grouped_countries_name=case)

    # Name countries following owid naming.
    clean["Country"] = (
        clean["Country"].str.replace("*", "", regex=False).replace(COUNTRY_REMAPPING)
    )

    # Ensure all countries have names in owid population dataset.
    assert (set(clean["Country"]) - set(population["Country"])) == set()

    # Reshape dataframe
    clean_melt = clean.melt(id_vars="Country", value_name=GHI_NAME, var_name="Year")
    clean_melt["Year"] = clean_melt["Year"].astype("int64")

    # Replace values given as ranges by the mean of the range.
    range_values = [
        value
        for value in clean_melt[GHI_NAME].unique()
        if not is_float(value)
        if "*" in value
    ]
    value_remapping = {
        value: np.array(value.replace("*", "").split("–")).astype(float).round().mean()
        for value in range_values
    }

    # Manually add other special cases.
    value_remapping.update(SPECIAL_CASES_REMAPPING)
    clean_melt[GHI_NAME] = clean_melt[GHI_NAME].replace(value_remapping)

    print(f"Saving dataset to file: {OUTPUT_FILE}")
    clean_melt.to_csv(OUTPUT_FILE, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and prepare dataset from the latest Global Hunger Index data release."
    )
    args = parser.parse_args()

    main()
