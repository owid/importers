"""Functions to fetch data from the United States Environmental Protection Agency (EPA) and create various datasets.

"""

import argparse
import os
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.error import HTTPError

from climate_change.src import READY_DIR

# Convert miles squared to kilometers squared.
MILES_SQUARED_TO_KM_SQUARED = (1.609344)**2


def get_downloadable_file(source_page: str) -> str:
    soup = BeautifulSoup(requests.get(source_page).content, "html.parser")
    source_url = soup.find(class_="download-data").find("a")["href"]
    return "https://www.epa.gov" + source_url


def decimal_date_to_date(year: int) -> str:
    return (
        pd.to_datetime(year, format="%Y") + pd.Timedelta(days=(year % 1) * 364.2425)
    ).date()


def process_ocean_heat_content_file(depth: str, source_url: str) -> pd.DataFrame:
    df = read_csv(source_url, skiprows=6, encoding_errors="ignore")
    header = [
        f"{depth}_ocean_heat_content_{col.lower().replace('/', '')}"
        if col != "Year"
        else "year"
        for col in df.columns
    ]
    df.columns = header
    return df.assign(location="World")


def ocean_heat_content():
    # latest_url = get_downloadable_file(
    #     "https://www.epa.gov/climate-indicators/climate-change-indicators-ocean-heat"
    # )
    # depths = {
    #     "700m": latest_url,
    #     "2000m": latest_url.replace("fig-1", "fig-2"),
    # }
    # To get the latest URL, go to the above link, right click on the spreadsheet icon below the figure for 700m,
    # and copy link address.
    # Idem for figure for 2000m.
    depths = {
        "700m": "https://www.epa.gov/sites/default/files/2021-04/ocean-heat_fig-1.csv",
        "2000m": "https://www.epa.gov/sites/default/files/2021-04/ocean-heat_fig-2.csv",
    }
    for k, v in depths.items():
        output_file = os.path.join(READY_DIR, f"epa_{k}-ocean-heat-content.csv")
        process_ocean_heat_content_file(depth=k, source_url=v).to_csv(output_file, index=False)


def read_csv(source_url: str, *args, **kwargs):
    # Wrapper to avoid 403 error.
    try:
        df = pd.read_csv(source_url, *args, **kwargs)
    except HTTPError:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'}
        text = requests.get(source_url, headers=headers).text
        df = pd.read_csv(StringIO(text), *args, **kwargs)

    return df


def antarctic_sea_ice():
    output_file = os.path.join(READY_DIR, "epa_antarctic-sea-ice.csv")
    # latest_url = get_downloadable_file(
    #     "https://www.epa.gov/climate-indicators/climate-change-indicators-antarctic-sea-ice"
    # )
    # To get the latest URL, go to the above link, right click on the spreadsheet icon below the figure,
    # and copy link address.
    latest_url = "https://www.epa.gov/system/files/other-files/2022-07/antarctic-sea-ice-fig-1.csv"
    df = (
        read_csv(latest_url, skiprows=6, encoding_errors="ignore")
        .rename(
            columns={
                "Year": "year",
                "February": "antarctic_sea_ice_february",
                "September": "antarctic_sea_ice_september",
            }
        )
        .assign(location="Antarctica")
    )
    df[["antarctic_sea_ice_february", "antarctic_sea_ice_september"]] *= MILES_SQUARED_TO_KM_SQUARED
    df[
        [
            "year",
            "location",
            "antarctic_sea_ice_february",
            "antarctic_sea_ice_september",
        ]
    ].sort_values("year").to_csv(output_file, index=False)


def mass_balance_global_glaciers():
    output_file = os.path.join(READY_DIR, "epa_mass-balance-global-glaciers.csv")
    # latest_url = get_downloadable_file(
    #     "https://www.epa.gov/climate-indicators/climate-change-indicators-glaciers"
    # ).replace("fig-1", "fig-2")
    # To get the latest URL, go to the above link, **SELECT FIGURE 2**,
    # right click on the spreadsheet icon below the figure,
    # and copy link address.
    latest_url = "https://www.epa.gov/sites/default/files/2021-03/glaciers_fig-2.csv"
    df = (
        read_csv(latest_url, skiprows=6, encoding_errors="ignore")
        .rename(columns={"Year": "year"})
        .melt(id_vars="year", var_name="location", value_name="cumulative_mass_balance")
        .dropna(subset=["cumulative_mass_balance"])
    )
    df.to_csv(output_file, index=False)


def snow_cover_north_america():
    output_file = os.path.join(READY_DIR, "epa_snow-cover-north-america.csv")
    # latest_url = get_downloadable_file(
    #     "https://www.epa.gov/climate-indicators/climate-change-indicators-snow-cover"
    # )
    # To get the latest URL, go to the above link, right click on the spreadsheet icon below the figure,
    # and copy link address.
    latest_url = "https://www.epa.gov/sites/default/files/2021-03/snow-cover_fig-1.csv"
    df = (
        read_csv(latest_url, skiprows=6, encoding_errors="ignore")
        .rename(columns={"Year": "year", "Average mi^2": "snow_cover_north_america"})
        .assign(location="North America")
    )
    df.to_csv(output_file, index=False)


def antarctica_greenland_ice_sheet_loss():
    output_file = os.path.join(READY_DIR, "epa_antarctica-greenland-ice-sheet-loss.csv")
    # latest_url = get_downloadable_file(
    #     "https://www.epa.gov/climate-indicators/climate-change-indicators-ice-sheets"
    # )
    # To get the latest URL, go to the above link, right click on the spreadsheet icon below the figure,
    # and copy link address.
    latest_url = "https://www.epa.gov/sites/default/files/2021-04/ice_sheets_fig-1.csv"
    ice_mass = (
        read_csv(
            latest_url,
            skiprows=6,
            encoding_errors="ignore",
            usecols=[
                "Year",
                "NASA - Antarctica land ice mass",
                "NASA - Greenland land ice mass",
            ],
        )
        .melt(id_vars="Year", var_name="location", value_name="land_ice_mass")
        .dropna()
    )
    ice_mass.loc[
        ice_mass.location.str.contains("Antarctica"), "location"
    ] = "Antarctica"
    ice_mass.loc[ice_mass.location.str.contains("Greenland"), "location"] = "Greenland"

    change = (
        read_csv(
            latest_url,
            skiprows=6,
            encoding_errors="ignore",
            usecols=[
                "Year",
                "IMBIE - Antarctica cumulative ice mass change",
                "IMBIE - Greenland cumulative ice mass change",
            ],
        )
        .melt(id_vars="Year", var_name="location", value_name="ice_mass_change")
        .dropna()
    )
    change.loc[change.location.str.contains("Antarctica"), "location"] = "Antarctica"
    change.loc[change.location.str.contains("Greenland"), "location"] = "Greenland"

    df = pd.merge(ice_mass, change, on=["Year", "location"], how="outer")
    df["date"] = df.Year.apply(decimal_date_to_date)
    df.drop(columns="Year").sort_values(["date", "location"]).to_csv(
        output_file, index=False
    )


def main():
    ocean_heat_content()
    antarctic_sea_ice()
    mass_balance_global_glaciers()
    snow_cover_north_america()
    antarctica_greenland_ice_sheet_loss()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    main()
