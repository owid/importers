"""Functions to fetch data from the National Oceanic and Atmospheric Administration (NOAA) and create various datasets.

"""

import argparse
import os
import requests

from bs4 import BeautifulSoup
import pandas as pd

from climate_change.src import READY_DIR


def process_concentration_file(
    gas: str, source_url: str, skiprows: int
) -> pd.DataFrame:
    df = pd.read_fwf(source_url, skiprows=skiprows)
    df["date"] = pd.to_datetime(
        df.year.astype(str) + "-" + df.month.astype(str) + "-15"
    )
    return (
        df[["date", "average", "trend"]]
        .rename(
            columns={
                # Monthly averaged concentrations.
                "average": f"monthly_{gas}_concentrations",
                # Yearly averaged concentrations.
                "trend": f"yearly_{gas}_concentrations",
            }
        )
        .assign(location="World")
    )


def monthly_concentrations():
    gases = {
        "co2": ("https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_gl.txt", 57),
        "ch4": ("https://gml.noaa.gov/webdata/ccgg/trends/ch4/ch4_mm_gl.txt", 60),
        "n2o": ("https://gml.noaa.gov/webdata/ccgg/trends/n2o/n2o_mm_gl.txt", 60),
    }
    for k, v in gases.items():
        output_file = os.path.join(READY_DIR, f"noaa_monthly-{k}-concentrations.csv")
        process_concentration_file(gas=k, source_url=v[0], skiprows=v[1]).to_csv(
            output_file, index=False
        )


def get_sea_level_url(source_page: str) -> str:
    soup = BeautifulSoup(requests.get(source_page).content, "html.parser")
    links = soup.find_all("a")
    for link in links:
        if link.text == "data":
            source_url = link["href"]
            break
    return source_url


def sea_level_rise():
    output_file = os.path.join(READY_DIR, "noaa_sea-level-rise.csv")
    source_page = "https://www.climate.gov/news-features/understanding-climate/climate-change-global-sea-level"
    source_url = get_sea_level_url(source_page)
    df = (
        pd.read_csv(
            source_url,
            sep="\t",
            encoding_errors="ignore",
            usecols=["Date", "CW_2011", "UHSLC_FD"],
        )
        .rename(
            columns={
                "Date": "date",
                "CW_2011": "sea_level_rise_cw2011",
                "UHSLC_FD": "sea_level_rise_uhslcfd",
            }
        )
        .assign(location="World")
    )

    df["sea_level_rise_average"] = (
        df[["sea_level_rise_cw2011", "sea_level_rise_uhslcfd"]].mean(axis=1).round(5)
    )

    # Fix dates
    start = df.index[df.date == "1/15/00"][0]
    end = df.index[df.date == "1/15/00"][1]
    df.loc[(df.index >= start) & (df.index < end), "date"] = (
        df.date.str.slice(0, -2) + "19" + df.date.str.slice(-2)
    )
    df.loc[df.index >= end, "date"] = (
        df.date.str.slice(0, -2) + "20" + df.date.str.slice(-2)
    )
    df.loc[df.index >= start, "date"] = pd.to_datetime(df.date).dt.date

    df.to_csv(output_file, index=False)


def year_bp_to_year(year_bp: pd.Series) -> pd.Series:
    """Convert year before present (where present refers to 1950) into default years (in the Gregorian calendar),
    avoiding year zero.

    Parameters
    ----------
    year_bp : float or pd.Series
        Year before 1950.

    Returns
    -------
    year : pd.Series
        Regular year.

    """
    year_bp = pd.Series(year_bp)
    year = 1950 - year_bp

    # Skip year zero.
    year[year < 1] = year[year < 1] - 1

    return year


def yearly_long_run_co2_concentration():
    """Create a dataset of CO2 concentration (in parts per million, ppm) from ~800,000 years ago until ~2000.

    The source is the Antarctic Ice Cores Revised 800KYr CO2 Data:
    https://www.ncei.noaa.gov/access/paleo-search/study/17975
    The specific dataset loaded in this function corresponds to the excel file "EPICA DOME C" on that page:
    https://www.ncei.noaa.gov/pub/data/paleo/icecore/antarctica/antarctica2015co2.xls

    """
    # Define paths to input and output files.
    co2_data_file = "https://www.ncei.noaa.gov/pub/data/paleo/icecore/antarctica/antarctica2015co2.xls"
    output_file = os.path.join(READY_DIR, "noaa_yearly-long-run-co2-concentrations.csv")
    # Name for output column of CO2 concentrations.
    co2_column = "yearly_co2_concentrations"

    # Load data.
    co2_data = pd.read_excel(co2_data_file, sheet_name="CO2 Composite", skiprows=14)
    co2_data = co2_data.rename(
        columns={
            co2_data.columns[0]: "year_bp",
            co2_data.columns[1]: co2_column,
        }
    )[["year_bp", co2_column]]

    # Since pandas datetime cannot handle such long past dates, for simplicity, round up years, and take average co2
    # concentrations of years that appear on more than one row.
    co2_data["year_bp"] = co2_data["year_bp"].round(0).astype(int)
    co2_data = co2_data.groupby("year_bp").agg({co2_column: "mean"}).reset_index()

    # Convert bp years to conventional years.
    co2_data["year"] = year_bp_to_year(co2_data["year_bp"])
    co2_data = (
        co2_data.assign(location="World")[["location", "year", co2_column]]
        .sort_values("year", ascending=True)
        .reset_index(drop=True)
    )

    # Output dataset.
    co2_data.to_csv(output_file, index=False)


def main():
    monthly_concentrations()
    sea_level_rise()
    yearly_long_run_co2_concentration()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    main()
