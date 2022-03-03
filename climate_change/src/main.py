from functools import reduce
import glob

import pandas as pd

import epa
import hawaii
import metoffice
import nasa
import noaa
import osisaf

ZERO_DAY = "1800-01-01"


def extract():
    epa.ocean_heat_content()
    epa.antarctic_sea_ice()
    epa.mass_balance_global_glaciers()
    epa.snow_cover_north_america()
    epa.antarctica_greenland_ice_sheet_loss()
    hawaii.ocean_ph()
    metoffice.annual_sea_surface_temperature()
    metoffice.monthly_sea_surface_temperature()
    nasa.global_temperature_anomaly()
    nasa.arctic_sea_ice_extent()
    noaa.monthly_concentrations()
    noaa.sea_level_rise()
    osisaf.arctic_sea_ice()


def transform():
    year_dataframes = []
    date_dataframes = []
    for file in glob.glob("ready/*.csv"):
        tmp_df = pd.read_csv(file)
        if "date" in tmp_df.columns:
            date_dataframes.append(tmp_df)
        else:
            year_dataframes.append(tmp_df)

    year_df = reduce(
        lambda left, right: pd.merge(left, right, on=["location", "year"], how="outer"),
        year_dataframes,
    )
    date_df = reduce(
        lambda left, right: pd.merge(left, right, on=["location", "date"], how="outer"),
        date_dataframes,
    )

    return year_df, date_df


def load(year_df: pd.DataFrame, date_df: pd.DataFrame):

    # Rounding tiny values
    date_df["monthly_sea_surface_temperature_anomaly"] = date_df[
        "monthly_sea_surface_temperature_anomaly"
    ].round(4)

    # Grapher file
    date_df["year"] = (pd.to_datetime(date_df.date) - pd.to_datetime(ZERO_DAY)).dt.days
    date_df = date_df.drop(columns="date")

    df = pd.merge(year_df, date_df, on=["location", "year"], how="outer").rename(
        columns={"location": "entity"}
    )

    first_column = df.pop("entity")
    df.insert(0, "entity", first_column)

    df.to_csv("output/Climate change impacts.csv", index=False)


def main():
    extract()
    year_df, date_df = transform()
    load(year_df, date_df)


if __name__ == "__main__":
    main()
