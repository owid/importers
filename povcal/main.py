from os import path
import os.path
from io import StringIO
from numpy import arange
from math import floor, ceil
import requests
import json
import os
import pandas as pd
import sys
import pdb
import time

sys.path.append("../..")
from importers.utils import write_file
import glob

from HeadCount_Files_Downloader import HeadCount_Files_Downloader

RELATIVE_POVERTY_LINES = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
ABSOLUTE_POVERTY_LINES = ["1.90", "3.20", "5.50", "10.00", "15.00", "20.00", "30.00"]

HEADCOUNTS_DIR = "output/headcounts_by_poverty_line"
DETAILED_DATA_DIR = "output/detailed_data_by_poverty_line"


def combine_country_year_headcount_files():
    all_files = glob.glob(HEADCOUNTS_DIR + "/*.csv")

    li = []

    for filename in all_files:
        df = pd.read_csv(filename, header=0)
        df["poverty_line"] = os.path.basename(os.path.splitext(filename)[0])
        li.append(df)

    frame = pd.concat(li, axis=0, ignore_index=True)
    return frame[frame.CountryName.isin(["United States", "China"])]


def population_under_income_line_by_country_year(df):
    dfg = df.sort_values(by=["HeadCount"]).groupby(["CountryName", "RequestYear"])
    median_income_by_country_year = {}
    for country_year_tuple in dfg.groups.keys():
        country_year_df = dfg.get_group(country_year_tuple)
        for income_line in RELATIVE_POVERTY_LINES:
            median_income = country_year_df.iloc[
                [country_year_df.HeadCount.searchsorted(income_line)]
            ]
            if country_year_tuple not in median_income_by_country_year:
                median_income_by_country_year[country_year_tuple] = []
            median_income_by_country_year[country_year_tuple].append(
                median_income.iloc[0].poverty_line
            )

    df = pd.DataFrame.from_dict(
        median_income_by_country_year, orient="index"
    ).reset_index()
    df = df.rename(columns={"index": "country_year"})
    for index, poverty_line in enumerate(RELATIVE_POVERTY_LINES):
        df = df.rename(columns={index: str(poverty_line)})

    df[["country", "year"]] = pd.DataFrame(df["country_year"].tolist(), index=df.index)
    df.drop(columns=["country_year"])
    return df[
        [
            "country",
            "year",
            *[str(income_line) for income_line in RELATIVE_POVERTY_LINES],
        ]
    ]


def extract_deciles_from_headcount_files_and_write_to_csv():
    # df = combine_country_year_headcount_files()
    # df.to_csv("TEMP_combined.csv")
    df = pd.read_csv("TEMP_combined.csv")
    combined_df = population_under_income_line_by_country_year(df)
    # df.to_csv("output/deciles_by_country_year.csv")
    # combined_df.to_html("output/deciles_by_country_year.html")
    return combined_df


def combine_raw_data():
    data_files = [
        f"{DETAILED_DATA_DIR}/{poverty_line}.csv"
        for poverty_line in ABSOLUTE_POVERTY_LINES
    ]
    data_csvs = [pd.read_csv(filename, header=0) for filename in data_files]

    combined_data_frame = pd.concat(data_csvs, axis=0)
    return combined_data_frame


def drop_unnecessary_columns(raw_data):
    return raw_data.drop(
        columns=[
            "isInterpolated",
            "useMicroData",
            "RegionCode",
            "PPP",
            "Watts",
            "SvyInfoID",
            "Polarization",
            "PovGapSqr",
            "pr.mld",
        ]
    )


def rename_columns(df):
    return df.rename(columns={"HeadCount": "poverty_percentage"})

def add_absolute_poverty_column(df):
    df["poverty_absolute"] = (
        df.poverty_percentage * df.ReqYearPopulation * 1000000
    ).astype(int)
    return df

def add_absolute_poverty_gap_column(df):
    df["absolute_poverty_gap"] = df.PovGap * 365 * df.ReqYearPopulation
    return df

def add_decile_averages_column(df):
    for decile in range(1, 11):
        df[f"Decile{decile}_average"] = df[f"Decile{decile}"] * df.Mean / 30
    return df

def add_mean_column(df):
    df["Mean"] = df.Mean / 365 / 12
    return df

def add_welfare_measure_column(df):
    df["welfare_measure"] = df.DataType.apply(
        lambda x: "consumption" if x == "X" else "income"
    )
    return df

def add_survey_year_column(df):
    df["survey_year"] = df.RequestYear == df.DataYear
    return df

def add_derived_columns(df):
    df = add_absolute_poverty_column(df)
    df = add_absolute_poverty_gap_column(df)
    df = add_decile_averages_column(df)
    df = add_mean_column(df)
    df = add_welfare_measure_column(df)
    df = df.drop(columns=["DataType"])
    df = add_survey_year_column(df)

    return df


def main():
    # headcountsDownloader = HeadCount_Files_Downloader(
    #     minimum_poverty_line=0,
    #     maximum_poverty_line=400,
    #     output_dir=HEADCOUNTS_DIR,
    #     detailed_data_dir=DETAILED_DATA_DIR,
    #     detailed_poverty_lines=ABSOLUTE_POVERTY_LINES,
    #     max_workers=1,
    # )
    # headcountsDownloader.download_headcount_files_by_poverty_line()
    # deciles_df = extract_deciles_from_headcount_files_and_write_to_csv()
    raw_data = combine_raw_data()
    raw_data_filtered = drop_unnecessary_columns(raw_data)
    raw_data_formatted = rename_columns(raw_data_filtered)
    df = add_derived_columns(raw_data_formatted)
    pdb.set_trace()


if __name__ == "__main__":
    main()
