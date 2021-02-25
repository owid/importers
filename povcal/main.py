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

DECILE_THRESHOLDS = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
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

    frame = pd.concat(li, ignore_index=True)
    return frame


def population_under_income_line_by_country_year(df):
    dfg = df.sort_values(by=["HeadCount"]).groupby(["CountryName", "RequestYear"])
    decile_thresholds_by_country_year = {}
    for country_year_tuple in dfg.groups.keys():
        country_year_df = dfg.get_group(country_year_tuple)
        for relative_income_line in DECILE_THRESHOLDS:
            actual_income_line = -1
            try:
                actual_income_line = (
                    country_year_df.iloc[
                        [country_year_df.HeadCount.searchsorted(relative_income_line)]
                    ]
                    .iloc[0]
                    .poverty_line
                )
            except IndexError:
                print(
                    f"headcount for income line {relative_income_line} not found for {country_year_tuple}"
                )

            if country_year_tuple not in decile_thresholds_by_country_year:
                decile_thresholds_by_country_year[country_year_tuple] = []
            decile_thresholds_by_country_year[country_year_tuple].append(
                actual_income_line
            )

    df = pd.DataFrame.from_dict(
        decile_thresholds_by_country_year, orient="index"
    ).reset_index()
    df = df.rename(columns={"index": "country_year"})
    for index, poverty_line in enumerate(DECILE_THRESHOLDS):
        df = df.rename(columns={index: str(poverty_line)})

    df[["CountryName", "RequestYear"]] = pd.DataFrame(
        df["country_year"].tolist(), index=df.index
    )
    df.drop(columns=["country_year"])
    return df[
        [
            "CountryName",
            "RequestYear",
            *[str(income_line) for income_line in DECILE_THRESHOLDS],
        ]
    ]


def extract_deciles_from_headcount_files_and_write_to_csv():
    # df = combine_country_year_headcount_files()
    # df.to_csv("TEMP_combined.csv", index=False)
    df = pd.read_csv("TEMP_combined.csv")
    combined_df = population_under_income_line_by_country_year(df)
def suffix_coverage_type_in_country_names(df, coverageType):
    df.loc[df.CoverageType == coverageType, "CountryName"] = df.loc[
        df.CoverageType == coverageType, "CountryName"
    ].map(lambda x: f"{x}_{coverageType}")
def generate_absolute_poverty_line_df():
    absolute_poverty_line_frames = []
    for poverty_line in ABSOLUTE_POVERTY_LINES:
        filename = f"{DETAILED_DATA_DIR}/{poverty_line}.csv"

        df = pd.read_csv(filename, header=0)

        suffix_coverage_type_in_country_names(df, "R")
        suffix_coverage_type_in_country_names(df, "U")

        df = df[
            ["CountryName", "RequestYear", "HeadCount", "ReqYearPopulation", "PovGap"]
        ]

        df = add_absolute_poverty_count_column(df)
        df = add_absolute_poverty_gap_column(df)

        df = df.drop(columns=["ReqYearPopulation"])

        df = df.rename(
            columns={
                "HeadCount": f"{poverty_line}_HeadCount",
                "PovGap": f"{poverty_line}_PovGap",
                "poverty_absolute": f"{poverty_line}_poverty_absolute",
                "absolute_poverty_gap": f"{poverty_line}_absolute_poverty_gap",
            }
        )
        absolute_poverty_line_frames.append(df)

    df = reduce(
        lambda df1, df2: pd.merge(df1, df2, on=["CountryName", "RequestYear"]),
        absolute_poverty_line_frames,
    )

    return df

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
    absolute_poverty_line_df = generate_absolute_poverty_line_df()


if __name__ == "__main__":
    main()
