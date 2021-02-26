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
from functools import reduce
from bisect import bisect_left


from HeadCount_Files_Downloader import HeadCount_Files_Downloader

pd.options.mode.chained_assignment = None  # default='warn'

DECILE_THRESHOLDS = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
ABSOLUTE_POVERTY_LINES = ["1.90", "3.20", "5.50", "10.00", "15.00", "20.00", "30.00"]
RELATIVE_POVERTY_LINES = [0.4, 0.5, 0.6]
MIN_POV_LINE = 0
MAX_POV_LINE = 400

DECILES_CSV_FILENAME = "output/deciles_by_country_year.csv"
ABSOLUTE_POVERTY_LINES_CSV_FILENAME = "output/absolute_poverty_lines.csv"
RELATIVE_POVERTY_LINES_CSV_FILENAME = "output/relative_poverty_lines.csv"
COUNTRY_YEAR_VARIABLE_CSV_FILENAME = "output/country_year_variable.csv"
MEGA_CSV_FILENAME = "output/mega.csv"

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
    for col_index, poverty_line in enumerate(DECILE_THRESHOLDS):
        df = df.rename(columns={col_index: f"decile_threshold_{poverty_line}"})

    df[["CountryName", "RequestYear"]] = pd.DataFrame(
        df["country_year"].tolist(), index=df.index
    )
    df.drop(columns=["country_year"])
    return df[
        [
            "CountryName",
            "RequestYear",
            *[f"decile_threshold_{income_line}" for income_line in DECILE_THRESHOLDS],
        ]
    ]


def extract_deciles_from_headcount_files():
    # df = combine_country_year_headcount_files()
    # df.to_csv("TEMP_combined.csv", index=False)
    df = pd.read_csv("TEMP_combined.csv")
    combined_df = population_under_income_line_by_country_year(df)
    return combined_df


def suffix_coverage_type_in_country_names(df, coverageType):
    df.loc[df.CoverageType == coverageType, "CountryName"] = df.loc[
        df.CoverageType == coverageType, "CountryName"
    ].map(lambda x: f"{x}_{coverageType}")


def get_headcount_for_country_year_and_poverty_line(countryName, poverty_line, year):
    closest_pov_line = poverty_line_as_string(
        get_closest_downloaded_headcount_file_for_poverty_line(float(poverty_line))
    )
    filename = f"{HEADCOUNTS_DIR}/{closest_pov_line}.csv"
    df = pd.read_csv(filename)
    return df[
        (df.CountryName == countryName) & (df.RequestYear == year)
    ].HeadCount.iloc[0]


def generate_relative_poverty_line_df(decile_df):
    df = decile_df[["CountryName", "RequestYear"]]
    df["median_income_line"] = decile_df["0.5"]

    for relative_income_line in RELATIVE_POVERTY_LINES:
        relative_income_line_in_dollars = int(relative_income_line * 100)
        df[f"{relative_income_line_in_dollars}%_median_income_line"] = (
            decile_df[str(relative_income_line)] * relative_income_line
        )

        df[f"{relative_income_line_in_dollars}%_median_income_line_formatted"] = df[
            f"{relative_income_line_in_dollars}%_median_income_line"
        ].map(lambda x: poverty_line_as_string(x))

        df[f"{relative_income_line_in_dollars}%_headcount"] = df.apply(
            lambda x: get_headcount_for_country_year_and_poverty_line(
                x.CountryName,
                x[f"{relative_income_line_in_dollars}%_median_income_line_formatted"],
                x.RequestYear,
            ),
            axis=1,
        )

    return df[
        [
            "CountryName",
            "RequestYear",
            *[
                f"{int(relative_income_line * 100)}%_headcount"
                for relative_income_line in RELATIVE_POVERTY_LINES
            ],
        ]
    ]


def suffix_coverage_types(df):
    suffix_coverage_type_in_country_names(df, "R")
    suffix_coverage_type_in_country_names(df, "U")
    return df


def generate_absolute_poverty_line_df():
    absolute_poverty_line_frames = []
    for poverty_line in ABSOLUTE_POVERTY_LINES:
        filename = f"{DETAILED_DATA_DIR}/{poverty_line}.csv"

        df = pd.read_csv(filename, header=0)

        suffix_coverage_types(df)

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


def country_year_variables_df():
    return pd.read_csv(f"{DETAILED_DATA_DIR}/{ABSOLUTE_POVERTY_LINES[0]}.csv", header=0)


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
            "DataType",
            "HeadCount",
            "CoverageType",
        ]
    )


def add_absolute_poverty_count_column(df):
    df["poverty_absolute"] = (df.HeadCount * df.ReqYearPopulation * 1000000).astype(int)
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
    df = add_absolute_poverty_count_column(df)
    df = add_absolute_poverty_gap_column(df)
    df = add_decile_averages_column(df)
    df = add_mean_column(df)
    df = add_welfare_measure_column(df)
    df = df.drop(columns=["DataType"])
    df = add_survey_year_column(df)

    return df


def poverty_line_as_string(line):
    return "{:.2f}".format(line)


def generate_poverty_lines_between(minimum_dollar, maximum_dollar):
    lines = all_cents_between_dollars(minimum_dollar, min(60, maximum_dollar), 0.01)
    lines.extend(
        all_cents_between_dollars(
            max(60.10, minimum_dollar), min(150, maximum_dollar), 0.10
        )
    )
    lines.extend(
        all_cents_between_dollars(max(155, minimum_dollar), min(400, maximum_dollar), 5)
    )

    return lines


def all_cents_between_dollars(minimum_dollar, maximum_dollar, increment=0.01):
    return [
        round(cent, 2)
        for cent in arange(minimum_dollar, maximum_dollar + increment, increment)
    ]


def get_closest_downloaded_headcount_file_for_poverty_line(poverty_line):
    return find_closest_number(
        generate_poverty_lines_between(MIN_POV_LINE, MAX_POV_LINE), poverty_line
    )


# https://stackoverflow.com/questions/12141150/from-list-of-integers-get-number-closest-to-a-given-value/12141511#12141511
def find_closest_number(myList, myNumber):
    """
    Assumes myList is sorted. Returns closest value to myNumber.

    If two numbers are equally close, return the smallest number.
    """
    pos = bisect_left(myList, myNumber)
    if pos == 0:
        return myList[0]
    if pos == len(myList):
        return myList[-1]
    before = myList[pos - 1]
    after = myList[pos]
    if after - myNumber < myNumber - before:
        return after
    else:
        return before


def generate_country_year_variable_df():
    df = country_year_variables_df()
    df = add_derived_columns(df)
    df = suffix_coverage_types(df)
    df = drop_unnecessary_columns(df)
    return df
def main():
    # poverty_lines = generate_poverty_lines_between(MIN_POV_LINE, MAX_POV_LINE)
    # headcountsDownloader = HeadCount_Files_Downloader(
    #     poverty_lines=[poverty_line_as_string(line) for line in poverty_lines],
    #     output_dir=HEADCOUNTS_DIR,
    #     detailed_data_dir=DETAILED_DATA_DIR,
    #     detailed_poverty_lines=ABSOLUTE_POVERTY_LINES,
    #     max_workers=1,
    # )
    # headcountsDownloader.download_headcount_files_by_poverty_line()

    # extract_deciles_from_headcount_files().to_csv(DECILES_CSV_FILENAME, index=False)

    # generate_absolute_poverty_line_df().to_csv(
    #     ABSOLUTE_POVERTY_LINES_CSV_FILENAME, index=False
    # )

    # generate_relative_poverty_line_df(
    #     pd.read_csv(DECILES_CSV_FILENAME, header=0)
    # ).to_csv(RELATIVE_POVERTY_LINES_CSV_FILENAME, index=False)

    # generate_country_year_variable_df().to_csv(
    #     COUNTRY_YEAR_VARIABLE_CSV_FILENAME, index=False
    # )


if __name__ == "__main__":
    main()
