import concurrent.futures

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

absolute_poverty_lines = ["1.90"]


def combine_country_year_headcount_files():
    path = "data_by_poverty_line"
    all_files = glob.glob(path + "/*.csv")

    li = []

    for filename in all_files:
        df = pd.read_csv(filename, index_col=0, header=0)
        df["poverty_line"] = os.path.basename(os.path.splitext(filename)[0])
        li.append(df)

    frame = pd.concat(li, axis=0, ignore_index=True)
    return frame[frame.CountryName.isin(["United States", "China"])]


def medians_by_country_year(df):
    dfg = df.sort_values(by=["HeadCount"]).groupby(["CountryName", "RequestYear"])
    median_poverty_line_by_country_year = {}
    for country_year_tuple in dfg.groups.keys():
        country_year_df = dfg.get_group(country_year_tuple)
        median_poverty_line = country_year_df.iloc[
            [country_year_df.HeadCount.searchsorted(0.5)]
        ]
        median_poverty_line_by_country_year[
            country_year_tuple
        ] = median_poverty_line.iloc[0].poverty_line

    df = pd.DataFrame.from_dict(
        median_poverty_line_by_country_year, orient="index"
    ).reset_index()
    df = df.rename(columns={"index": "country_year", 0: "poverty_line"})
    df[["country", "year"]] = pd.DataFrame(df["country_year"].tolist(), index=df.index)
    df.drop(columns=["country_year"])
    return df[["country", "year", "poverty_line"]]


def generate_country_year_variables():
    df = combine_country_year_headcount_files()
    df = medians_by_country_year(df)
    pdb.set_trace()


def download_data_and_write_csv(poverty_line):
    rounded_poverty_line = "{:.2f}".format(round(poverty_line, 2))

    filename = f"data_by_poverty_line2/{rounded_poverty_line}.csv"
    if path.exists(filename):
        return
    else:
        print(f"request starting for {rounded_poverty_line}")

    payload = {
        "Countries": "all",
        "YearSelected": "all",
        "PovertyLine": rounded_poverty_line,
        "display": "C",
    }
    result = requests.get(
        "http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx",
        params=payload,
        timeout=10,
    )

    df = pd.read_csv(StringIO(result.text))
    df = df[df.CoverageType.isin(["N", "A"])]
    df = df[["CountryName", "RequestYear", "HeadCount"]]

    df.to_csv(filename)

    print(f"{filename} completed")


def download_raw_data():
    poverty_lines = []
    for dollar in range(1, 3, 1):
        poverty_lines.extend(
            [round(cent, 2) for cent in arange(dollar - 1, 0.50, 0.01)]
        )

    print(poverty_lines)

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        while len(poverty_lines):
            future_to_poverty_line = {
                executor.submit(download_data_and_write_csv, poverty_line): poverty_line
                for poverty_line in poverty_lines
            }

            failed = set()
            for future in concurrent.futures.as_completed(future_to_poverty_line):
                poverty_line = future_to_poverty_line[future]
                try:
                    future.result()
                except Exception:
                    failed.add(poverty_line)

            poverty_lines = list(failed)
            print(f"{len(poverty_lines)} failed requests")
            print("Retrying in 10 seconds...")
            time.sleep(10)


def combine_raw_data():
    data_files = [
        f"data_by_poverty_line/{poverty_line}"
        for poverty_line in absolute_poverty_lines
    ]
    data_csvs = [
        pd.read_csv(filename, index_col=None, header=0) for filename in data_files
    ]

    combined_data_frame = pd.concat(data_csvs, axis=0, ignore_index=True)
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


def add_derived_columns(df):
    df["poverty_absolute"] = (
        df.poverty_percentage * df.ReqYearPopulation * 1000000
    ).astype(int)

    df["absolute_poverty_gap"] = df.PovGap * 365 * df.ReqYearPopulation

    for decile in range(1, 11):
        df[f"Decile{decile}_average"] = df[f"Decile{decile}"] * df.Mean / 30

    df["Mean"] = df.Mean / 365 / 12

    df["welfare_measure"] = df.DataType.apply(
        lambda x: "consumption" if x == "X" else "income"
    )
    df = df.drop(columns=["DataType"])

    df["survey_year"] = df.RequestYear == df.DataYear

    return df


def main():
    # download_raw_data()
    generate_country_year_variables()
    # raw_data = combine_raw_data()
    # raw_data_filtered = drop_unnecessary_columns(raw_data)
    # raw_data_formatted = rename_columns(raw_data_filtered)
    # df = add_derived_columns(raw_data_formatted)
    # df = medians_by_country()
    # pdb.set_trace()


# df.to_html('temp.html')


if __name__ == "__main__":
    main()
