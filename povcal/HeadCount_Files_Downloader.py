from numpy import arange
import requests
import pandas as pd
import numpy as np
import concurrent.futures
import time
import pdb
from os import path
from io import StringIO


class HeadCount_Files_Downloader:
    def __init__(
        self,
        poverty_lines,
        output_dir,
        detailed_data_dir,
        detailed_poverty_lines,
        max_workers=20,
    ):
        self.poverty_lines = poverty_lines
        self.output_dir = output_dir
        self.detailed_data_dir = detailed_data_dir
        self.detailed_poverty_lines = detailed_poverty_lines
        self.max_workers = max_workers

    def download_headcount_files_by_poverty_line(self):
        """
        Queries PovCal API and writes a headcount file for every poverty line (e.g. $0.00 to $60.00).

        Each headcount file provides headcount population under the poverty line
        for each country-year available and will be written to output/ directory.
        """
        poverty_lines = self.poverty_lines

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers
        ) as executor:
            while len(poverty_lines):
                future_to_poverty_line = {
                    executor.submit(
                        self.download_one_headcount_file, poverty_line
                    ): poverty_line
                    for poverty_line in poverty_lines
                }

                failed = set()
                for future in concurrent.futures.as_completed(future_to_poverty_line):
                    poverty_line = future_to_poverty_line[future]
                    try:
                        future.result()
                    except Exception as error:
                        print(
                            f"{self.headcount_output_filename(poverty_line)} failed. Will retry."
                        )
                        print(type(error))
                        print(error.args)
                        failed.add(poverty_line)

                poverty_lines = list(failed)
                if poverty_lines:
                    print(f"{len(poverty_lines)} failed requests")
                    print("Retrying in 10 seconds...")
                    time.sleep(10)

            print("Files successfully downloaded.")

    def requires_detailed_download(self, poverty_line):
        return poverty_line in self.detailed_poverty_lines

    def file_already_downloaded(self, poverty_line):
        if self.requires_detailed_download(poverty_line) and not path.exists(
            self.detailed_data_output_filename(poverty_line)
        ):
            return False
        if not path.exists(self.headcount_output_filename(poverty_line)):
            return False
        return True

    def download_one_headcount_file(self, poverty_line):
        filename = self.headcount_output_filename(poverty_line)

        if self.file_already_downloaded(poverty_line):
            print(f"data exists for {poverty_line}. Skipping.")
            return
        else:
            print(f"request starting for {poverty_line}")

        api_result = self.request_headcounts_by_poverty_line(poverty_line)
        region_data = self.request_headcounts_by_poverty_line(
            poverty_line, regional_data=True
        )

        df = csv_to_dataframe(api_result)
        df = mark_missing_values_as_NaN(df)
        df = suffix_coverage_types(df)
        df = rename_columns(df)

        r_df = csv_to_dataframe(region_data)
        r_df = mark_missing_values_as_NaN(r_df)
        r_df = r_df.iloc[::-1]
        r_df = r_df.rename(
            columns={
                "requestYear": "RequestYear",
                "regionTitle": "CountryName",
                "hc": "headcount_ratio",
                "regionCID": "CountryCode",
                "povertyLine": "PovertyLine",
                "pg": "poverty_gap",
                "population": "ReqYearPopulation",
            }
        )
        r_df = r_df.drop(columns=["p2"])

        if self.requires_detailed_download(poverty_line):
            detailed_filename = self.detailed_data_output_filename(poverty_line)
            detailed_df = pd.concat([df, r_df], ignore_index=True)
            detailed_df.to_csv(detailed_filename, index=False)
            print(f"{detailed_filename} written")

        df = self.filter_only_headcount(df)
        r_df = r_df[["CountryName", "RequestYear", "headcount_ratio"]]
        combined_df = pd.concat([df, r_df], ignore_index=True)

        combined_df.to_csv(filename, index=False)
        print(f"{filename} written")

    def headcount_output_filename(self, poverty_line):
        return f"{self.output_dir}/{poverty_line}.csv"

    def detailed_data_output_filename(self, poverty_line):
        return f"{self.detailed_data_dir}/{poverty_line}.csv"

    def request_headcounts_by_poverty_line(self, poverty_line, regional_data=False):
        api_address = "http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx"
        params = {
            "Countries": "all",
            "YearSelected": "all",
            "PovertyLine": poverty_line,
            "display": "C",
        }
        if regional_data:
            params["display"] = "R"
            params["GroupedBy"] = "WB"

        result = requests.get(
            api_address,
            params=params,
            timeout=30,
        )
        return result.text

    def filter_only_headcount(self, df):
        return df[["CountryName", "RequestYear", "headcount_ratio"]]


def csv_to_dataframe(csv):
    return pd.read_csv(StringIO(csv))


def mark_missing_values_as_NaN(df):
    return df.replace(-1, np.NaN)


def suffix_coverage_types(df):
    suffix_coverage_type_in_country_names(df, "R")
    suffix_coverage_type_in_country_names(df, "U")
    return df


def suffix_coverage_type_in_country_names(df, coverageType):
    df.loc[df.CoverageType == coverageType, "CountryName"] = df.loc[
        df.CoverageType == coverageType, "CountryName"
    ].map(lambda x: f"{x}_{coverageType}")


def rename_columns(df):
    return df.rename(
        columns={
            "HeadCount": "headcount_ratio",
            "Decile1": "decile1",
            "Decile2": "decile2",
            "Decile3": "decile3",
            "Decile4": "decile4",
            "Decile5": "decile5",
            "Decile6": "decile6",
            "Decile7": "decile7",
            "Decile8": "decile8",
            "Decile9": "decile9",
            "Decile10": "decile10",
            "Mean": "mean",
            "Gini": "gini",
            "PovGap": "poverty_gap",
        }
    )
