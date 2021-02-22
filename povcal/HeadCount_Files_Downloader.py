from numpy import arange
import requests
import pandas as pd
import concurrent.futures
import time
import pdb
from os import path
from io import StringIO


class HeadCount_Files_Downloader:
    def __init__(
        self,
        minimum_poverty_line,
        maximum_poverty_line,
        output_dir,
        max_workers=20,
    ):
        self.minimum_poverty_line = minimum_poverty_line
        self.maximum_poverty_line = maximum_poverty_line
        self.output_dir = output_dir
        self.max_workers = max_workers

    def download_headcount_files_by_poverty_line(self):
        """
        Queries PovCal API and writes a headcount file for every poverty line (e.g. $0.00 to $60.00).

        Each headcount file provides headcount population under the poverty line
        for each country-year available and will be written to output/ directory.
        """
        poverty_lines = generate_poverty_lines_between(
            self.minimum_poverty_line, self.maximum_poverty_line
        )

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
                    except Exception:
                        failed.add(poverty_line)

                poverty_lines = list(failed)
                print(f"{len(poverty_lines)} failed requests")
                print("Retrying in 10 seconds...")
                time.sleep(10)

    def download_one_headcount_file(self, poverty_line):
        filename = self.output_filename(poverty_line)

        if path.exists(filename):
            print(f"data exists for {poverty_line}. Skipping.")
            return
        else:
            print(f"request starting for {poverty_line}")

        try:
            api_result = self.request_headcounts_by_poverty_line(poverty_line)
        except Exception as error:
            print(f"{filename} failed. Will retry.")
            print(type(error))
            print(error.args)
            raise error

        df = csv_to_dataframe(api_result)
        df = self.filter_necessary_data(df)

        df.to_csv(filename, index=False)
        print(f"{filename} written")

    def output_filename(self, poverty_line):
        return f"{self.output_dir}/{poverty_line}.csv"

    def request_headcounts_by_poverty_line(self, poverty_line):
        api_address = "http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx"
        params = {
            "Countries": "all",
            "YearSelected": "all",
            "PovertyLine": poverty_line,
            "display": "C",
        }
        result = requests.get(
            api_address,
            params=params,
            timeout=30,
        )
        return result.text

    def filter_necessary_data(self, df):
        df = df[df.CoverageType.isin(["N", "A"])]
        return df[["CountryName", "RequestYear", "HeadCount"]]


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

    return ["{:.2f}".format(line) for line in lines]


def all_cents_between_dollars(minimum_dollar, maximum_dollar, increment=0.01):
    return [
        round(cent, 2)
        for cent in arange(minimum_dollar, maximum_dollar + increment, increment)
    ]


def csv_to_dataframe(csv):
    return pd.read_csv(StringIO(csv))