import requests
import os
import glob
import time
import pandas as pd
import zipfile
import io
from pathlib import Path


CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))

from ihme_gbd_mental_health import INPATH, OUTPATH, ENTFILE, CONFIGPATH

################################################################################
# This dataset download is a bit hard to automate, a human needs to follow the
# steps below instead:
#
# 1. Go to the GHDx results tool: http://ghdx.healthdata.org/gbd-results-tool
# 2. Select the following:
#
#    Measure:
#    - Prevalence
#
#    Age:
#    - All Ages
#    - Age-standardized
#    - Under 5
#    - 5-14 years
#    - 15-49 years
#    - 50-69 years
#    - 70+ years
#    - 10 to 14
#    - 15 to 19
#    - 20 to 24
#    - 25 to 29
#    - 30 to 34
#
#    Metric:
#    - Number
#    - Rate
#    - Percent
#
#    Year: select all
#
#    Cause:
#    - All under B.6
#    - All under B.7
#
#    Context: Cause
#
#    Location:
#    - select all countries
#    and then also:
#    - All "higher level" districts, e.g. Sub-Saharan Africa
#    - Also England, Scotland, Wales & Northern Ireland
#
#    Sex:
#    - Both
#    - Male
#    - Female
#
#
#
#   When prompted select names only for data columns to include


url_lead = "https://s3.healthdata.org/gbd-api-2019-public/4e6e42bccc2497b99d4709602fb0c4b8_files/IHME-GBD_2019_DATA-4e6e42bc-"  # this will need to be updated - it times out pretty quickly (couple of days)


def main() -> None:
    make_dirs()
    download_data(url_lead)
    load_and_filter()


def make_dirs() -> None:
    Path(INPATH).mkdir(parents=True, exist_ok=True)
    Path(OUTPATH, "datapoints").mkdir(parents=True, exist_ok=True)
    Path(CONFIGPATH).mkdir(parents=True, exist_ok=True)


def download_data(url: str) -> None:
    for i in range(1, 32):
        fname = url + "%s.zip" % i
        trycnt = 3
        while trycnt > 0:
            try:
                r = requests.get(fname)
                assert r.ok
                zname = os.path.join(INPATH, os.path.basename(fname))
                z = zipfile.ZipFile(io.BytesIO(r.content))
                z.extractall(os.path.join(INPATH, "csv"))
                break
            except requests.exceptions.ChunkedEncodingError:
                if trycnt <= 0:
                    print("Failed to retrieve: " + fname + "\n")  # done retrying
                else:
                    trycnt -= 1  # retry
                time.sleep(0.5)
    os.remove(os.path.join(INPATH, "csv", "citation.txt"))


def load_and_filter() -> None:
    if not os.path.isfile(os.path.join(INPATH, "all_data_filtered.csv")):
        all_files = [i for i in glob.glob(os.path.join(INPATH, "csv", "*.csv"))]
        fields = [
            "measure",
            "location",
            "sex",
            "age",
            "cause",
            "metric",
            "year",
            "val",
        ]  # removing id columns and the upper and lower bounds around value in the hope the all_data file will be smaller.
        df_from_each_file = (pd.read_csv(f, sep=",", usecols=fields) for f in all_files)
        df_merged = pd.concat(df_from_each_file, ignore_index=True)
        assert sum(df_merged.isnull().sum()) == 0, print("Null values in dataframe")
        df_merged.to_csv(os.path.join(INPATH, "all_data_filtered.csv"), index=False)
        print("Saving all data from raw csv files")
    if not os.path.isfile(ENTFILE):
        df_merged[["location_name"]].drop_duplicates().dropna().rename(
            columns={"location_name": "Country"}
        ).to_csv(ENTFILE, index=False)
        print(
            "Saving entity files"
        )  # use this file in the country standardizer tool - save standardized file as config/standardized_entity_names.csv


if __name__ == "__main__":
    main()
