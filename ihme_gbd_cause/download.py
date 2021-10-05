import requests
import os
import glob
import time
import pandas as pd
import zipfile
import io
from pathlib import Path

CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))

from ihme_gbd_cause import INPATH, OUTPATH, ENTFILE, CONFIGPATH

################################################################################
### Causes                                                                   ###
################################################################################
# This dataset download is a bit hard to automate, a human needs to follow the
# steps below instead:
#
# 1. Go to the GHDx results tool: http://ghdx.healthdata.org/gbd-results-tool
# 2. Select the following:
#
#    Measure:
#    - Deaths
#    - DALYs (Disability-Adjusted Life Years)
#
#    Age:
#    - All Ages
#    - Age-standardized
#    - Under 5
#    - 5-14 years
#    - 15-49 years
#    - 50-69 years
#    - 70+ years
#
#    Metric:
#    - Number
#    - Rate
#    - Percent
#
#    Year: select all
#
#    Cause: select all
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
#
# 3. The tool will then create a dataset for you in chunks. Once it's finished
#    (which may take several hours) this command might be helpful to download
#    them all:
#
#         for i in {1..<number of files>}; do
#             wget http://s3.healthdata.org/gbd-api-2017-public/<hash of a file...>-$i.zip;
#         done
#
# 4. Then, unzip them all and put them in a single folder. This should be the
#    `csv_dir` specified below. Helpful command:
#
#         unzip \*.zip -x citation.txt -d csv/
#


url_lead = "https://s3.healthdata.org/gbd-api-2019-public/6bc81b0cecef147c44df55608fe573f3_files/IHME-GBD_2019_DATA-6bc81b0c-"


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
        print(fname)
        trycnt = 3
        while trycnt > 0:
            try:
                r = requests.get(fname)
                assert r.ok
                zname = os.path.join(INPATH, os.path.basename(fname))
                print(zname)
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
            "measure_name",
            "location_name",
            "sex_name",
            "age_name",
            "cause_name",
            "metric_name",
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
