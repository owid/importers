"""downloads raw data and saves it to disk.
"""

import os
import shutil
import requests
import pandas as pd

from bp_statreview import INPATH

import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    delete_input()
    download_data()


def delete_input() -> None:
    """deletes all files and folders in `{INPATH}`.

    WARNING: this method deletes all input data and is only intended for use
    immediately prior to `download_data()`.
    """
    if os.path.exists(INPATH):
        shutil.rmtree(INPATH)
    logger.info(f"Deleted all existing input files in {INPATH}")


def download_data() -> None:
    """Downloads the raw World Development Indicators data and saves it
    in csv format to `{INPATH}`.
    """
    if not os.path.exists(INPATH):
        os.makedirs(INPATH)
    _download_data_csv()
    _download_data_excel()
    logger.info(f"Data successfully downloaded to {INPATH}")


def _download_data_csv(wide: bool = False) -> None:
    if wide:
        url = "https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/xlsx/energy-economics/statistical-review/bp-stats-review-2021-consolidated-dataset-panel-format.csv"
    else:
        url = "https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/xlsx/energy-economics/statistical-review/bp-stats-review-2021-consolidated-dataset-narrow-format.csv"
    logger.info(f'Downloading data from "{url}"...')
    df = pd.read_csv(url)
    df.to_csv(os.path.join(INPATH, "data.csv"), index=False)


def _download_data_excel() -> None:
    url = "https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/xlsx/energy-economics/statistical-review/bp-stats-review-2021-all-data.xlsx"
    logger.info(f'Downloading data from "{url}"...')
    res = requests.get(url)
    with open(os.path.join(INPATH, "data.xlsx"), "wb") as f:
        f.write(res.content)


if __name__ == "__main__":
    main()
