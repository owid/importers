"""Delete the content of the input folders, download raw data files from BP and save them in the input folder.

"""

import os
import shutil
import requests
import pandas as pd

from bp_statreview import ALL_DATA_LINK, INPATH, CONSOLIDATED_DATASET_NARROW_FORMAT_LINK,\
    CONSOLIDATED_DATASET_PANEL_FORMAT_LINK

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
        url = CONSOLIDATED_DATASET_PANEL_FORMAT_LINK
    else:
        url = CONSOLIDATED_DATASET_NARROW_FORMAT_LINK
    logger.info(f'Downloading data from "{url}"...')
    df = pd.read_csv(url)
    df.to_csv(os.path.join(INPATH, "data.csv"), index=False)


def _download_data_excel() -> None:
    url = ALL_DATA_LINK
    logger.info(f'Downloading data from "{url}"...')
    res = requests.get(url)
    with open(os.path.join(INPATH, "data.xlsx"), "wb") as f:
        f.write(res.content)


if __name__ == "__main__":
    main()
