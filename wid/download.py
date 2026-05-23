import os
import shutil
import zipfile
from io import BytesIO
import requests

from wid import INPATH

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
    """Downloads the raw EdStats data and saves it in CSV format to `{INPATH}`."""
    if not os.path.exists(INPATH):
        os.makedirs(INPATH)
    _download_data_csv()
    logger.info(f"Data succcessfully downloaded to {INPATH}")


def _download_data_csv() -> None:
    url = "https://wid.world/bulk_download/wid_all_data.zip"
    logger.info(f'Downloading data from "{url}"...')
    res = requests.get(url)
    zf = zipfile.ZipFile(BytesIO(res.content))
    zf.extractall(path=INPATH)


if __name__ == "__main__":
    main()
