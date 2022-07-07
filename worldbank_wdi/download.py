"""downloads WDI data and saves it to disk.
"""
# type: ignore

import os
import shutil
import zipfile
from io import BytesIO
import requests
import pandas as pd
from tqdm import tqdm

from worldbank_wdi import INPATH

import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MAX_RETRIES = 5
CHUNK_SIZE = 8192


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
    try:
        _download_data_csv()
    except:  # noqa
        _download_data_excel()
    logger.info(f"Data succcessfully downloaded to {INPATH}")


def _download_data_csv() -> None:
    url = "http://databank.worldbank.org/data/download/WDI_csv.zip"
    content = _download_file(url, MAX_RETRIES)
    zf = zipfile.ZipFile(BytesIO(content))
    fnames = zf.namelist()
    zf.extractall(path=INPATH)
    for fname in fnames:
        fname_zip = f"{fname}.zip"
        pd.read_csv(os.path.join(INPATH, fname)).to_csv(
            os.path.join(INPATH, fname_zip), index=False, compression="gzip"
        )
        os.remove(os.path.join(INPATH, fname))


def _download_data_excel() -> None:
    url = "http://databank.worldbank.org/data/download/WDI_excel.zip"
    content = _download_file(url, MAX_RETRIES)
    zf = zipfile.ZipFile(BytesIO(content))
    fnames = zf.namelist()
    assert len(fnames) == 1, "Expected only one file in xlsx zip archive."
    sheet2df = pd.read_excel(
        BytesIO(zf.read(fnames[0])), sheet_name=None, engine="openpyxl"
    )
    for sheet, df in sheet2df.items():
        fname_zip = f"WDI{sheet}.csv.zip"
        df.to_csv(os.path.join(INPATH, fname_zip), index=False, compression="gzip")


def _download_file(url, max_retries: int, bytes_read: int = None) -> bytes:
    logger.info(f'Downloading data from "{url}"...')
    if bytes_read:
        headers = {"Range": f"bytes={bytes_read}-"}
    else:
        headers = {}
        bytes_read = 0
    content = b""
    try:
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            for chunk in tqdm(r.iter_content(chunk_size=CHUNK_SIZE)):
                bytes_read += CHUNK_SIZE
                content += chunk
    except requests.exceptions.ChunkedEncodingError:
        if max_retries > 0:
            logger.info(
                "Encountered ChunkedEncodingError, attempting to resume " "download..."
            )
            content += _download_file(
                url, max_retries - 1, bytes_read
            )  # attempt to resume download
        else:
            logger.info(
                "Encountered ChunkedEncodingError, but max_retries has been "
                "exceeded. Download may not have been fully completed."
            )
    return content


if __name__ == "__main__":
    main()
