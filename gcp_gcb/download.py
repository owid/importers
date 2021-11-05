import os
import re
import logging
import requests
import pandas as pd

from gcp_gcb import DATASET_DIR, INPATH
from utils import delete_input

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main() -> None:
    logger.info("Downloading dataset...")
    delete_input(DATASET_DIR)
    download_official()
    download_unofficial()


def mk_inpath() -> None:
    if not os.path.exists(INPATH):
        os.makedirs(INPATH)


def download_unofficial() -> None:
    """downloads the unofficial Global Carbon Budget data.

    Contains time series from 1750-today.
    """
    mk_inpath()

    # retrieves data
    url_data = "https://zenodo.org/record/5569235/files/GCB2021v34_MtCO2_flat.csv"
    fname_data = url_data.split("/")[-1]
    pd.read_csv(url_data, encoding="ISO-8859-1").to_csv(
        os.path.join(INPATH, fname_data), index=False
    )

    # retrieves variable metadata
    url_meta = (
        "https://zenodo.org/record/5569235/files/GCB2021v34_MtCO2_flat_metadata.json"
    )
    fname_meta = url_meta.split("/")[-1]
    res = requests.get(url_meta)
    assert res.ok
    with open(os.path.join(INPATH, fname_meta), "w") as f:
        f.write(res.text)


def download_official() -> None:
    """downloads the official data supplement to the Global Carbon Budget
    paper.

    Contains time series from 1959-today.
    """
    mk_inpath()
    # national data
    res = requests.get(
        "https://data.icos-cp.eu/licence_accept?ids=%5B%22rmU_viZcddCV7LdflaFGN-My%22%5D"
    )
    fname = re.search(
        r'attachment; filename="(.+)"', res.headers["Content-Disposition"]
    ).groups()[0]
    with open(os.path.join(INPATH, fname), "wb") as f:
        f.write(res.content)

    # global data
    res = requests.get(
        "https://data.icos-cp.eu/licence_accept?ids=%5B%22axNWlHezpbMiXg1Z1VyFI9Fa%22%5D"
    )
    fname = re.search(
        r'attachment; filename="(.+)"', res.headers["Content-Disposition"]
    ).groups()[0]
    with open(os.path.join(INPATH, fname), "wb") as f:
        f.write(res.content)


if __name__ == "__main__":
    main()
