import os
import re
import zipfile
import logging
from io import BytesIO
import requests

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
    res = requests.get("https://figshare.com/ndownloader/files/30968722")
    zf = zipfile.ZipFile(BytesIO(res.content))
    zf.extractall(os.path.join(DATASET_DIR, "input"))


def download_official() -> None:
    """downloads the official data supplement to the Global Carbon Budget
    paper.

    Contains time series from 1959-today.
    """
    mk_inpath()
    # national data
    res = requests.get(
        "https://data.icos-cp.eu/licence_accept?ids=%5B%22xUUehljs1oTazlGlmigAhvfe%22%5D"
    )
    fname = re.search(
        r'attachment; filename="(.+)"', res.headers["Content-Disposition"]
    ).groups()[0]
    with open(os.path.join(DATASET_DIR, "input", fname), "wb") as f:
        f.write(res.content)

    # global data
    res = requests.get(
        "https://data.icos-cp.eu/licence_accept?ids=%5B%226QlPjfn_7uuJtAeuGGFXuPwz%22%5D"
    )
    fname = re.search(
        r'attachment; filename="(.+)"', res.headers["Content-Disposition"]
    ).groups()[0]
    with open(os.path.join(DATASET_DIR, "input", fname), "wb") as f:
        f.write(res.content)


if __name__ == "__main__":
    main()
