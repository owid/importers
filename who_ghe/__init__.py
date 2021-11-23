import os
import pathlib
import datetime

DATASET_NAME = "Global Health Estimates"
DATASET_AUTHORS = "World Health Organization"
DATASET_VERSION = "2021-09"
DATASET_LINK = "https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_COD_COMPLETE"
DATASET_RETRIEVED_DATE = "2021-09-08"
DATASET_DIR = "who_ghe"  # os.path.dirname(__file__).split("/")[-1]
DATASET_NAMESPACE = f"{DATASET_DIR}@{DATASET_VERSION}"
INPATH = os.path.join(DATASET_DIR, "input")
OUTPATH = os.path.join(DATASET_DIR, "output")
INFILE = os.path.join(INPATH, "who_ghe", "_all_countries.csv")
ENTFILE = os.path.join(INPATH, "entities-" + DATASET_VERSION + ".csv")
CONFIGPATH = os.path.join(DATASET_DIR, "config")
