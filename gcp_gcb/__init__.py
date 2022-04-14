import os

# Dataset constants.
DATASET_NAME = "Global Carbon Budget"
DATASET_AUTHORS = "Global Carbon Project"
DATASET_VERSION = "v2021"
DATASET_LINK = "https://doi.org/10.18160/gcp-2021"
DATASET_RETRIEVED_DATE = "14-April-2022"
DATASET_DIR = os.path.dirname(__file__).split("/")[-1]
DATASET_NAMESPACE = f"{DATASET_DIR}@{DATASET_VERSION}"
CONFIGPATH = os.path.join(DATASET_DIR, "config")
INPATH = os.path.join(DATASET_DIR, "input")
OUTPATH = os.path.join(DATASET_DIR, "output")
