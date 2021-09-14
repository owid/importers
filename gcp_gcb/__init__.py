import os

# Dataset constants.
DATASET_NAME = "Global Carbon Budget"
DATASET_AUTHORS = "Global Carbon Project"
DATASET_VERSION = "2020"
DATASET_LINK = "https://folk.universitetetioslo.no/roberan/GCB2020.shtml"
DATASET_RETRIEVED_DATE = "14-September-2021"
DATASET_DIR = os.path.dirname(__file__).split("/")[-1]
DATASET_NAMESPACE = f"{DATASET_DIR}@{DATASET_VERSION}"
CONFIGPATH = os.path.join(DATASET_DIR, "config")
INPATH = os.path.join(DATASET_DIR, "input")
OUTPATH = os.path.join(DATASET_DIR, "output")
