import os

# Dataset constants.
DATASET_NAME = "World Bank EdStats"
DATASET_AUTHORS = "World Bank"
DATASET_VERSION = "2020"
DATASET_LINK = "https://datacatalog.worldbank.org/dataset/education-statistics"
DATASET_RETRIEVED_DATE = "25-August-2021"
DATASET_DIR = os.path.dirname(__file__).split("/")[-1]
DATASET_NAMESPACE = f"{DATASET_DIR}@{DATASET_VERSION}"
CONFIGPATH = os.path.join(DATASET_DIR, "config")
INPATH = os.path.join(DATASET_DIR, "input")
OUTPATH = os.path.join(DATASET_DIR, "output")
