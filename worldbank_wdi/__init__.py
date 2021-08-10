import os

# Dataset constants.
DATASET_NAME = "World Development Indicators"
DATASET_AUTHORS = "World Bank"
DATASET_VERSION = "2021.07.30"
DATASET_LINK = "http://data.worldbank.org/data-catalog/world-development-indicators"
DATASET_RETRIEVED_DATE = "08-August-2021"
DATASET_DIR = os.path.dirname(__file__).split("/")[-1]
DATASET_NAMESPACE = f"{DATASET_DIR}@{DATASET_VERSION}"
CONFIGPATH = os.path.join(DATASET_DIR, "config")
INPATH = os.path.join(DATASET_DIR, "input")
OUTPATH = os.path.join(DATASET_DIR, "output")


# Cleaning config

# CLEAN_ALL_VARIABLES: if True, cleans and upserts all variables in the WB WDI
# dataset. Otherwise, only cleans and upserts variables that have been previously
# used in an OWID chart.
CLEAN_ALL_VARIABLES = True
