import os
# Dataset constants.
DATASET_NAME = "World Development Indicators"
DATASET_AUTHORS = "World Bank"
DATASET_VERSION = "2021.03"
DATASET_LINK = "http://data.worldbank.org/data-catalog/world-development-indicators"
DATASET_RETRIEVED_DATE = "16-April-2021"

DATASET_DIR = os.path.dirname(__file__)
# CURRENT_DIR = os.path.join(os.getcwd(), 'worldbank_wdi')
CONFIGPATH = os.path.join(DATASET_DIR, 'config')
INPATH = os.path.join(DATASET_DIR, 'input')
OUTPATH = os.path.join(DATASET_DIR, 'output')
