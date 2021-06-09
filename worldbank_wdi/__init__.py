import os
# Dataset constants.
DATASET_NAME = "World Development Indicators"
DATASET_AUTHORS = "World Bank"
DATASET_VERSION = "2021.05.25"
DATASET_LINK = "http://data.worldbank.org/data-catalog/world-development-indicators"
DATASET_RETRIEVED_DATE = "08-June-2021"
DATASET_DIR = os.path.dirname(__file__).split('/')[-1]
DATASET_NAMESPACE = f"{DATASET_DIR}@{DATASET_VERSION}"
CONFIGPATH = os.path.join(DATASET_DIR, 'config')
INPATH = os.path.join(DATASET_DIR, 'input')
OUTPATH = os.path.join(DATASET_DIR, 'output')
