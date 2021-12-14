import os

# Dataset constants.
DATASET_NAME = "Global Health Observatory"
DATASET_AUTHORS = "World Health Organization"
DATASET_VERSION = "2021.03"
DATASET_LINK = "https://ghoapi.azureedge.net/api/"
DATASET_RETRIEVED_DATE = "31-March-2021"
DELETE_EXISTING_INPUTS = True
DOWNLOAD_INPUTS = True
KEEP_PATHS = ["standardized_entity_names.csv"]
CURRENT_DIR = os.path.dirname(__file__)
INPATH = os.path.join(CURRENT_DIR, "input")
OUTPATH = os.path.join(CURRENT_DIR, "output")
CONFIGPATH = os.path.join(CURRENT_DIR, "config")
SELECTED_VARS_ONLY = False  # should we download just the selected vars in config/
