import os

# Dataset constants.
DATASET_NAME = "Global Health Observatory"
DATASET_AUTHORS = "World Health Organization"
DATASET_VERSION = "2021.12"
DATASET_LINK = "https://ghoapi.azureedge.net/api/"
DATASET_RETRIEVED_DATE = "14-December-2021"
DELETE_EXISTING_INPUTS = True
DOWNLOAD_INPUTS = True
KEEP_PATHS = ["standardized_entity_names.csv"]
CURRENT_DIR = os.path.dirname(__file__)
INPATH = os.path.join(CURRENT_DIR, "input")
OUTPATH = os.path.join(CURRENT_DIR, "output")
CONFIGPATH = os.path.join(CURRENT_DIR, "config")
SELECTED_VARS_ONLY = False  # should we download just the selected vars in config/
DATASET_DIR = os.path.dirname(__file__).split("/")[-1]
DATASET_NAMESPACE = f"{DATASET_DIR}@{DATASET_VERSION}"
FIX_VAR_CODE = True  # Fixing issues where there is uplication of indicator and description in get_metadata_url
DELETE_OUTPUT = False
