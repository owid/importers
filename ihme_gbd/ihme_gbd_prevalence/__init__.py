import os

DATASET_NAME = "IHME - Global Burden of Disease - Prevalence & Incidence"
DATASET_AUTHORS = "Institute for Health Metrics and Evaluation "
DATASET_VERSION = "2021-09"
DATASET_LINK = "http://ghdx.healthdata.org/gbd-results-tool"
PARENT_DIR = "ihme_gbd"
DATASET_DIR = os.path.join(PARENT_DIR, os.path.dirname(__file__).split("/")[-1])
NAMESPACE = os.path.dirname(__file__).split("/")[-1]
DATASET_NAMESPACE = f"{NAMESPACE}@{DATASET_VERSION}"
CONFIGPATH = os.path.join(DATASET_DIR, "config")
if os.path.isdir(
    os.path.join(
        "/mnt",
        "owid_staging_neurath_temp",
        "importers",
        "ihme_gbd_prevalence",
        "input",
    )
):
    INPATH = os.path.join(
        "/mnt",
        "owid_staging_neurath_temp",
        "importers",
        "ihme_gbd_prevalence",
        "input",
    )
elif os.path.isdir(
    os.path.join("/mnt", "importers", "data", "gbd_2021", "gbd_prevalence")
):
    INPATH = os.path.join("/mnt", "importers", "data", "gbd_2021", "gbd_prevalence")
else:
    INPATH = os.path.join(DATASET_DIR, "input")
ENTFILE = os.path.join(INPATH, "entities-" + DATASET_VERSION + ".csv")
OUTPATH = os.path.join(DATASET_DIR, "output")
DATASET_RETRIEVED_DATE = "2021-10-04"
URL_STUB = "https://s3.healthdata.org/gbd-api-2019-public/a9c3199ebacbc64ad2c3d33375b975bb_files/IHME-GBD_2019_DATA-a9c3199e-"  # this will need to be updated - it times out pretty quickly (couple of days)
DATAPOINTS_DIR = os.path.join(DATASET_DIR, "output", "datapoints")
CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))
CLEAN_ALL_VARIABLES = False
FILTER_FIELDS = [
    "measure",
    "location",
    "sex",
    "age",
    "cause",
    "metric",
    "year",
    "val",
]
COUNTRY_COL = "location"
CALCULATE_OWID_VARS = False
UPDATE_EXISTING_DATA_VERSION = True
