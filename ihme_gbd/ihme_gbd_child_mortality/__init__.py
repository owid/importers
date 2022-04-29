import os

from ihme_gbd.ihme_gbd_risk import CALCULATE_OWID_VARS

DATASET_NAME = "IHME - Global Burden of Disease - Child Mortality"
DATASET_AUTHORS = "Institute for Health Metrics and Evaluation "
DATASET_VERSION = "2022-02"
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
        "ihme_gbd_child_mortality",
        "input",
    )
):
    INPATH = os.path.join(
        "/mnt",
        "owid_staging_neurath_temp",
        "importers",
        "ihme_gbd_child_mortality",
        "input",
    )
elif os.path.isdir(
    os.path.join("/mnt", "importers", "data", "gbd_2021", "gbd_child_mortality")
):
    INPATH = os.path.join(
        "/mnt", "importers", "data", "gbd_2021", "gbd_child_mortality"
    )
else:
    INPATH = os.path.join(DATASET_DIR, "input")
ENTFILE = os.path.join(INPATH, "entities-" + DATASET_VERSION + ".csv")
OUTPATH = os.path.join(DATASET_DIR, "output")
DATASET_RETRIEVED_DATE = "2022-02-01"
URL_STUB = "https://s3.healthdata.org/gbd-api-2019-public/88c7860e9edbbdb5d1e27878596a54eb_files/IHME-GBD_2019_DATA-88c7860e-"  # link to data without "X.zip"
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
CALCULATE_OWID_VARS = True
UPDATE_EXISTING_DATA_VERSION = True
