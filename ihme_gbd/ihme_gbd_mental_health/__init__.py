from ihme_gbd.ihme_gbd_cause import UPDATE_EXISTING_DATA_VERSION
from ihme_gbd.ihme_gbd_prevalence import URL_STUB
import os

DATASET_NAME = "IHME - Global Burden of Disease - Mental Health"
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
        "ihme_gbd_mental_health",
        "input",
    )
):
    INPATH = os.path.join(
        "/mnt",
        "owid_staging_neurath_temp",
        "importers",
        "ihme_gbd_mental_health",
        "input",
    )
elif os.path.isdir(
    os.path.join("/mnt", "importers", "data", "gbd_2021", "gbd_mental_health")
):
    INPATH = os.path.join("/mnt", "importers", "data", "gbd_2021", "gbd_mental_health")
else:
    INPATH = os.path.join(DATASET_DIR, "input")
ENTFILE = os.path.join(INPATH, "entities-" + DATASET_VERSION + ".csv")
OUTPATH = os.path.join(DATASET_DIR, "output")
DATASET_RETRIEVED_DATE = "2021-10-05"
URL_STUB = "https://s3.healthdata.org/gbd-api-2019-public/a97cb290845e5764ac77a3e9b5111a8d_files/IHME-GBD_2019_DATA-a97cb290-"
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
