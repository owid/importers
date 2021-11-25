import os

DATASET_NAME = "IHME - Global Burden of Disease - Deaths and DALYs"
DATASET_AUTHORS = "Institute for Health Metrics and Evaluation "
DATASET_VERSION = "2021-09"
DATASET_LINK = "http://ghdx.healthdata.org/gbd-results-tool"
DATASET_DIR = os.path.join("ihme_gbd", os.path.dirname(__file__).split("/")[-1])
NAMESPACE = os.path.dirname(__file__).split("/")[-1]
DATASET_NAMESPACE = f"{NAMESPACE}@{DATASET_VERSION}"
CONFIGPATH = os.path.join(DATASET_DIR, "config")
CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))
# INPATH = os.path.join(
#    "/mnt",
#    "owid_staging_neurath_temp",
#    "importers",
#    "ihme_gbd_cause",
#    "input",
# )
INPATH = os.path.join(DATASET_DIR, "input")
ENTFILE = os.path.join(INPATH, "entities-" + DATASET_VERSION + ".csv")
OUTPATH = os.path.join(DATASET_DIR, "output")
DATASET_RETRIEVED_DATE = "2021-09-22"
URL_STUB = "https://s3.healthdata.org/gbd-api-2019-public/6bc81b0cecef147c44df55608fe573f3_files/IHME-GBD_2019_DATA-6bc81b0c-"
DATAPOINTS_DIR = os.path.join(DATASET_DIR, "output", "datapoints")
