import os

DATASET_NAME = "IHME - Global Burden of Disease - Prevalence & Incidence"
DATASET_AUTHORS = "Institute for Health Metrics and Evaluation "
DATASET_VERSION = "2021-09"
DATASET_LINK = "http://ghdx.healthdata.org/gbd-results-tool"
DATASET_DIR = os.path.join("ihme_gbd", os.path.dirname(__file__).split("/")[-1])
NAMESPACE = os.path.dirname(__file__).split("/")[-1]
DATASET_NAMESPACE = f"{NAMESPACE}@{DATASET_VERSION}"
CONFIGPATH = os.path.join(DATASET_DIR, "config")
INPATH = os.path.join(DATASET_DIR, "input")
ENTFILE = os.path.join(INPATH, "entities-" + DATASET_VERSION + ".csv")
OUTPATH = os.path.join(DATASET_DIR, "output")
DATASET_RETRIEVED_DATE = "2021-10-04"
URL_STUB = "https://s3.healthdata.org/gbd-api-2019-public/aaa9739a148f29b5e234b43154512da9_files/IHME-GBD_2019_DATA-aaa9739a-"  # this will need to be updated - it times out pretty quickly (couple of days)
