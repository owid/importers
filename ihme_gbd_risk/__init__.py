import os

DATASET_NAME = "IHME - Global Burden of Disease - Risk Factors"
DATASET_AUTHORS = "Institute for Health Metrics and Evaluation "
DATASET_VERSION = "2021-09"
DATASET_LINK = "http://ghdx.healthdata.org/gbd-results-tool"
DATASET_DIR = os.path.dirname(__file__).split("/")[-1]
DATASET_NAMESPACE = f"{DATASET_DIR}@{DATASET_VERSION}"
CONFIGPATH = os.path.join(DATASET_DIR, "config")
INPATH = os.path.join(DATASET_DIR, "input")
ENTFILE = os.path.join(INPATH, "entities-" + DATASET_VERSION + ".csv")
OUTPATH = os.path.join(DATASET_DIR, "output")
DATASET_RETRIEVED_DATE = "2021-10-01"
