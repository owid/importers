import os

# Dataset constants.
DATASET_NAME = "Statistical Review of World Energy"
DATASET_AUTHORS = "BP"
DATASET_VERSION = "2021"
DATASET_LINK = "https://www.bp.com/en/global/corporate/energy-economics/statistical-review-of-world-energy.html"
DATASET_RETRIEVED_DATE = "08-July-2021"
DATASET_DIR = os.path.dirname(__file__).split("/")[-1]
DATASET_NAMESPACE = f"{DATASET_DIR}@{DATASET_VERSION}"
CONFIGPATH = os.path.join(DATASET_DIR, "config")
INPATH = os.path.join(DATASET_DIR, "input")
OUTPATH = os.path.join(DATASET_DIR, "output")
