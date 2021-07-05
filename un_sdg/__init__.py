import os

DATASET_NAME = "United Nations Sustainable Development Goals Indicators"
DATASET_AUTHORS = "United Nations"
DATASET_VERSION = "2021-03"
DATASET_LINK = "https://unstats.un.org/sdgs/indicators/database/"
DATASET_RETRIEVED_DATE = "10-May-2021"
DATASET_DIR = os.path.dirname(__file__)
DATASET_NAMESPACE = f"{DATASET_DIR.split('/')[-1]}@{DATASET_VERSION}"
CONFIGPATH = os.path.join(DATASET_DIR, "config")
INPATH = os.path.join(DATASET_DIR, "input")
OUTPATH = os.path.join(DATASET_DIR, "output")
INFILE = os.path.join(INPATH, "un-sdg-" + DATASET_VERSION + ".csv.zip")
ENTFILE = os.path.join(INPATH, "entities-" + DATASET_VERSION + ".csv")
