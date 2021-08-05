import os
import pathlib
import datetime

DATASET_NAME = "United Nations Sustainable Development Goals"
DATASET_AUTHORS = "United Nations"
DATASET_VERSION = "2021-03"
DATASET_LINK = "https://unstats.un.org/sdgs/indicators/database/"
DATASET_DIR = os.path.dirname(__file__).split("/")[-1]
DATASET_NAMESPACE = f"{DATASET_DIR}@{DATASET_VERSION}"
CONFIGPATH = os.path.join(DATASET_DIR, "config")
INPATH = os.path.join(DATASET_DIR, "input")
OUTPATH = os.path.join(DATASET_DIR, "output")
INFILE = os.path.join(INPATH, "un-sdg-" + DATASET_VERSION + ".csv.zip")
ENTFILE = os.path.join(INPATH, "entities-" + DATASET_VERSION + ".csv")
fname = pathlib.Path(INFILE)
mtime = datetime.datetime.fromtimestamp(fname.stat().st_mtime)
DATASET_RETRIEVED_DATE = mtime.strftime("%d-%B-%y")
METAPATH = os.path.join(DATASET_DIR, "metadata")
METADATA_LOC = "https://unstats.un.org/sdgs/metadata/files/SDG-indicator-metadata.zip"
