import os

# Dataset constants.
DATASET_NAME = "World Inequality Database"
DATASET_AUTHORS = "World Inequality Lab"
DATASET_VERSION = "2020"
DATASET_LINK = "https://datacatalog.worldbank.org/dataset/education-statistics"
DATASET_ADDITIONAL_INFO = "The World Inequality Database aims to provide open and convenient access to an extensive available database on the historical evolution of the world distribution of income and wealth, both within countries and between countries. For further information, please visit the WID.world website: https://wid.world/"
DATASET_RETRIEVED_DATE = "September 28, 2021"
DATASET_DIR = os.path.dirname(__file__).split("/")[-1]
DATASET_NAMESPACE = f"{DATASET_DIR}@{DATASET_VERSION}"
CONFIGPATH = os.path.join(DATASET_DIR, "config")
INPATH = os.path.join(DATASET_DIR, "input")
OUTPATH = os.path.join(DATASET_DIR, "output")
