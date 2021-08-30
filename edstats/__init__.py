import os

# Dataset constants.
DATASET_NAME = "World Bank EdStats"
DATASET_AUTHORS = "World Bank"
DATASET_VERSION = "2020"
DATASET_LINK = "https://datacatalog.worldbank.org/dataset/education-statistics"
DATASET_ADDITIONAL_INFO = "The World Bank EdStats data holds over 4,000 internationally comparable indicators that describe education access, progression, completion, literacy, teachers, population, and expenditures. The indicators cover the education cycle from pre-primary to vocational and tertiary education. The data also holds learning outcome data from international and regional learning assessments (e.g. PISA, TIMSS, PIRLS), equity data from household surveys, and projection/attainment data to 2050. For further information, please visit the EdStats website: https://datatopics.worldbank.org/education/"
DATASET_RETRIEVED_DATE = "August 25, 2021"
DATASET_DIR = os.path.dirname(__file__).split("/")[-1]
DATASET_NAMESPACE = f"{DATASET_DIR}@{DATASET_VERSION}"
CONFIGPATH = os.path.join(DATASET_DIR, "config")
INPATH = os.path.join(DATASET_DIR, "input")
OUTPATH = os.path.join(DATASET_DIR, "output")
