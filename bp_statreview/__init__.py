import os
from datetime import datetime

# Dataset constants (that should not change on each update).
DATASET_NAME = "Statistical Review of World Energy"
DATASET_AUTHORS = "BP"
# URL to the web where all data can be manually downloaded.
DATASET_LINK = "https://www.bp.com/en/global/corporate/energy-economics/statistical-review-of-world-energy.html"

# Dataset variables (that change on each update).
# Dataset version, which is the year when it was released, and is assumed to be the current year (e.g. "2022").
DATASET_VERSION = str(datetime.today().year)
# URL to the download link to the excel file with all data.
ALL_DATA_LINK = f"https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/xlsx/energy-economics/statistical-review/bp-stats-review-{DATASET_VERSION}-all-data.xlsx"
# URL to the download link to the consolidated dataset in panel format.
CONSOLIDATED_DATASET_PANEL_FORMAT_LINK = f"https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/xlsx/energy-economics/statistical-review/bp-stats-review-{DATASET_VERSION}-consolidated-dataset-panel-format.csv"
# URL to the download link to the consolidated dataset in narrow format.
CONSOLIDATED_DATASET_NARROW_FORMAT_LINK = f"https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/xlsx/energy-economics/statistical-review/bp-stats-review-{DATASET_VERSION}-consolidated-dataset-narrow-format.csv"
# Current date, when the data was retrieved (e.g. "07-July-2022").
DATASET_RETRIEVED_DATE = datetime.today().strftime("%d-%B-%y")

# Derived constants (that should not be edited).
DATASET_DIR = os.path.dirname(__file__).split("/")[-1]
DATASET_NAMESPACE = f"{DATASET_DIR}@{DATASET_VERSION}"
CONFIGPATH = os.path.join(DATASET_DIR, "config")
INPATH = os.path.join(DATASET_DIR, "input")
OUTPATH = os.path.join(DATASET_DIR, "output")
