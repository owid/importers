import os
import pandas as pd


DATASET_DIR = os.path.dirname(__file__)
OUTPATH = os.path.join(DATASET_DIR, "output")
DATASET_NAME = "Food Security and Nutrition: Suite of Food Security Indicators"
DATASET_AUTHORS = "Food and Agriculture Organization of the United Nations"
DATASET_VERSION = "2021-07-21"
DATASET_RETRIEVED_DATE = "01-August-21"


def generate_entitites_raw(df: pd.DataFrame, output_path: str):
    """Generate raw entity table file.

    Args:
        df (pd.DataFrame): Original source data.
        output_path (str): Path to store entity table file (contains one column : "Country").
    """
    (
        df[["Area"]]
        .drop_duplicates()
        .dropna()
        .rename(columns={"Area": "Country"})
        .to_csv(output_path, index=False)
    )


def load(input_path: str):
    df = pd.read_csv(input_path)
    # Drop nulls
    df = df.dropna(subset=["Value", "Area"])
    return df


def create_datsets() -> pd.DataFrame:
    df = pd.DataFrame({
         {"id": 0, "name": f"{DATASET_NAME} - {DATASET_AUTHORS} ({DATASET_VERSION})"}
    })
    df.to_csv(os.path.join(OUTPATH, "datasets.csv"), index=False)
    return df


def create_sources():
    source_description_template = {
        "dataPublishedBy": "Food and Agriculture Organization of the United Nations",
        "dataPublisherSource": None,
        "link": "http://www.fao.org/faostat/en/#data",
        "retrievedDate": DATASET_RETRIEVED_DATE,
        "additionalInfo": None,
    }


def create_dictinct_entities():
    """This file lists all entities present in the data, so that new entities can be created if necessary. Located in
    output/distinct_countries_standardized.csv:
    
    - name: name of the entity.
    """
    pass
#     df = None
#     df.to_csv(
#         os.path.join(OUTPATH, "distinct_countries_standardized.csv"), index=False
#     ) 