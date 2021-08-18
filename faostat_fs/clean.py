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

    This is required to then produce the country_standardized table using Grapher Admin UI.

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

def load_data(input_path: str) -> pd.DataFrame:
    """Load input CSV data.

    Args:
        input_path (str): Path to input data (csv).

    Returns:
        pd.DataFrame: Input data
    """
    df = pd.read_csv(input_path)
    # Drop nulls
    df = df.dropna(subset=["Value", "Area"])
    return df


def create_datsets() -> pd.DataFrame:
    """Create datasets.csv file.

    Returns:
        pd.DataFrame: pd.Data
    """
    # Create data file
    df = pd.DataFrame(
        [{"id": 0, "name": f"{DATASET_NAME} - {DATASET_AUTHORS} ({DATASET_VERSION})"}]
    )
    # Export
    df.to_csv(os.path.join(OUTPATH, "datasets.csv"), index=False)


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
    col = "Our World In Data Name"
    (
        pd.read_csv(
            os.path.join(DATASET_DIR, "config", "standardized_entity_names.csv"),
            usecols=[col],
        )
        .rename(columns={col: "name"})
        .to_csv(
            os.path.join(OUTPATH, "distinct_countries_standardized.csv"), index=False
        )
    )


def create_variables():
    pass


def main(input_path: str):
    # Load data into memory
    df = load_data(input_path)
    # datasets.csv
    create_datsets()
    # distinct_countries_standardized.csv
    create_dictinct_entities()
