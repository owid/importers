import os
import json
from datetime import datetime

import pandas as pd


DATASET_DIR = os.path.dirname(__file__)
OUTPATH = os.path.join(DATASET_DIR, "output")


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


def load_metadata(input_path: str):
    """Load metadata.

    Args:
        input_path (str): Path to metadata file (JSON). Assumed to have Walden format.

    Returns:
        pd.DataFrame: Input data
    """
    with open(input_path, "r") as f:
        return json.load(f)


def create_datsets(metadata: dict) -> pd.DataFrame:
    """Create datasets.csv file.

    Returns:
        pd.DataFrame: pd.Data
    """
    # Create data file
    df = pd.DataFrame([{"id": 0, "name": metadata["source_name"]}])
    # Export
    output_path = os.path.join(OUTPATH, "datasets.csv")
    df.to_csv(output_path, index=False)
    return output_path


def create_sources(metadata: dict):
    # Create sources data frame
    source_description = {
        "dataPublishedBy": metadata["source_name"],
        "dataPublisherSource": None,
        "link": metadata["url"],
        "retrievedDate": datetime.strptime(
            metadata["date_accessed"], "%Y-%m-%d"
        ).strftime("%d-%B-%Y"),
        "additionalInfo": None,
    }
    df = pd.DataFrame(
        [
            {
                "id": 0,
                "dataset_id": 0,
                "name": metadata["source_name"],
                "description": source_description,
            }
        ]
    )
    # Export
    output_path = os.path.join(OUTPATH, "sources.csv")
    df.to_csv(output_path, index=False)
    return output_path


def create_dictinct_entities():
    """This file lists all entities present in the data, so that new entities can be created if necessary. Located in
    output/distinct_countries_standardized.csv:

    - name: name of the entity.
    """
    output_path = os.path.join(OUTPATH, "distinct_countries_standardized.csv")
    col = "Our World In Data Name"
    (
        pd.read_csv(
            os.path.join(DATASET_DIR, "config", "standardized_entity_names.csv"),
            usecols=[col],
        )
        .rename(columns={col: "name"})
        .to_csv(output_path, index=False)
    )
    return output_path


def create_variables():
    return "Not Implemented"


def create_datapoints():
    return "Not Implemented"


def main(path_data: str, path_metadata: str):
    # Load data into memory
    df = load_data(path_data)
    metadata = load_metadata(path_metadata)
    # datasets.csv
    path_datasets = create_datsets(metadata)
    # sources.csv
    path_sources = create_sources(metadata)
    # distinct_countries_standardized.csv
    path_entities = create_dictinct_entities()
    # TODO: variables.csv
    path_variables = create_variables()
    # TODO: datapoints.csv
    path_datapoints = create_datapoints()
