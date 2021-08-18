import os
import json
from datetime import datetime

import pandas as pd


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


def create_datsets(metadata: dict, output_dir: str) -> str:
    """Create datasets.csv file.

    Args:
        metadata (dict): Metadata dictionary.
        output_dir (str): Folder to store all generated files.

    Returns:
        str: Path to datasets CSV file.
    """
    # Create data file
    df = pd.DataFrame([{"id": 0, "name": metadata["source_name"]}])
    # Export
    output_path = os.path.join(output_dir, "datasets.csv")
    df.to_csv(output_path, index=False)
    return output_path


def create_sources(metadata: dict, output_dir: str) -> str:
    """Create sources.csv file.

    Args:
        output_dir (str): Folder to store all generated files.
        metadata (dict): Metadata dictionary.

    Returns:
        str: Path to sources CSV file.
    """
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
    output_path = os.path.join(output_dir, "sources.csv")
    df.to_csv(output_path, index=False)
    return output_path


def create_dictinct_entities(entities_path: str, output_dir: str) -> str:
    """This file lists all entities present in the data, so that new entities can be created if necessary. Located in
    output/distinct_countries_standardized.csv:

    - name: name of the entity.

    Args:
        output_dir (str): Folder to store all generated files.
        entities_path (str): Path to standardized entity names.

    Returns:
        str: Path to entities CSV file.
    """
    output_path = os.path.join(output_dir, "distinct_countries_standardized.csv")
    if not os.path.exists(entities_path):
        raise FileNotFoundError(f"File {entities_path} was not found. It is required!")
    col = "Our World In Data Name"
    (
        pd.read_csv(
            entities_path,
            usecols=[col],
        )
        .rename(columns={col: "name"})
        .to_csv(output_path, index=False)
    )
    return output_path


def create_variables_datapoints(df: pd.DataFrame, output_dir: str) -> tuple:
    """Create datapoints_*.csv and variables.csv files

    Args:
        df (pd.DataFrame): Input data
        output_dir (str): Folder to store all generated files.

    Returns:
        tuple: Two strings, path to variables.csv file and datapoints/ directory.
    """
    # Create year column (latest value)
    df = df.copy().assign(
        year=df.Year.str.split("-", 1, expand=True)
        .astype("float")
        .max(axis=1)
        .astype(int)
    )
    # Build variables data frame
    df_var = _create_variables(df)
    path_variables = os.path.join(output_dir, "variables.csv")
    df.to_csv(path_variables, index=False)
    # Build datapoints dataframes
    var2id = _build_var_2_id(df_var)
    path_datapoints = create_datapoints(df, var2id, output_dir)
    return path_variables, path_datapoints


def _create_variables(df: pd.DataFrame) -> pd.DataFrame:
    """Create variables.csv file.

    Args:
        df (pd.DataFrame): Data.

    Returns:
        pd.DataFrame: Variables data frame.
    """
    # Build variable data frame
    grouper = df.groupby(["Item", "Item Code"])
    df = (
        pd.DataFrame(
            grouper.year.min().astype(str) + "-" + grouper.year.max().astype(str)
        )
        .reset_index()
        .reset_index()
        .rename(
            columns={
                "index": "id",
                "Item": "name",
                "Item Code": "code",
                "year": "timespan",
            }
        )
        .assign(
            dataset_id=0,
            sources_id=0,
            original_metadata=pd.NA,
            coverage=pd.NA,
            display=pd.NA,
            description=(
                "Download at Definitions and standards file for Item field at http://www.fao.org/faostat/en/#data/FS"
            ),
            unit=pd.NA,
            short_unit=pd.NA,
        )
    )
    return df


def _build_var_2_id(df):
    return dict(zip(df.name, df.id))


def create_datapoints(df: pd.DataFrame, var2id: dict, output_dir: str):
    """Create datapoints_*.csv files.

    Args:
        df (pd.DataFrame): Data.

    Returns:
        str: Path to folder containing datapoints CSV files.
    """
    output_path = os.path.join(output_dir, "datapoints")
    os.makedirs(output_path, exist_ok=True)
    # Create and export
    for name, i in var2id.items():
        df_ = (
            df.loc[df.Item == name][["Area", "year", "Value"]]
            .rename(
                columns={
                    "Area": "Country",
                    "year": "Year",
                }
            )
            .sort_values(["Country", "Year"])
        )
        df_.to_csv(os.path.join(output_path, f"datapoints_{i}.csv"), index=False)
    return output_path


def main(path_data: str, path_metadata: str, entities_path: str, output_dir: str):
    """Clean pipeline.

    Args:
        path_data (str): Path to downlaoded data file.
        path_metadata (str): Path to metadata file.
        entities_path (str): Path to entities standardized file. You can generate it with method
                             `generate_entitites_raw`
        output_dir (str): Folder where to export generated files.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    # Load data into memory
    df = load_data(path_data)
    metadata = load_metadata(path_metadata)
    # datasets.csv
    path_datasets = create_datsets(metadata, output_dir)
    # sources.csv
    path_sources = create_sources(metadata, output_dir)
    # distinct_countries_standardized.csv
    path_entities = create_dictinct_entities(entities_path, output_dir)
    # TODO: variables.csv, datapoints/datapoints_*.csv
    path_variables, path_datapoints = create_variables_datapoints(df, output_dir)
