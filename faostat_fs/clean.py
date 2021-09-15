import os
from datetime import datetime

import pandas as pd

from faostat_fs.utils import load_data, load_metadata_walden, load_metadata_fao


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


def create_datsets(metadata: dict, output_dir: str) -> str:
    """Create datasets.csv file.

    Args:
        metadata (dict): Metadata dictionary.
        output_dir (str): Folder to store all generated files.

    Returns:
        str: Path to datasets CSV file.
    """
    # Create data file
    df = pd.DataFrame([{"id": 0, "name": metadata["name"]}])
    # Export
    output_path = os.path.join(output_dir, "datasets.csv")
    df.to_csv(output_path, index=False)
    return output_path


def create_sources(
    df_meta_fao: pd.DataFrame, metadata_walden: dict, output_dir: str
) -> str:
    """Create sources.csv file.

    Args:
        output_dir (str): Folder to store all generated files.
        metadata_walden (dict): Metadata dictionary.

    Returns:
        str: Path to sources CSV file.
    """

    def _create_source_description(name, metadata):
        return {
            "dataPublishedBy": name,
            "dataPublisherSource": None,
            "link": metadata["url"],
            "retrievedDate": datetime.strptime(
                metadata["date_accessed"], "%Y-%m-%d"
            ).strftime("%d-%B-%Y"),
            "additionalInfo": None,
        }

    cols = ["source_name", "source_id"]
    df = df_meta_fao[cols].drop_duplicates()
    df = df.rename(columns={"source_name": "name", "source_id": "id"})
    df = df.assign(
        dataset_id=0,
        description=df.name.apply(
            lambda name: _create_source_description(name, metadata_walden)
        ),
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


def create_variables_datapoints(
    df: pd.DataFrame, df_meta_fao: pd.DataFrame, output_dir: str
) -> tuple:
    """Create datapoints_*.csv and variables.csv files

    Args:
        df (pd.DataFrame): Input data.
        df_meta_fao (pd.DataFrame): Auxiliary metadata table from FAO.
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
    _check_uniqueness(df)
    # Build variables data frame
    df_var = _create_variables(df, df_meta_fao)
    path_variables = os.path.join(output_dir, "variables.csv")
    df_var.to_csv(path_variables, index=False)
    # Build datapoints dataframes
    var2id = _build_var_2_id(df_var)
    path_datapoints = create_datapoints(df, var2id, output_dir)
    return path_variables, path_datapoints


def _check_uniqueness(df: pd.DataFrame) -> pd.DataFrame:
    if (df.groupby("Item")[["Item Code", "Unit"]].nunique() > 1).any(axis=None):
        raise ValueError(
            "Field `Item` should have only one `Item Code` and `Unit` value."
        )


def _create_variables(df: pd.DataFrame, df_meta_fao: pd.DataFrame) -> pd.DataFrame:
    """Create variables.csv file.

    Args:
        df (pd.DataFrame): Data.
        df_meta_fao (pd.DataFrame): Auxiliary metadata table from FAO.

    Returns:
        pd.DataFrame: Variables data frame.
    """
    # Build variable data frame
    grouper = df.groupby(["Item", "Item Code", "Unit"])
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
                "Unit": "unit",
            }
        )
        .assign(
            dataset_id=0,
            sources_id=0,  # Build from special df
            original_metadata=pd.NA,
            coverage=pd.NA,
            display=pd.NA,
            description=(  # Build from special df
                "Download at Definitions and standards file for Item field at"
                " http://www.fao.org/faostat/en/#data/FS"
            ),
            short_unit=pd.NA,
        )
    )
    # print(df.shape)
    # print(df_meta_fao.columns)
    df = df.merge(
        df_meta_fao[["description", "source_id", "variable_name"]],
        left_on="name",
        right_on="variable_name",
    )
    # print(df.shape)
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


def main(
    path_data: str,
    path_metadata_walden: str,
    path_metadata_fao: str,
    entities_path: str,
    output_dir: str,
):
    """Clean pipeline.

    Args:
        path_data (str): Path to downlaoded data file.
        path_metadata_walden (str): Path to metadata file in Walden.
        path_metadata_fao (str): Path to metadata file in FAO site.
        entities_path (str): Path to entities standardized file. You can generate it with method
                             `generate_entitites_raw`
        output_dir (str): Folder where to export generated files.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    # Load data into memory
    df = load_data(path_data)
    metadata_walden = load_metadata_walden(path_metadata_walden)
    df_meta_fao = load_metadata_fao(path_metadata_fao)
    # datasets.csv
    print("Cleaning datasets")
    path_datasets = create_datsets(metadata_walden, output_dir)
    # sources.csv
    print("Cleaning sources")
    path_sources = create_sources(df_meta_fao, metadata_walden, output_dir)
    # distinct_countries_standardized.csv
    print("Cleaning entities")
    path_entities = create_dictinct_entities(entities_path, output_dir)
    # variables.csv and datapoints/datapoints_*.csv
    print("Cleaning variables and datapoints")
    path_variables, path_datapoints = create_variables_datapoints(
        df, df_meta_fao, output_dir
    )
