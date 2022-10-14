# type: ignore
"""Imports a dataset and associated data sources, variables, and data points
into the SQL database.

Usage:

    >>> from standard_importer import import_dataset
    >>> dataset_dir = "worldbank_wdi"
    >>> dataset_namespace = "worldbank_wdi@2021.05.25"
    >>> import_dataset.main(dataset_dir, dataset_namespace)
"""

import logging
import os
import re
import traceback
from glob import glob

import pandas as pd
from db import get_connection
from db_utils import DBUtils
from dotenv import load_dotenv
from tqdm import tqdm

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()
DEBUG = os.getenv("DEBUG") == "True"
USER_ID = int(os.getenv("USER_ID"))  # type: ignore

CURRENT_DIR = os.path.dirname(__file__)
# CURRENT_DIR = os.path.join(os.getcwd(), 'standard_importer')


def main(dataset_dir: str, dataset_namespace: str):
    data_path = os.path.join(dataset_dir, "output")

    try:
        connection = get_connection()
        connection.autocommit(False)
        cursor = connection.cursor()
        db = DBUtils(cursor)

        # Upsert entities
        logger.info("---\nUpserting entities...")
        entities = pd.read_csv(
            os.path.join(data_path, "distinct_countries_standardized.csv")
        )
        for entity_name in tqdm(entities.name):
            db_entity_id = db.get_or_create_entity(entity_name)
            entities.loc[entities.name == entity_name, "db_entity_id"] = db_entity_id
        logger.info(f"Upserted {len(entities)} entities.")

        # Upsert datasets
        logger.info("---\nUpserting datasets...")
        datasets = pd.read_csv(os.path.join(data_path, "datasets.csv"))
        for i, dataset_row in tqdm(datasets.iterrows()):
            db_dataset_id = db.upsert_dataset(
                name=dataset_row["name"], namespace=dataset_namespace, user_id=USER_ID
            )
            datasets.at[i, "db_dataset_id"] = db_dataset_id
        logger.info(f"Upserted {len(datasets)} datasets.")

        # Upsert datasets
        logger.info("---\nUpserting namespace...")
        if datasets.shape[0] == 1:
            namespace_description = datasets["name"].iloc[0]
        else:
            namespace_description = f"{dataset_dir} datasets"
        db.upsert_namespace(name=dataset_namespace, description=namespace_description)
        logger.info("Upserted 1 namespace.")

        # Upsert sources
        logger.info("---\nUpserting sources...")
        sources = pd.read_csv(os.path.join(data_path, "sources.csv"))
        assert all(
            sources.groupby(["dataset_id", "name", "description"])
            .size()
            .reset_index()[0]
            == 1
        ), "All sources in a dataset must have a unique dataset_id-name-description combination."
        sources = pd.merge(
            sources,
            datasets,
            left_on="dataset_id",
            right_on="id",
            suffixes=["__source", "__dataset"],
            how="left",
        )
        for i, source_row in tqdm(sources.iterrows()):
            db_source_id = db.upsert_source(
                name=source_row.name__source,
                description=source_row.description,
                dataset_id=source_row.db_dataset_id,
            )
            sources.at[i, "db_source_id"] = db_source_id
        logger.info(f"Upserted {len(sources)} sources.")

        # Upsert variables
        logger.info("---\nUpserting variables...")
        variables = pd.read_csv(os.path.join(data_path, "variables.csv"))
        variables = variables.fillna("")
        if "notes" in variables:
            logger.warning(
                'The "notes" column in `variables.csv` is '
                'deprecated, and should be named "description" instead.'
            )
            variables.rename(columns={"notes": "description"}, inplace=True)
        variables = pd.merge(
            variables,
            datasets,
            left_on="dataset_id",
            right_on="id",
            suffixes=["__variable", "__dataset"],
            validate="m:1",
        )
        variables = pd.merge(
            variables,
            sources,
            left_on="source_id",
            right_on="id__source",
            how="left",
            validate="m:1",
            suffixes=["__variable", "__source"],
        )
        for i, variable_row in tqdm(variables.iterrows()):
            db_variable_id = db.upsert_variable(
                name=variable_row["name__variable"],
                source_id=variable_row["db_source_id"],
                dataset_id=variable_row["db_dataset_id__variable"],
                description=variable_row["description__variable"],
                code=variable_row["code"]
                if "code" in variable_row and variable_row["code"] != ""
                else None,
                unit=variable_row["unit"] if "unit" in variable_row else "",
                short_unit=variable_row["short_unit"]
                if "short_unit" in variable_row
                else None,
                timespan=variable_row["timespan"] if "timespan" in variable_row else "",
                coverage=variable_row["coverage"] if "coverage" in variable_row else "",
                display=variable_row["display"] if "display" in variable_row else None,
                original_metadata=variable_row["original_metadata"]
                if "original_metadata" in variable_row
                else None,
            )
            variables.at[i, "db_variable_id"] = db_variable_id
        logger.info(f"Upserted {len(variables)} variables.")

        # Upsert entities
        print("---\nUpserting entities...")
        entities = pd.read_csv(
            os.path.join(data_path, "distinct_countries_standardized.csv")
        )
        for entity_name in tqdm(entities.name):
            db_entity_id = db.get_or_create_entity(entity_name)
            entities.loc[entities.name == entity_name, "db_entity_id"] = db_entity_id
        print(f"Upserted {len(entities)} entities.")

        # Upserting datapoints
        logger.info("---\nUpserting datapoints...")
        datapoint_files = glob(os.path.join(data_path, "datapoints/datapoints_*.csv"))
        for datapoint_file in tqdm(datapoint_files):
            variable_id = int(re.search("\\d+", datapoint_file)[0])  # type: ignore
            db_variable_id = variables[variables["id__variable"] == variable_id][
                "db_variable_id"
            ]
            data = pd.read_csv(datapoint_file)
            data = pd.merge(
                data,
                entities,
                left_on="country",
                right_on="name",
                how="left",
                validate="m:1",
            )
            data_tuples = zip(
                data["value"],
                data["year"].astype(int),
                data["db_entity_id"].astype(int),
                [int(db_variable_id)] * len(data),
            )
            db.cursor.execute(
                """
                DELETE FROM data_values WHERE variableId=%s
            """,
                [int(db_variable_id)],
            )

            query = """
                INSERT INTO data_values
                    (value, year, entityId, variableId)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    value = VALUES(value),
                    year = VALUES(year),
                    entityId = VALUES(entityId),
                    variableId = VALUES(variableId)
            """
            db.upsert_many(query, data_tuples)
        logger.info(f"Upserted {len(datapoint_files)} datapoint files.")

        connection.commit()

    except Exception as e:
        logger.error(f"Error encountered during import: {e}")
        logger.error("Rolling back changes...")
        connection.rollback()
        if DEBUG:
            traceback.print_exc()
    finally:
        cursor.close()
        connection.close()
